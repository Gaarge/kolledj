import os
from datetime import date as Date, datetime, timedelta
from typing import List, Optional

import asyncpg
import jwt
from jwt import InvalidTokenError
from fastapi import FastAPI, HTTPException, Query, Response, Depends, Header
from pydantic import BaseModel


APP_NAME = "schedule-api"
DATABASE_URL = os.getenv("DATABASE_URL")  # postgresql://user:pass@host:5432/db

JWT_SECRET = os.getenv("SECRET_KEY", "dev-secret-change-me")
JWT_ALGO = os.getenv("JWT_ALGORITHM", "HS256")
JWT_EXPIRES_DAYS = int(os.getenv("JWT_EXPIRES_DAYS", "7"))

app = FastAPI(title=APP_NAME, version="1.4.0")

class LoginIn(BaseModel):
    username: str
    password: str


class ScheduleItem(BaseModel):
    id: int
    group_name: str
    weekday: int
    pair_number: int
    time_start: str   # строго строка "HH:MM"
    time_end: str     # строго строка "HH:MM"
    subject: str
    teacher: str
    room: str
    week_type: str


_pool: Optional[asyncpg.Pool] = None

async def get_pool() -> asyncpg.Pool:
    global _pool
    if _pool is None:
        _pool = await asyncpg.create_pool(DATABASE_URL, min_size=1, max_size=5)
    return _pool

def make_token(payload: dict) -> str:
    now = datetime.utcnow()
    exp = now + timedelta(days=JWT_EXPIRES_DAYS)
    to_encode = {
        "iat": int(now.timestamp()),
        "exp": int(exp.timestamp()),
        **payload
    }
    return jwt.encode(to_encode, JWT_SECRET, algorithm=JWT_ALGO)


class CurrentUser(BaseModel):
    id: int
    username: str
    role: str

async def get_current_user(authorization: str = Header(None)) -> CurrentUser:
    if not authorization or not authorization.startswith("Bearer "):
        raise HTTPException(status_code=401, detail="Missing or invalid Authorization header")
    token = authorization.split(" ", 1)[1]
    try:
        data = jwt.decode(token, JWT_SECRET, algorithms=[JWT_ALGO])
    except InvalidTokenError:
        # подпись не сошлась / токен протух / формат неверный
        raise HTTPException(status_code=401, detail="Invalid token")
    return CurrentUser(id=data["id"], username=data["username"], role=data["role"])

class WeekOverviewItem(BaseModel):
    date: str   # YYYY-MM-DD
    count: int  # сколько пар в этот день после наложения правок

@app.get("/api/week_overview")
async def week_overview(
    current: CurrentUser = Depends(get_current_user),
    group: Optional[str] = Query(None, min_length=1, max_length=128),
    teacher: Optional[str] = Query(None, min_length=1, max_length=128),
    monday: str = Query(..., min_length=10, max_length=10),  # понедельник недели YYYY-MM-DD
):
    # Разрешаем РОВНО один из параметров: либо group, либо teacher
    if bool(group) == bool(teacher):
        raise HTTPException(status_code=400, detail="Pass exactly one of 'group' or 'teacher'")

    try:
        m = Date.fromisoformat(monday)
    except ValueError:
        raise HTTPException(status_code=400, detail="Invalid 'monday' (YYYY-MM-DD)")

    pool = await get_pool()
    out = []

    async with pool.acquire() as conn:
        for i in range(7):
            d = m + timedelta(days=i)
            weekday = d.isoweekday()  # 1..7

            # тот же расчёт чётности
            anchor_str = os.getenv('ODD_WEEK_ANCHOR')
            if anchor_str:
                try:
                    anchor = Date.fromisoformat(anchor_str)
                    delta_days = (d - anchor).days
                    parity = 'odd' if (delta_days // 7) % 2 == 0 else 'even'
                except Exception:
                    parity = 'even' if (d.isocalendar()[1] % 2 == 0) else 'odd'
            else:
                parity = 'even' if (d.isocalendar()[1] % 2 == 0) else 'odd'

            if group:
                # Простой случай: одна группа
                merged = await merge_by_group_date(conn, group, d, weekday, parity)
                count = len(merged)
            else:
                # Случай преподавателя: группы могут отличаться на разных днях -> собираем динамически
                teacher_norm = (teacher or "").strip().lower()

                # кандидаты групп из базы
                base_groups = await conn.fetch(
                    """
                    SELECT DISTINCT group_name
                    FROM weekday_schedule
                    WHERE weekday = $1
                      AND (COALESCE(week_type,'all')='all' OR COALESCE(week_type,'all')=$2)
                      AND lower(trim(teacher)) = lower($3)
                    """, weekday, parity, teacher
                )
                groups_set = {r["group_name"] for r in base_groups}

                # кандидаты из weekly правок
                weekly_groups = await conn.fetch(
                    """
                    SELECT DISTINCT group_name
                    FROM weekly_edits
                    WHERE day_of_week = $1
                      AND (week_type='all' OR week_type=$2)
                      AND lower(COALESCE(teacher,'')) = lower($3)
                    """, weekday, parity, teacher
                )
                groups_set.update(r["group_name"] for r in weekly_groups)

                # кандидаты из once правок на текущую дату
                once_groups = await conn.fetch(
                    """
                    SELECT DISTINCT group_name
                    FROM once_edits
                    WHERE edit_date = $1
                      AND lower(COALESCE(teacher,'')) = lower($2)
                    """, d, teacher
                )
                groups_set.update(r["group_name"] for r in once_groups)

                # теперь считаем «точки» для этого дня:
                # строим итог по каждой группе и считаем только пары, где учитель совпал ПОСЛЕ наложений
                total = 0
                for g in groups_set:
                    merged = await merge_by_group_date(conn, g, d, weekday, parity)
                    total += sum(1 for it in merged if (it.get("teacher") or "").strip().lower() == teacher_norm)

                count = total

            out.append({"date": d.isoformat(), "count": count})

    return out

@app.get("/healthz")
async def healthz():
    try:
        pool = await get_pool()
        async with pool.acquire() as conn:
            await conn.execute("SELECT 1;")
        return {"status": "ok"}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

# Функция нормализации строки с именем группы на стороне SQL:
# 1) lower()
# 2) translate латинских двойников -> кириллица (A↔А, O↔О, P↔Р, C↔С, E↔Е, X↔Х, H↔Н, K↔К, M↔М, T↔Т, Y↔У)
# 3) убрать всё, что не буква/цифра (пробелы/дефисы/точки)
NORMALIZE_SQL_EXPR = """
  regexp_replace(
    lower(
      translate(
        $1,
        'ABCEHKMOPTXYabcehkmoptxy',
        'АВСЕНКМОРТХУавсенкмортху'
      )
    ),
    '[^0-9a-zа-яё]+', '', 'g'
  )
"""


@app.post("/api/login")
async def login(body: LoginIn):
    pool = await get_pool()
    async with pool.acquire() as conn:
        row = await conn.fetchrow(
            """
            SELECT id, username, role
            FROM users
            WHERE username = $1
              AND password_hash = crypt($2, password_hash)
            """,
            body.username, body.password
        )
    if not row:
        raise HTTPException(status_code=401, detail="Неверный логин или пароль")

    token = make_token({"id": row["id"], "username": row["username"], "role": row["role"]})
    return {"token": token, "role": row["role"], "username": row["username"]}


# ---------- Хелпер: объединение базы и правок для группы+даты ----------
async def merge_by_group_date(conn: asyncpg.Connection, group: str, d: Date, weekday: int, parity: str) -> List[dict]:
    # База (weekday_schedule)
    base_rows = await conn.fetch(
        """
        SELECT
          s.id,
          s.group_name,
          s.weekday,
          s.pair_number,
          to_char(s.time_start,'HH24:MI') AS time_start,
          to_char(s.time_end,  'HH24:MI') AS time_end,
          COALESCE(s.subject,'')          AS subject,
          COALESCE(s.teacher,'')          AS teacher,
          COALESCE(s.room,'')             AS room,
          COALESCE(s.week_type,'all')     AS week_type
        FROM weekday_schedule s
        WHERE
          regexp_replace(
            lower(translate(s.group_name,
              'ABCEHKMOPTXYabcehkmoptxy',
              'АВСЕНКМОРТХУавсенкмортху'
            )),
            '[^0-9a-zа-яё]+','', 'g'
          ) = regexp_replace(
                lower(translate($1,
                  'ABCEHKMOPTXYabcehkmoptxy',
                  'АВСЕНКМОРТХУавсенкмортху'
                )),
                '[^0-9a-zа-яё]+','', 'g'
              )
          AND s.weekday = $2
          AND (COALESCE(s.week_type,'all') = 'all' OR COALESCE(s.week_type,'all') = $3)
        ORDER BY s.pair_number ASC
        """,
        group, weekday, parity
    )

    # Шаблонные правки (weekly)
    weekly_rows = await conn.fetch(
        """
        SELECT pair_number, subject, teacher, room, time_start, time_end, deleted
        FROM weekly_edits
        WHERE group_name = $1
          AND day_of_week = $2
          AND (week_type = 'all' OR week_type = $3)
        """,
        group, weekday, parity
    )

    # Разовые правки (once)
    once_rows = await conn.fetch(
        """
        SELECT pair_number, subject, teacher, room, time_start, time_end, deleted
        FROM once_edits
        WHERE group_name = $1
          AND edit_date  = $2
        """,
        group, d
    )

    by_pair: dict[int, dict] = {}
    for r in base_rows:
        by_pair[int(r["pair_number"])] = {
            "id": r["id"],
            "group_name": r["group_name"],
            "weekday": r["weekday"],
            "pair_number": int(r["pair_number"]),
            "time_start": r["time_start"] or "",
            "time_end": r["time_end"] or "",
            "subject": r["subject"] or "",
            "teacher": r["teacher"] or "",
            "room": r["room"] or "",
            "week_type": r["week_type"] or "all",
        }

    def overlay(rows):
        for e in rows:
            p = int(e["pair_number"])
            prev = by_pair.get(p, {
                "id": 0, "group_name": group, "weekday": weekday, "pair_number": p,
                "time_start": "", "time_end": "", "subject": "", "teacher": "", "room": "",
                "week_type": "all"
            })
            if e["deleted"]:
                by_pair[p] = {
                    **prev,
                    "subject": "",
                    "teacher": "",
                    "room": ""
                }
                if e.get("time_start"): by_pair[p]["time_start"] = e["time_start"]
                if e.get("time_end"):   by_pair[p]["time_end"]   = e["time_end"]
            else:
                if e.get("subject"):    prev["subject"]    = e["subject"]
                if e.get("teacher"):    prev["teacher"]    = e["teacher"]
                if e.get("room"):       prev["room"]       = e["room"]
                if e.get("time_start"): prev["time_start"] = e["time_start"]
                if e.get("time_end"):   prev["time_end"]   = e["time_end"]
                by_pair[p] = prev

    overlay(weekly_rows)
    overlay(once_rows)

    return [by_pair[k] for k in sorted(by_pair.keys()) if k > 0]


@app.get("/api/schedule", response_model=List[ScheduleItem])
async def get_schedule(
    response: Response,
    current: CurrentUser = Depends(get_current_user),
    group: str = Query(..., min_length=1, max_length=64, alias="group"),
    date_: str = Query(..., alias="date", min_length=10, max_length=10),
):
    # дата + ISO-день недели + чётность
    try:
        d = Date.fromisoformat(date_)
    except ValueError:
        raise HTTPException(status_code=400, detail="Invalid 'date' (YYYY-MM-DD)")

    weekday = d.isoweekday()  # Пн=1..Вс=7
    anchor_str = os.getenv('ODD_WEEK_ANCHOR')
    if anchor_str:
        try:
            anchor = Date.fromisoformat(anchor_str)
            delta_days = (d - anchor).days
            parity = 'odd' if (delta_days // 7) % 2 == 0 else 'even'  # якорная неделя — нечётная
        except Exception:
            parity = 'even' if (d.isocalendar()[1] % 2 == 0) else 'odd'
    else:
        parity = 'even' if (d.isocalendar()[1] % 2 == 0) else 'odd'

    pool = await get_pool()
    async with pool.acquire() as conn:
        result = await merge_by_group_date(conn, group, d, weekday, parity)
    return result

    
@app.get("/api/groups")
async def get_groups(current: CurrentUser = Depends(get_current_user)):
    pool = await get_pool()
    async with pool.acquire() as conn:
        rows = await conn.fetch("SELECT DISTINCT group_name FROM weekday_schedule ORDER BY 1;")
    return {"groups": [r["group_name"] for r in rows]}


# ---------- Дополнения: поддержка расписания для преподавателей ----------

@app.get("/api/teachers")
async def get_teachers(current: CurrentUser = Depends(get_current_user)):
    """
    Вернуть список преподавателей из таблицы расписания.
    """
    pool = await get_pool()
    async with pool.acquire() as conn:
        rows = await conn.fetch("SELECT DISTINCT teacher FROM weekday_schedule WHERE teacher IS NOT NULL AND trim(teacher)<>'' ORDER BY 1;")
    return {"teachers": [r["teacher"] for r in rows]}

@app.get("/api/schedule_by_teacher", response_model=List[ScheduleItem])
async def get_schedule_by_teacher(
    response: Response,
    current: CurrentUser = Depends(get_current_user),
    teacher: str = Query(..., min_length=1, max_length=128, alias="teacher"),
    date_: str = Query(..., alias="date", min_length=10, max_length=10),  # YYYY-MM-DD
):
    try:
        d = Date.fromisoformat(date_)
    except ValueError:
        raise HTTPException(status_code=400, detail="Invalid 'date' (YYYY-MM-DD)")

    weekday = d.isoweekday()  # 1..7 (Пн=1..Вс=7)
    anchor_str = os.getenv('ODD_WEEK_ANCHOR')
    if anchor_str:
        try:
            anchor = Date.fromisoformat(anchor_str)
            delta_days = (d - anchor).days
            parity = 'odd' if (delta_days // 7) % 2 == 0 else 'even'
        except Exception:
            parity = 'even' if (d.isocalendar()[1] % 2 == 0) else 'odd'
    else:
        parity = 'even' if (d.isocalendar()[1] % 2 == 0) else 'odd'

    teacher_norm = (teacher or "").strip().lower()
    if not teacher_norm:
        return []

    pool = await get_pool()
    async with pool.acquire() as conn:
        # 1) кандидаты групп из базы (на этот weekday+parity по текущему teacher)
        base_groups = await conn.fetch(
            """
            SELECT DISTINCT group_name
            FROM weekday_schedule
            WHERE weekday = $1
              AND (COALESCE(week_type,'all')='all' OR COALESCE(week_type,'all')=$2)
              AND lower(trim(teacher)) = lower($3)
            """,
            weekday, parity, teacher
        )
        groups_set = {r["group_name"] for r in base_groups}

        # 2) кандидаты групп из шаблонных правок (weekly) — если в них teacher совпадает
        weekly_groups = await conn.fetch(
            """
            SELECT DISTINCT group_name
            FROM weekly_edits
            WHERE day_of_week = $1
              AND (week_type='all' OR week_type=$2)
              AND lower(COALESCE(teacher,'')) = lower($3)
            """,
            weekday, parity, teacher
        )
        groups_set.update(r["group_name"] for r in weekly_groups)

        # 3) кандидаты групп из разовых правок (once) на конкретную дату
        once_groups = await conn.fetch(
            """
            SELECT DISTINCT group_name
            FROM once_edits
            WHERE edit_date = $1
              AND lower(COALESCE(teacher,'')) = lower($2)
            """,
            d, teacher
        )
        groups_set.update(r["group_name"] for r in once_groups)

        # 4) для каждой группы строим итог по алгоритму, затем фильтруем по teacher
        merged_all: List[dict] = []
        for g in groups_set:
            merged_all.extend(await merge_by_group_date(conn, g, d, weekday, parity))

    # финальная фильтрация по преподавателю — уже ПОСЛЕ наложения правок
    filtered = [it for it in merged_all if (it.get("teacher") or "").strip().lower() == teacher_norm]
    filtered.sort(key=lambda x: (x.get("pair_number") or 0, x.get("time_start") or ""))
    return filtered


def require_admin(current: CurrentUser = Depends(get_current_user)) -> CurrentUser:
    if current.role != "admin":
        raise HTTPException(status_code=403, detail="admin only")
    return current

class OnceEditIn(BaseModel):
    group: str
    date: str           # YYYY-MM-DD
    pair: int
    subject: str | None = None
    teacher: str | None = None
    room: str | None = None
    time_start: str | None = None
    time_end: str | None = None
    deleted: bool = False

class WeeklyEditIn(BaseModel):
    group: str
    day_of_week: int    # 1..7 ISO
    pair: int
    scope: str = "all"  # all/even/odd
    subject: str | None = None
    teacher: str | None = None
    room: str | None = None
    time_start: str | None = None
    time_end: str | None = None
    deleted: bool = False

@app.post("/api/edits/once")
async def upsert_once_edit(body: OnceEditIn, current: CurrentUser = Depends(require_admin)):
    try:
        edit_date = Date.fromisoformat(body.date)
    except ValueError:
        raise HTTPException(status_code=400, detail="Invalid 'date'")
    if body.pair <= 0:
        raise HTTPException(status_code=400, detail="pair>0")

    pool = await get_pool()
    async with pool.acquire() as conn:
        async with conn.transaction():
            await conn.execute("""
                DELETE FROM once_edits
                 WHERE group_name=$1 AND edit_date=$2 AND pair_number=$3
            """, body.group, edit_date, body.pair)
            await conn.execute("""
                INSERT INTO once_edits
                  (group_name, edit_date, pair_number, subject, teacher, room, time_start, time_end, deleted)
                VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9)
            """, body.group, edit_date, body.pair,
                 body.subject, body.teacher, body.room, body.time_start, body.time_end, body.deleted)
    return {"ok": True}

@app.delete("/api/edits/once")
async def delete_once_for_day(
    group: str = Query(..., min_length=1),
    date: str = Query(..., min_length=10, max_length=10),
    current: CurrentUser = Depends(require_admin),
):
    try:
        edit_date = Date.fromisoformat(date)
    except ValueError:
        raise HTTPException(status_code=400, detail="Invalid 'date'")
    pool = await get_pool()
    async with pool.acquire() as conn:
        await conn.execute("DELETE FROM once_edits WHERE group_name=$1 AND edit_date=$2", group, edit_date)
    return {"ok": True}

@app.post("/api/edits/weekly")
async def upsert_weekly_edit(body: WeeklyEditIn, current: CurrentUser = Depends(require_admin)):
    if body.pair <= 0 or not (1 <= body.day_of_week <= 7):
        raise HTTPException(status_code=400, detail="bad pair/day_of_week")
    scope = (body.scope or "all").lower()
    if scope not in ("all","even","odd"):
        scope = "all"
    pool = await get_pool()
    async with pool.acquire() as conn:
        async with conn.transaction():
            await conn.execute("""
                DELETE FROM weekly_edits
                 WHERE group_name=$1 AND day_of_week=$2 AND pair_number=$3 AND week_type=$4
            """, body.group, body.day_of_week, body.pair, scope)
            await conn.execute("""
                INSERT INTO weekly_edits
                  (group_name, day_of_week, pair_number, week_type, subject, teacher, room, time_start, time_end, deleted)
                VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10)
            """, body.group, body.day_of_week, body.pair, scope,
                 body.subject, body.teacher, body.room, body.time_start, body.time_end, body.deleted)
    return {"ok": True}
