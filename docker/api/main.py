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


@app.get("/api/schedule", response_model=List[ScheduleItem])
async def get_schedule(
    response: Response,
    current: CurrentUser = Depends(get_current_user),
    group: str = Query(..., min_length=1, max_length=64, alias="group"),
    date_: str = Query(..., alias="date", min_length=10, max_length=10),  # YYYY-MM-DD
):
    # --- расчёт даты/дня недели/чётности ---
    try:
        d = Date.fromisoformat(date_)
    except ValueError:
        raise HTTPException(status_code=400, detail="Invalid 'date' (YYYY-MM-DD)")

    weekday = d.isoweekday()  # Пн=1..Вс=7
    # чётность недели (якорь можно задать через ODD_WEEK_ANCHOR, по умолчанию ISO):
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

    # --- простые заглушки по ролям ---
    if current.role == "teacher":
        return []  # TODO: позже вернуть пары текущего преподавателя
    if current.role == "admin":
        return []  # TODO: позже сделать обзор по всем группам

    # --- выборка для студента (или дефолт) ---
    pool = await get_pool()
    async with pool.acquire() as conn:
        rows = await conn.fetch(
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
              COALESCE(s.week_type,'all')     AS week_type,
              ''::text                        AS session_type
            FROM weekday_schedule s
            WHERE
              -- нормализуем и сравниваем имя группы (латиница->кириллица, lower, убрать лишние символы):
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
            group,   # $1
            weekday, # Пн=1..Вс=7  ($2)
            parity   # 'odd' / 'even'           ($3)
        )
        

    result = []
    for r in rows:
        item = dict(r)
        item["date"] = d
        result.append(item)
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

    weekday = d.isoweekday()  # 1..7
    if weekday == 7:
        return []

    parity = 'even' if (d.isocalendar()[1] % 2 == 0) else 'odd'

    pool = await get_pool()
    async with pool.acquire() as conn:
        rows = await conn.fetch(
            """
            SELECT
              id,
              COALESCE(group_name,'')                    AS group_name,
              weekday,
              pair_number,
              to_char(time_start,'HH24:MI')              AS time_start,
              to_char(time_end,  'HH24:MI')              AS time_end,
              COALESCE(subject,'')                       AS subject,
              COALESCE(teacher,'')                       AS teacher,
              COALESCE(room,'')                          AS room,
              COALESCE(week_type,'all')                  AS week_type
            FROM weekday_schedule
            WHERE trim(teacher) = $1
              AND weekday = $2
              AND (COALESCE(week_type,'all') = 'all' OR COALESCE(week_type,'all') = $3)
            ORDER BY pair_number
            """,
            teacher, weekday, parity
        )

    result = []
    for r in rows:
        item = dict(r)
        item["date"] = d
        result.append(item)
    return result
