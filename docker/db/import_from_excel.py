#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Импорт расписания из Excel в таблицу weekday_schedule с ожиданием готовности БД,
ретраями и подробным логированием.

Зависимости (ставятся в Dockerfile):
  pandas, openpyxl, psycopg2-binary
"""

import os
import re
import sys
import time
from datetime import time as dtime
from typing import List, Dict, Any, Optional, Tuple

import pandas as pd
import psycopg2
from psycopg2.extras import execute_values


# ---------- Константы и утилиты ----------

EXCEL_PATH = os.getenv("EXCEL_PATH", "/app/excel/schedule.xlsx")

# 1=Пн .. 7=Вс
WEEKDAY_MAP = {
    "понедельник": 1, "вторник": 2, "среда": 3, "среда ": 3, "четверг": 4, "четверг ": 4,
    "пятница": 5, "пятница ": 5, "суббота": 6, "суббота ": 6, "воскресенье": 7,
}
REV_WEEKDAY = {v: k for k, v in WEEKDAY_MAP.items()}

# маппинг значений из колонки «Тип недели»
WEEK_TYPE_MAP = {
    'все': 'all', 'всё': 'all', 'all': 'all', '': 'all', 'nan': 'all',
    'четная': 'even', 'чётная': 'even', 'чет': 'even', 'ч': 'even',
    'нечетная': 'odd', 'нечётная': 'odd', 'нечет': 'odd', 'нч': 'odd', 'н': 'odd',
}

STRUCT_COLS = [
    "группа", "день недели", "номер пары", "время начала", "время окончания",
    "название предмета", "преподаватель", "аудитория", "тип недели"
]

TIME_RE = re.compile(r'(\d{1,2})[.:](\d{2})\s*[-–]\s*(\d{1,2})[.:](\d{2})')

def log(*a): print("[import]", *a, flush=True)
def warn(*a): print("[import][WARN]", *a, flush=True, file=sys.stderr)
def err(*a): print("[import][ERROR]", *a, flush=True, file=sys.stderr)

def to_int(x, default=None):
    try:
        return int(x)
    except Exception:
        return default

def to_time_pair(s: str) -> Tuple[Optional[dtime], Optional[dtime]]:
    if not isinstance(s, str):
        return None, None
    m = TIME_RE.search(s)
    if not m:
        return None, None
    h1, m1, h2, m2 = map(int, m.groups())
    return dtime(h1, m1), dtime(h2, m2)


# ---------- Парсинг Excel ----------

def try_load_structured(xl: pd.ExcelFile) -> Optional[List[Dict[str, Any]]]:
    """
    Ожидается один лист (например, 'Расписание') с колонками STRUCT_COLS.
    """
    try:
        df = xl.parse(xl.sheet_names[0])
    except Exception:
        return None

    cols = [str(c).strip().lower() for c in df.columns]
    if not all(c in cols for c in STRUCT_COLS):
        return None

    map_idx = {c: cols.index(c) for c in STRUCT_COLS}
    rows: List[Dict[str, Any]] = []

    for _, row in df.iterrows():
        group = str(row.iloc[map_idx["группа"]]).strip()
        if not group or group.lower() == "nan":
            continue

        day = str(row.iloc[map_idx["день недели"]]).strip().lower()
        weekday = WEEKDAY_MAP.get(day, 0)
        if weekday == 0:
            continue

        pair = to_int(row.iloc[map_idx["номер пары"]])
        if not pair:
            continue

        # время может быть как в отдельных колонках, так и «08:20-09:50»
        t1, t2 = None, None
        v_start = row.iloc[map_idx["время начала"]]
        v_end   = row.iloc[map_idx["время окончания"]]
        if isinstance(v_start, str) and "-" in v_start:
            t1, t2 = to_time_pair(v_start)
        else:
            # отдельные значения, например 08:20 и 09:50
            try:
                s = str(v_start)
                h, m = map(int, s.split(":"))
                t1 = dtime(h, m)
            except Exception:
                pass
            try:
                s = str(v_end)
                h, m = map(int, s.split(":"))
                t2 = dtime(h, m)
            except Exception:
                pass

        if not t1 or not t2:
            # попробуем из «Название предмета» извлечь время, если оно там
            ts, te = to_time_pair(str(row.iloc[map_idx["название предмета"]]))
            t1 = t1 or ts
            t2 = t2 or te

        if not t1 or not t2:
            continue

        subject = str(row.iloc[map_idx["название предмета"]]).strip()
        teacher = str(row.iloc[map_idx["преподаватель"]]).strip()
        room    = str(row.iloc[map_idx["аудитория"]]).strip()
        week_raw = str(row.iloc[map_idx.get("тип недели", -1)]) if ("тип недели" in map_idx) else ""
        week_raw = (week_raw or "").strip().lower()
        week_type = WEEK_TYPE_MAP.get(week_raw, 'all')

        rows.append({
            "weekday": weekday,
            "pair_number": pair,
            "time_start": t1.strftime("%H:%M"),
            "time_end":   t2.strftime("%H:%M"),
            "subject": subject,
            "room": room,
            "teacher": teacher,
            "group_name": group,
            "week_type": week_type,
        })

    return rows


def parse_legacy(xl: pd.ExcelFile) -> List[Dict[str, Any]]:
    """
    Резервный парсер — по листам, названным днями недели.
    subject в таком формате часто не извлекается надёжно -> оставляем пустым.
    """
    def find_time_col(df):
        for c in range(min(5, df.shape[1])):
            if any(isinstance(x, str) and TIME_RE.search(str(x)) for x in df.iloc[:, c][:8].tolist()):
                return c
        return 0

    def collect_rooms(df, header_row, time_col):
        rooms = {}
        probe = df.iloc[header_row:header_row+3, :]
        for r in range(probe.shape[0]):
            for c in range(probe.shape[1]):
                if c == time_col:
                    continue
                val = str(probe.iat[r, c]).strip()
                # простая эвристика «Ауд 34»/«34» и т.п.
                if re.search(r'(Ауд|ауд|^[0-9A-Za-zА-Яа-я\-]+$)', val):
                    rooms[c] = val
        return rooms

    all_rows: List[Dict[str, Any]] = []

    for sh in xl.sheet_names:
        sh_norm = sh.strip().lower()
        if sh_norm not in WEEKDAY_MAP:
            continue

        df = xl.parse(sh, header=None)
        header_row = 0
        for r in range(min(5, df.shape[0])):
            row = df.iloc[r, :].astype(str).tolist()
            if any('Ауд' in x for x in row):
                header_row = r
                break

        time_col = find_time_col(df)
        rooms = collect_rooms(df, header_row, time_col)

        # разбор строк с временами «08:20-09:50»
        pair_idx = 0
        for r in range(header_row + 1, df.shape[0]):
            time_cell = str(df.iat[r, time_col])
            t1, t2 = to_time_pair(time_cell)
            if not t1 or not t2:
                continue
            pair_idx += 1

            for c in range(df.shape[1]):
                if c == time_col:
                    continue
                cell = str(df.iat[r, c]).strip()
                if not cell or cell.lower() == 'nan':
                    continue

                # грубая эвристика для групп и ФИО
                groups = re.findall(r'[A-Za-zА-Яа-яё0-9/.\-]{3,}', cell)
                teacher = re.sub(
                    r'.*?\b([А-ЯЁ][а-яё]+(?:\s+[А-ЯЁ][а-яё]+){0,2})\b.*',
                    r'\1', cell
                ).strip()
                room = rooms.get(c, "")

                for g in groups:
                    if not g:
                        continue
                    all_rows.append({
                        "weekday": WEEKDAY_MAP.get(sh_norm, 0),
                        "pair_number": pair_idx,
                        "time_start": t1.strftime("%H:%M"),
                        "time_end":   t2.strftime("%H:%M"),
                        "subject": "",          # в legacy надёжно не извлекаем
                        "room": str(room),
                        "teacher": teacher if teacher else "",
                        "group_name": g,
                        "week_type": "all",
                    })

    return all_rows


# ---------- Схема БД ----------

def ensure_schema(conn) -> None:
    with conn.cursor() as cur:
        cur.execute("""
        
CREATE TABLE IF NOT EXISTS weekday_schedule (
  id SERIAL PRIMARY KEY,
  weekday SMALLINT NOT NULL CHECK (weekday BETWEEN 1 AND 7),
  pair_number SMALLINT NOT NULL CHECK (pair_number BETWEEN 1 AND 20),
  time_start TIME NOT NULL,
  time_end   TIME NOT NULL,
  subject    TEXT,
  room       VARCHAR(32),
  teacher    TEXT,
  group_name VARCHAR(32) NOT NULL,
  week_type  VARCHAR(8) NOT NULL DEFAULT 'all',  -- 'all' | 'odd' | 'even'
  created_at TIMESTAMPTZ DEFAULT now()
);
-- на случай первого запуска создадим индекс, по которому чаще всего фильтруем
CREATE INDEX IF NOT EXISTS idx_weekday_schedule_group_day
  ON weekday_schedule (group_name, weekday);
-- миграция: добавить колонку week_type, если её нет; удалить старый уникальный констрейнт и создать новый
DO $$
DECLARE
  c TEXT;
BEGIN
  -- добавить колонку week_type, если её нет
  BEGIN
    ALTER TABLE weekday_schedule
      ADD COLUMN IF NOT EXISTS week_type VARCHAR(8) NOT NULL DEFAULT 'all';
  EXCEPTION WHEN others THEN
    NULL;
  END;

  -- найти старый уникальный констрейнт (group_name, weekday, pair_number)
  SELECT conname INTO c
  FROM pg_constraint pc
  JOIN pg_class t ON t.oid = pc.conrelid
  WHERE t.relname = 'weekday_schedule'
    AND pc.contype = 'u'
    AND (
      SELECT array_agg(a.attname ORDER BY a.attnum)
      FROM unnest(pc.conkey) AS k(attnum)
      JOIN pg_attribute a ON a.attrelid = t.oid AND a.attnum = k.attnum
    ) = ARRAY['group_name','weekday','pair_number']::name[];  -- ← приведение типов

  IF c IS NOT NULL THEN
    EXECUTE format('ALTER TABLE weekday_schedule DROP CONSTRAINT %I', c);
  END IF;
END $$;

-- убрать старый индекс, если вдруг остался
DROP INDEX IF EXISTS uniq_weekday_schedule_gwpw;
-- новый уникальный индекс: различаем подгруппы (teacher/room)
CREATE UNIQUE INDEX IF NOT EXISTS uniq_weekday_gwpwtr
  ON weekday_schedule (group_name, weekday, pair_number, week_type, teacher, room);
        """)
    conn.commit()


# ---------- Ожидание БД с ретраями ----------

def make_dsn_from_env() -> str:
    env = os.environ
    user = env.get("POSTGRES_USER", "postgres")
    db   = env.get("POSTGRES_DB",   "postgres")
    host = env.get("POSTGRES_HOST", "127.0.0.1")
    port = env.get("POSTGRES_PORT", "5432")
    pwd  = env.get("POSTGRES_PASSWORD", "")

    dsn = env.get("DATABASE_URL") or f"postgresql://{user}:{pwd}@{host}:{port}/{db}"
    return dsn

def wait_for_db(dsn: str,
                timeout_sec: int = None,
                retry_interval_sec: int = None) -> None:
    """
    Ждём, пока БД начнёт принимать подключения.
    """
    timeout_sec = timeout_sec or int(os.getenv("DB_WAIT_TIMEOUT_SEC", "300"))
    retry_interval_sec = retry_interval_sec or int(os.getenv("RETRY_INTERVAL_SEC", "5"))

    start = time.time()
    attempt = 0
    while True:
        attempt += 1
        try:
            conn = psycopg2.connect(dsn)
            conn.close()
            log(f"DB is ready after {attempt} attempt(s).")
            return
        except Exception as e:
            elapsed = int(time.time() - start)
            if elapsed >= timeout_sec:
                raise RuntimeError(f"DB not ready after {elapsed}s: {e}")
            warn(f"DB not ready yet (attempt {attempt}): {e}")
            time.sleep(retry_interval_sec)


# ---------- Основной поток ----------

def main() -> None:
    # Читаем Excel
    if not os.path.exists(EXCEL_PATH):
        raise FileNotFoundError(f"Excel not found at {EXCEL_PATH}")

    log(f"reading excel: {EXCEL_PATH}")
    xl = pd.ExcelFile(EXCEL_PATH)

    rows: Optional[List[Dict[str, Any]]] = try_load_structured(xl)
    if rows is not None:
        log(f"structured rows: {len(rows)}")
    else:
        log("structured format not detected -> fallback to legacy parsing")
        rows = parse_legacy(xl)
        log(f"legacy rows: {len(rows)}")

    if not rows:
        log("nothing to import")
        return

    # Подключение к Postgres
    dsn = make_dsn_from_env()
    log("connecting to:", dsn)

    # Ожидаем готовности БД
    wait_for_db(dsn)

    inserted = 0
    with psycopg2.connect(dsn) as conn:
        ensure_schema(conn)

        with conn.cursor() as cur:
            cur.execute("TRUNCATE weekday_schedule;")
        conn.commit()

        cols = ["weekday","pair_number","time_start","time_end",
                "subject","room","teacher","group_name","week_type"]

        # Готовим значения
        values = [[row.get(c) for c in cols] for row in rows]

        # Пачками, чтобы не упираться в размер запроса
        page_size = int(os.getenv("BULK_PAGE_SIZE", "2000"))
        log(f"inserting {len(values)} rows (page_size={page_size})...")
        with conn.cursor() as cur:
            execute_values(
                cur,
                f"INSERT INTO weekday_schedule ({', '.join(cols)}) "
                f"VALUES %s ON CONFLICT DO NOTHING",
                values,
                page_size=page_size
            )
            inserted = cur.rowcount if cur.rowcount is not None else 0
        conn.commit()

    log(f"import done. inserted (or kept due to conflict): ~{inserted} rows")

if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        err(e)
        # Ненулевой код — чтобы в логах было видно, что импорт сломался.
        # (В postStart он сейчас завернут в '|| true', так что pod не упадёт.)
        sys.exit(1)
