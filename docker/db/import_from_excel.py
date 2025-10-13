#!/usr/bin/env python3
import os, re, time, sys
from datetime import time as dtime
import pandas as pd
import psycopg2
from psycopg2.extras import execute_values

EXCEL_PATH = os.getenv("EXCEL_PATH", "/app/excel/schedule.xlsx")

WEEKDAY_MAP = {
    "понедельник": 1, "вторник": 2, "среда": 3, "среда ": 3,
    "четверг": 4, "четверг ": 4, "пятница": 5, "пятница ": 5,
    "суббота": 6, "суббота ": 6, "воскресенье": 7,
}

TIME_RE  = re.compile(r'(\d{1,2})[.:](\d{2})\s*[-–]\s*(\d{1,2})[.:](\d{2})')
GROUP_RE = re.compile(r'([A-Za-zА-Яа-яЁё]{1,3}\d{2}-\d{2})')

def log(*a): print("[import]", *a, flush=True, file=sys.stdout)

def parse_time_range(s):
    if not isinstance(s, str): return None, None
    m = TIME_RE.search(s)
    if not m: return None, None
    h1, m1, h2, m2 = map(int, m.groups())
    return dtime(h1, m1), dtime(h2, m2)

def extract_groups_teacher(cell: str):
    if not isinstance(cell, str): return [], ""
    txt = cell.strip()
    txt = re.sub(r'\([^)]*\)', '', txt)  # убрать скобки с датами/ремарками
    groups = GROUP_RE.findall(txt)
    teacher = ""
    words = [w for w in re.split(r'\s+', txt) if w]
    for w in reversed(words):
        if re.match(r'^[А-ЯЁA-Z][а-яёa-z\-]{2,}$', w) and not GROUP_RE.match(w):
            teacher = w
            break
    return list(dict.fromkeys(groups)), teacher

def find_time_col(df):
    for c in range(min(3, df.shape[1])):
        vals = df.iloc[:10, c].astype(str).tolist()
        if any(TIME_RE.search(v) for v in vals): return c
    return 0

def collect_rooms(df, header_row, time_col):
    rooms = {}
    probe = df.iloc[header_row:header_row+3, :]
    for r in range(probe.shape[0]):
        for c in range(probe.shape[1]):
            if c == time_col: continue
            val = str(probe.iat[r, c]).strip()
            if not val or val.lower() == 'nan': continue
            if re.fullmatch(r'\d+[A-Za-zА-Яа-я\-]*', val):
                rooms[c] = val
    if not rooms:
        row = header_row + 1
        for c in range(1, min(30, df.shape[1])):
            val = str(df.iat[row, c]).strip()
            if val and val.lower() != 'nan':
                rooms[c] = val
    return rooms

def parse_sheet(xl, sheet_name):
    df = xl.parse(sheet_name, header=None)
    header_row = 0
    for r in range(min(5, df.shape[0])):
        row = df.iloc[r, :].astype(str).tolist()
        if any('Ауд' in x for x in row):
            header_row = r; break
    time_col = find_time_col(df)
    rooms = collect_rooms(df, header_row, time_col)

    data = []
    pair_idx = 0
    for r in range(header_row+1, df.shape[0]):
        t1, t2 = parse_time_range(str(df.iat[r, time_col]))
        if not t1: 
            continue
        pair_idx += 1
        for c, room in rooms.items():
            cell = df.iat[r, c]
            if (isinstance(cell, float) and pd.isna(cell)) or (isinstance(cell, str) and not cell.strip()):
                continue
            groups, teacher = extract_groups_teacher(str(cell))
            for g in groups:
                if not g: continue
                data.append({
                    "weekday": WEEKDAY_MAP.get(sheet_name.strip().lower(), 0),
                    "pair_number": pair_idx,
                    "time_start": t1.strftime("%H:%M"),
                    "time_end":   t2.strftime("%H:%M"),
                    "subject": "",
                    "session_type": "",
                    "room": str(room),
                    "teacher": teacher or "",
                    "group_name": g,
                })
    return data

def ensure_schema(conn):
    with conn.cursor() as cur:
        cur.execute("""
        CREATE TABLE IF NOT EXISTS weekday_schedule (
          id SERIAL PRIMARY KEY,
          weekday SMALLINT NOT NULL CHECK (weekday BETWEEN 1 AND 7),
          pair_number SMALLINT NOT NULL CHECK (pair_number BETWEEN 1 AND 20),
          time_start TIME NOT NULL,
          time_end   TIME NOT NULL,
          subject    TEXT,
          session_type VARCHAR(16),
          room       VARCHAR(32),
          teacher    TEXT,
          group_name VARCHAR(32) NOT NULL,
          created_at TIMESTAMPTZ DEFAULT now(),
          UNIQUE (group_name, weekday, pair_number)
        );
        CREATE INDEX IF NOT EXISTS idx_weekday_schedule_group_day
          ON weekday_schedule (group_name, weekday);
        """)

def try_connect(dsn_list, timeout_sec=300):
    """Пробуем по очереди набор DSN до удачи, максимум timeout_sec."""
    start = time.time()
    last_err = None
    while time.time() - start < timeout_sec:
        for dsn in dsn_list:
            try:
                conn = psycopg2.connect(dsn)
                conn.autocommit = True
                log(f"connected via: {dsn}")
                return conn
            except Exception as e:
                last_err = e
        time.sleep(2)
    raise RuntimeError(f"could not connect to postgres in {timeout_sec}s: {last_err}")

def main():
    log(f"excel: {EXCEL_PATH}")
    xl = pd.ExcelFile(EXCEL_PATH)

    all_rows = []
    for sh in xl.sheet_names:
        if sh.strip().lower() in WEEKDAY_MAP:
            parsed = parse_sheet(xl, sh)
            all_rows.extend(parsed)
    log(f"parsed rows: {len(all_rows)}")
    if not all_rows:
        log("nothing to import")
        return

    # Ждём готовности сервера (по unix-сокету, без авторизации)
    os.system("pg_isready -h /var/run/postgresql -p 5432 || true")

    # Список вариантов подключения
    env = os.environ
    durl = env.get("DATABASE_URL")
    db   = env.get("POSTGRES_DB", "postgres")
    user = env.get("POSTGRES_USER", "postgres")

    dsn_candidates = []
    # 1) Unix-сокет, как postgres к БД postgres (peer-auth обычно ок)
    dsn_candidates.append("dbname=postgres user=postgres host=/var/run/postgresql")
    # 2) Unix-сокет с env-пользователем/БД
    dsn_candidates.append(f"dbname={db} user={user} host=/var/run/postgresql")
    # 3) DATABASE_URL если задан
    if durl:
        dsn_candidates.append(durl)
    # 4) TCP как раньше (могут подойти, если кластер совпадает с env)
    host = env.get("POSTGRES_HOST", "127.0.0.1")
    port = env.get("POSTGRES_PORT", "5432")
    pwd  = env.get("POSTGRES_PASSWORD", "")
    dsn_candidates.append(f"postgresql://{user}:{pwd}@{host}:{port}/{db}")

    conn = try_connect(dsn_candidates, timeout_sec=300)  # ждём до 5 минут

    with conn:
        ensure_schema(conn)
        with conn.cursor() as cur:
            cur.execute("TRUNCATE TABLE weekday_schedule;")
        cols = ["weekday","pair_number","time_start","time_end","subject","session_type","room","teacher","group_name"]
        values = [[row[c] for c in cols] for row in all_rows]
        with conn.cursor() as cur:
            execute_values(cur,
                f"INSERT INTO weekday_schedule ({', '.join(cols)}) VALUES %s ON CONFLICT DO NOTHING",
                values, page_size=2000)
    log("import done.")

if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        print("[import][ERROR]", e, file=sys.stderr, flush=True)
        sys.exit(0)  # не валим контейнер даже при ошибке
