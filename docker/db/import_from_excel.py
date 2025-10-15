#!/usr/bin/env python3
import os, re, sys
from datetime import time as dtime
import pandas as pd
import psycopg2
from psycopg2.extras import execute_values

EXCEL_PATH = os.getenv("EXCEL_PATH", "/app/excel/schedule.xlsx")

# 1=Пн .. 7=Вс
WEEKDAY_MAP = {
    "понедельник":1,"вторник":2,"среда":3,"среда ":3,"четверг":4,"четверг ":4,
    "пятница":5,"пятница ":5,"суббота":6,"суббота ":6,"воскресенье":7,
}
REV_WEEKDAY = {v:k for k,v in WEEKDAY_MAP.items()}

TIME_RE = re.compile(r'(\d{1,2})[.:](\d{2})\s*[-–]\s*(\d{1,2})[.:](\d{2})')

def log(*a): print("[import]", *a, flush=True)

def to_time_pair(s: str):
    if not isinstance(s, str): return None, None
    m = TIME_RE.search(s)
    if not m: return None, None
    h1, m1, h2, m2 = map(int, m.groups())
    return dtime(h1, m1), dtime(h2, m2)

def to_int(x, default=None):
    try: return int(x)
    except: return default

def norm_week_type(x: str):
    if not isinstance(x, str): return "все"
    t = x.strip().lower()
    if "четн" in t: return "четная"
    if "нечет" in t or "н/ч" in t or "нч" in t: return "нечетная"
    return "все"

# ====== Ветка 1: структурированный Excel ======
STRUCT_COLS = [
    "группа","день недели","номер пары","время начала","время окончания",
    "название предмета","преподаватель","аудитория","тип недели"
]

def try_load_structured(xl: pd.ExcelFile):
    try:
        df = xl.parse(xl.sheet_names[0])
    except Exception:
        return None

    cols = [str(c).strip().lower() for c in df.columns]
    if not all(c in cols for c in STRUCT_COLS):
        return None

    map_idx = {c: cols.index(c) for c in STRUCT_COLS}
    rows = []
    for _, row in df.iterrows():
        group = str(row.iloc[map_idx["группа"]]).strip()
        day   = str(row.iloc[map_idx["день недели"]]).strip().lower()
        pair  = to_int(row.iloc[map_idx["номер пары"]])
        t1s   = str(row.iloc[map_idx["время начала"]]).strip()
        t2s   = str(row.iloc[map_idx["время окончания"]]).strip()
        sub   = str(row.iloc[map_idx["название предмета"]]).strip()
        teach = str(row.iloc[map_idx["преподаватель"]]).strip()
        room  = str(row.iloc[map_idx["аудитория"]]).strip()
        wtype = norm_week_type(str(row.iloc[map_idx["тип недели"]]))

        if not group or day not in WEEKDAY_MAP or not pair:
            continue

        # NB: тип недели храним в session_type, как у тебя в схеме
        rows.append({
            "weekday": WEEKDAY_MAP[day],
            "pair_number": pair,
            "time_start": t1s,
            "time_end": t2s,
            "subject": sub,
            "session_type": wtype,
            "room": room,
            "teacher": teach,
            "group_name": group,
        })
    return rows

# ====== Ветка 2 (fallback): старый «грязный» Excel ======
def parse_legacy(xl: pd.ExcelFile):
    # Этот парсер похож на твой: он умеет вытаскивать время/ауд./преп./группы,
    # но SUBJECT в таком файле извлечь надёжно нельзя -> будет пустым.
    def find_time_col(df):
        for c in range(min(5, df.shape[1])):
            if any(isinstance(x,str) and TIME_RE.search(str(x)) for x in df.iloc[:, c][:8].tolist()):
                return c
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
        return rooms

    def parse_time_range(s): return to_time_pair(s)

    all_rows = []
    for sh in xl.sheet_names:
        if sh.strip().lower() not in WEEKDAY_MAP: continue
        df = xl.parse(sh, header=None)
        header_row = 0
        for r in range(min(5, df.shape[0])):
            row = df.iloc[r, :].astype(str).tolist()
            if any('Ауд' in x for x in row):
                header_row = r; break
        time_col = find_time_col(df)
        rooms = collect_rooms(df, header_row, time_col)

        pair_idx = 0
        for r in range(header_row+1, df.shape[0]):
            t1, t2 = parse_time_range(str(df.iat[r, time_col]))
            if not t1: 
                continue
            pair_idx += 1
            for c in range(time_col+1, df.shape[1]):
                cell = str(df.iat[r, c]).strip()
                if not cell or cell.lower() == 'nan': continue
                # грубое выделение групп и преподавателя
                groups = re.findall(r'[A-Za-zА-Яа-яё0-9/.-]{3,}', cell)
                teacher = re.sub(r'.*?\b([А-ЯЁ][а-яё]+(?:\s+[А-ЯЁ][а-яё]+){0,2})\b.*', r'\1', cell).strip()
                room = rooms.get(c, "")
                for g in groups:
                    if not g: continue
                    all_rows.append({
                        "weekday": WEEKDAY_MAP.get(sh.strip().lower(), 0),
                        "pair_number": pair_idx,
                        "time_start": t1.strftime("%H:%M"),
                        "time_end":   t2.strftime("%H:%M"),
                        "subject": "",              # вот почему у тебя пусто
                        "session_type": "",
                        "room": str(room),
                        "teacher": teacher if teacher else "",
                        "group_name": g,
                    })
    return all_rows

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
        conn.commit()

def main():
    log(f"excel: {EXCEL_PATH}")
    xl = pd.ExcelFile(EXCEL_PATH)

    rows = try_load_structured(xl)
    if rows is not None:
        log(f"structured rows: {len(rows)}")
    else:
        log("structured format not detected -> fallback to legacy parsing")
        rows = parse_legacy(xl)
        log(f"legacy rows: {len(rows)}")

    if not rows:
        log("nothing to import"); return

    # подключение к Postgres
    env = os.environ
    user = env.get("POSTGRES_USER", "postgres")
    db   = env.get("POSTGRES_DB",   "postgres")
    host = env.get("POSTGRES_HOST", "127.0.0.1")
    port = env.get("POSTGRES_PORT", "5432")
    pwd  = env.get("POSTGRES_PASSWORD", "")

    dsn = env.get("DATABASE_URL") or f"postgresql://{user}:{pwd}@{host}:{port}/{db}"
    log("connect:", dsn)
    with psycopg2.connect(dsn) as conn:
        ensure_schema(conn)
        with conn.cursor() as cur:
            cur.execute("TRUNCATE weekday_schedule;")

        cols = ["weekday","pair_number","time_start","time_end","subject","session_type","room","teacher","group_name"]
        values = [[row.get(c) for c in cols] for row in rows]
        with conn.cursor() as cur:
            execute_values(cur,
                f"INSERT INTO weekday_schedule ({', '.join(cols)}) VALUES %s ON CONFLICT DO NOTHING",
                values, page_size=2000)
        conn.commit()
    log("import done.")

if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        print("[import][ERROR]", e, file=sys.stderr, flush=True)
        sys.exit(0)  # не валим контейнер даже при ошибке
