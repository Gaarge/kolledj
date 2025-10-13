import os
from datetime import date as Date
from typing import List

import asyncpg
from fastapi import FastAPI, HTTPException, Query, Response
from pydantic import BaseModel

APP_NAME = "schedule-api"
DATABASE_URL = os.getenv("DATABASE_URL")  # postgresql://user:pass@host:5432/db

app = FastAPI(title=APP_NAME, version="1.3.0")

class ScheduleItem(BaseModel):
    id: int
    date: Date
    pair_number: int
    time_start: str
    time_end: str
    subject: str
    session_type: str
    room: str
    teacher: str
    group_name: str

_pool = None

async def get_pool():
    global _pool
    if _pool is None:
        if not DATABASE_URL:
            raise RuntimeError("DATABASE_URL is not set")
        _pool = await asyncpg.create_pool(DATABASE_URL, min_size=1, max_size=5)
    return _pool

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

@app.get("/api/schedule", response_model=List[ScheduleItem])
async def get_schedule(
    response: Response,
    group: str = Query(..., min_length=1, max_length=64, alias="group"),
    date_: str = Query(..., alias="date", min_length=10, max_length=10),  # YYYY-MM-DD
):
    # день недели: Пн=1 .. Вс=7
    try:
        d = Date.fromisoformat(date_)
        weekday = d.isoweekday()
    except Exception:
        raise HTTPException(status_code=400, detail="Invalid 'date' (YYYY-MM-DD)")

    pool = await get_pool()
    async with pool.acquire() as conn:
        rows = await conn.fetch(
            f"""
            WITH inp AS ( SELECT {NORMALIZE_SQL_EXPR} AS g )
            SELECT
               s.id,
               s.pair_number,
               to_char(s.time_start,'HH24:MI') AS time_start,
               to_char(s.time_end,'HH24:MI')   AS time_end,
               COALESCE(s.subject,'')          AS subject,
               COALESCE(s.session_type,'')     AS session_type,
               COALESCE(s.room,'')             AS room,
               COALESCE(s.teacher,'')          AS teacher,
               s.group_name
            FROM weekday_schedule s, inp
            WHERE
              regexp_replace(
                lower(
                  translate(
                    s.group_name,
                    'ABCEHKMOPTXYabcehkmoptxy',
                    'АВСЕНКМОРТХУавсенкмортху'
                  )
                ),
                '[^0-9a-zа-яё]+','', 'g'
              ) = inp.g
              AND s.weekday = $2
            ORDER BY s.pair_number
            """,
            group, weekday
        )

    result = []
    for r in rows:
        item = dict(r)
        item["date"] = d
        result.append(item)
    return result

@app.get("/api/groups")
async def get_groups():
    pool = await get_pool()
    async with pool.acquire() as conn:
        rows = await conn.fetch("SELECT DISTINCT group_name FROM weekday_schedule ORDER BY 1;")
    return {"groups": [r["group_name"] for r in rows]}
