import os
from datetime import date as Date
from typing import List

import asyncpg
from fastapi import FastAPI, HTTPException, Query, Response
from pydantic import BaseModel

APP_NAME = "schedule-api"
DATABASE_URL = os.getenv("DATABASE_URL")  # postgresql://user:pass@host:5432/db

app = FastAPI(title=APP_NAME, version="1.1.0")

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

@app.get("/api/schedule", response_model=List[ScheduleItem])
async def get_schedule(
    response: Response,
    group: str = Query(..., min_length=3, max_length=32, alias="group"),
    date_: str = Query(..., alias="date", min_length=10, max_length=10), # YYYY-MM-DD
):
    # вычисляем день недели из даты — 1..7
    try:
        d = Date.fromisoformat(date_)
        weekday = d.isoweekday()  # Пн=1..Вс=7
    except Exception:
        raise HTTPException(status_code=400, detail="Invalid 'date' (YYYY-MM-DD)")

    group_norm = group.strip()

    pool = await get_pool()
    async with pool.acquire() as conn:
        rows = await conn.fetch(
            """
            SELECT id,
                   pair_number,
                   to_char(time_start,'HH24:MI') AS time_start,
                   to_char(time_end,'HH24:MI')   AS time_end,
                   COALESCE(subject,'')          AS subject,
                   COALESCE(session_type,'')     AS session_type,
                   COALESCE(room,'')             AS room,
                   COALESCE(teacher,'')          AS teacher,
                   group_name
            FROM weekday_schedule
            WHERE group_name = $1 AND weekday = $2
            ORDER BY pair_number
            """,
            group_norm, weekday,
        )

    # добавим клиенту ту же дату, что он запросил
    result = []
    for r in rows:
        item = dict(r)
        item["date"] = d
        result.append(item)

    return result
