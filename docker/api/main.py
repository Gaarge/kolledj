import os
import hashlib
from datetime import date as Date
from typing import List, Optional

import asyncpg
from fastapi import FastAPI, HTTPException, Query, Response
from pydantic import BaseModel

APP_NAME = "schedule-api"
DATABASE_URL = os.getenv("DATABASE_URL")  # postgresql://user:pass@host:5432/db

app = FastAPI(title=APP_NAME, version="1.0.0")

class ScheduleItem(BaseModel):
    id: int
    date: Date
    pair_number: int
    time_start: str
    time_end: str
    subject: str
    session_type: Optional[str] = None
    room: Optional[str] = None
    teacher: Optional[str] = None
    group_name: str

async def get_pool() -> asyncpg.Pool:
    if not hasattr(app.state, "pool"):
        if not DATABASE_URL:
            raise RuntimeError("DATABASE_URL is not configured")
        app.state.pool = await asyncpg.create_pool(dsn=DATABASE_URL, min_size=2, max_size=20)
    return app.state.pool

@app.get("/healthz")
async def healthz():
    return {"status": "ok"}

@app.get("/api/schedule", response_model=List[ScheduleItem])
async def get_schedule(response: Response,
                       group: str = Query(..., min_length=1, max_length=64),
                       date_: str = Query(alias="date", min_length=10, max_length=10)):
    try:
        # строго разбираем в python-date
        d = Date.fromisoformat(date_)         # <-- главное изменение
    except Exception:
        raise HTTPException(status_code=400, detail="Invalid 'date' (YYYY-MM-DD)")

    pool = await get_pool()
    async with pool.acquire() as conn:
        rows = await conn.fetch(
            """
            SELECT id, date, pair_number,
                   to_char(time_start,'HH24:MI') AS time_start,
                   to_char(time_end,'HH24:MI')   AS time_end,
                   subject, session_type, room, teacher, group_name
            FROM schedule
            WHERE group_name = $1 AND date = $2       -- <-- без ::date, передаём python-date
            ORDER BY pair_number
            """,
            group, d,
        )

    data = [dict(r) for r in rows] if rows else []
    # ... остальной код без изменений
    return data
