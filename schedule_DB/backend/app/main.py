import os
import asyncpg
from fastapi import FastAPI

app = FastAPI()
DB_DSN = os.getenv("DATABASE_URL")

@app.on_event("startup")
async def startup():
    app.state.pool = await asyncpg.create_pool(dsn=DB_DSN, min_size=2, max_size=20)
