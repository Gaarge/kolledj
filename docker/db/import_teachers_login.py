#!/usr/bin/env python3
# -*- coding: utf-8 -*-
import os, sys
import pandas as pd
import psycopg2

EXCEL_PATH = os.getenv("TEACHERS_EXCEL_PATH", "/app/excel/teachers.xlsx")

def get_conn():
    url = os.getenv("DATABASE_URL")
    if url:
        return psycopg2.connect(url)
    # Fallback к переменным из контейнера Postgres
    return psycopg2.connect(
        dbname=os.getenv("POSTGRES_DB", "schedule_db"),
        user=os.getenv("POSTGRES_USER", "schedule_user"),
        password=os.getenv("POSTGRES_PASSWORD", "schedule_pass"),
        host=os.getenv("POSTGRES_HOST", "127.0.0.1"),
        port=int(os.getenv("POSTGRES_PORT", "5432"))
    )

def norm(s):
    return (str(s) if s is not None else "").strip()

def main():
    if not os.path.exists(EXCEL_PATH):
        print(f"[users-import] file not found: {EXCEL_PATH}", file=sys.stderr)
        return

    df = pd.read_excel(EXCEL_PATH)
    # Нормализуем названия колонок (снятие регистра/пробелов)
    cols = {c.lower().strip(): c for c in df.columns}
    fio_col     = cols.get("фио сотрудника")
    user_col    = cols.get("логин в stud_8")
    pass_col    = cols.get("пароль в stud_8")

    if not (fio_col and user_col and pass_col):
        raise RuntimeError("Не найдены необходимые колонки: 'ФИО сотрудника', 'Логин в stud_8', 'Пароль в stud_8'")

    conn = get_conn()
    conn.autocommit = False
    with conn, conn.cursor() as cur:
        cur.execute("CREATE EXTENSION IF NOT EXISTS pgcrypto;")
        for _, row in df.iterrows():
            fio = norm(row[fio_col])
            username = norm(row[user_col])
            password = norm(row[pass_col])
            if not (fio and username and password):
                continue
            # UPSERT: по username
            cur.execute("""
                INSERT INTO users (username, password_hash, role, full_name)
                VALUES (%s, crypt(%s, gen_salt('bf')), 'teacher', %s)
                ON CONFLICT (username) DO UPDATE
                SET password_hash = EXCLUDED.password_hash,
                    role = 'teacher',
                    full_name = EXCLUDED.full_name;
            """, (username, password, fio))
    conn.commit()
    print("[users-import] done")

if __name__ == "__main__":
    main()
