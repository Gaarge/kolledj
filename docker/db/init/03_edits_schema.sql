-- 03_edits_schema.sql

-- enum для чётности недель
DO $$
BEGIN
  IF NOT EXISTS (SELECT 1 FROM pg_type WHERE typname = 'week_type_enum') THEN
    CREATE TYPE week_type_enum AS ENUM ('all','even','odd');
  END IF;
END$$;

-- Разовые правки (на конкретную дату)
CREATE TABLE IF NOT EXISTS once_edits (
  id           BIGSERIAL PRIMARY KEY,
  group_name   TEXT NOT NULL,
  edit_date    DATE NOT NULL,
  pair_number  INTEGER NOT NULL CHECK (pair_number > 0 AND pair_number <= 20),
  subject      TEXT,
  teacher      TEXT,
  room         TEXT,
  time_start   TEXT,
  time_end     TEXT,
  deleted      BOOLEAN NOT NULL DEFAULT FALSE
);
CREATE INDEX IF NOT EXISTS idx_once_edits_group_date
  ON once_edits (group_name, edit_date);

-- Еженедельные правки (по дню недели и чётности)
CREATE TABLE IF NOT EXISTS weekly_edits (
  id           BIGSERIAL PRIMARY KEY,
  group_name   TEXT NOT NULL,
  day_of_week  INTEGER NOT NULL CHECK (day_of_week BETWEEN 1 AND 7), -- ISO: Пн=1..Вс=7
  pair_number  INTEGER NOT NULL CHECK (pair_number > 0 AND pair_number <= 20),
  week_type    week_type_enum NOT NULL DEFAULT 'all',
  subject      TEXT,
  teacher      TEXT,
  room         TEXT,
  time_start   TEXT,
  time_end     TEXT,
  deleted      BOOLEAN NOT NULL DEFAULT FALSE
);
CREATE INDEX IF NOT EXISTS idx_weekly_edits_group_day
  ON weekly_edits (group_name, day_of_week, week_type);
