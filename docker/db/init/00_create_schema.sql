-- Таблица расписания по дням недели (1=Пн .. 7=Вс)
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

-- Быстрый поиск
CREATE INDEX IF NOT EXISTS idx_weekday_schedule_group_day
  ON weekday_schedule (group_name, weekday);

-- При желании можно оставить старую таблицу schedule как «наследие», но она больше не используется.
-- DROP TABLE IF EXISTS schedule;
