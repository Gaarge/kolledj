-- 02_add_normalized_column_and_indexes.sql

-- Нормализованное имя группы (совпадает с выражением в WHERE)
ALTER TABLE IF NOT EXISTS weekday_schedule
  ADD COLUMN IF NOT EXISTS normalized_group_name TEXT
  GENERATED ALWAYS AS (
    regexp_replace(
      lower(translate(group_name,
        'ABCEHKMOPTXYabcehkmoptxy',
        'АВСЕНКМОРТХУавсенкмортху')),
      '[^0-9a-zа-яё]+','','g')
  ) STORED;

-- Индексы под частые запросы
CREATE INDEX IF NOT EXISTS idx_ws_norm_group_weekday_type
  ON weekday_schedule (normalized_group_name, weekday, week_type);

CREATE INDEX IF NOT EXISTS idx_ws_teacher_day_type
  ON weekday_schedule (weekday, week_type, (lower(trim(teacher))));

CREATE INDEX IF NOT EXISTS idx_once_group_date_pair
  ON once_edits (group_name, edit_date, pair_number);

CREATE INDEX IF NOT EXISTS idx_weekly_group_day_type_pair
  ON weekly_edits (group_name, day_of_week, week_type, pair_number);
