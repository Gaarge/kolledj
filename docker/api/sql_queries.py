# docker/api/sql_queries.py

from datetime import date as Date, timedelta
from typing import Optional, Tuple

def week_bounds(monday_iso: str) -> Tuple[Date, Date]:
    """
    Преобразует monday 'YYYY-MM-DD' в границы недели [monday..sunday].
    """
    m = Date.fromisoformat(monday_iso)
    start = m
    end = m + timedelta(days=6)
    return start, end

def parity_for(date_obj: Date, anchor_str: Optional[str]) -> Optional[str]:
    """
    Возвращает 'odd' либо 'even' исходя из якоря ODD_WEEK_ANCHOR (понедельник «нечётной» недели).
    Если anchor_str пустой/None — вернёт None, и на уровне SQL фильтра чётности не будет.
    """
    if not anchor_str:
        return None
    anchor = Date.fromisoformat(anchor_str)
    delta_days = (date_obj - anchor).days
    # если anchor — понедельник нечётной недели, то anchor..anchor+6 — нечётная
    # diff // 7 == 0 -> odd; 1 -> even; и т.д.
    return "odd" if (delta_days // 7) % 2 == 0 else "even"

WEEK_QUERY = """
WITH base AS (
  SELECT 
    group_name,
    teacher,
    day_of_week::int AS day_of_week,
    pair_number::int AS pair_number,
    time_start,
    time_end,
    subject,
    room,
    NULL::date AS edit_date,
    NULL::text AS week_type,   -- у базового расписания нет чётности
    'base'::text AS src
  FROM weekday_schedule
  WHERE
    (
      ($1::text IS NOT NULL AND group_name = $1)
      OR
      ($2::text IS NOT NULL AND teacher = $2)
    )
    AND day_of_week BETWEEN 1 AND 7
),
weekly AS (
  SELECT
    group_name,
    teacher,
    day_of_week::int AS day_of_week,
    pair_number::int AS pair_number,
    time_start,
    time_end,
    subject,
    room,
    NULL::date AS edit_date,
    week_type::text AS week_type,
    'weekly'::text AS src
  FROM weekly_edits
  WHERE
    (
      ($1::text IS NOT NULL AND group_name = $1)
      OR
      ($2::text IS NOT NULL AND teacher   = $2)
    )
    -- если фильтр чётности передан, берём совпадающие записи и 'all'
    AND (
      $5::text IS NULL
      OR week_type = $5::text
      OR week_type = 'all'
    )
),
once AS (
  SELECT
    group_name,
    teacher,
    EXTRACT(ISODOW FROM edit_date)::int AS day_of_week,
    pair_number::int AS pair_number,
    time_start,
    time_end,
    subject,
    room,
    edit_date,
    NULL::text AS week_type,
    'once'::text AS src
  FROM once_edits
  WHERE
    (
      ($1::text IS NOT NULL AND group_name = $1)
      OR
      ($2::text IS NOT NULL AND teacher   = $2)
    )
    AND edit_date BETWEEN $3::date AND $4::date
)
, unioned AS (
  SELECT * FROM once
  UNION ALL
  SELECT * FROM weekly
  UNION ALL
  SELECT * FROM base
)
, ranked AS (
  SELECT
    *,
    ROW_NUMBER() OVER (
      PARTITION BY
        COALESCE(NULLIF(group_name, ''), NULLIF(teacher, '')),
        day_of_week,
        pair_number
      ORDER BY
        CASE src WHEN 'once' THEN 1 WHEN 'weekly' THEN 2 ELSE 3 END
    ) AS rn
  FROM unioned
)
SELECT
  group_name,
  teacher,
  day_of_week,
  pair_number,
  time_start,
  time_end,
  subject,
  room,
  edit_date,
  week_type,
  src
FROM ranked
WHERE rn = 1
ORDER BY day_of_week, pair_number;
"""
