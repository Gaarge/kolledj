DROP TABLE IF EXISTS schedule;
DROP TABLE IF EXISTS groups;

CREATE TABLE schedule (
    id         INTEGER PRIMARY KEY AUTOINCREMENT,
    group_name TEXT,
    day_of_week TEXT,
    pair_number INTEGER,
    time_start  TEXT,
    time_end    TEXT,
    subject     TEXT,
    teacher     TEXT,
    room        TEXT,
    week_type   TEXT
);

CREATE TABLE groups (
    name TEXT PRIMARY KEY
);
