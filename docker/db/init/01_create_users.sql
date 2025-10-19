-- 01_create_users.sql
CREATE EXTENSION IF NOT EXISTS pgcrypto;

CREATE TABLE IF NOT EXISTS users (
    id SERIAL PRIMARY KEY,
    username TEXT UNIQUE NOT NULL,
    password_hash TEXT NOT NULL,
    role TEXT NOT NULL CHECK (role IN ('student','teacher','admin'))
);

-- Тестовые аккаунты (пароли хэшируются через bcrypt/pgcrypto):
-- student1 / studentpass
INSERT INTO users (username, password_hash, role) VALUES
('student1', crypt('studentpass', gen_salt('bf')), 'student')
ON CONFLICT (username) DO NOTHING;

-- teacher1 / teacherpass
INSERT INTO users (username, password_hash, role) VALUES
('teacher1', crypt('teacherpass', gen_salt('bf')), 'teacher')
ON CONFLICT (username) DO NOTHING;

-- admin1 / adminpass
INSERT INTO users (username, password_hash, role) VALUES
('admin1', crypt('adminpass', gen_salt('bf')), 'admin')
ON CONFLICT (username) DO NOTHING;
