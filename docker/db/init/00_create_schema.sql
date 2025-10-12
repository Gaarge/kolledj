-- Схема расписания
CREATE TABLE IF NOT EXISTS schedule (
  id SERIAL PRIMARY KEY,
  date DATE NOT NULL,
  pair_number SMALLINT NOT NULL,
  time_start TIME NOT NULL,
  time_end TIME NOT NULL,
  subject TEXT NOT NULL,
  session_type VARCHAR(16) NOT NULL,
  room VARCHAR(32),
  teacher VARCHAR(128),
  group_name VARCHAR(32) NOT NULL,
  created_at TIMESTAMPTZ DEFAULT now(),
  UNIQUE (group_name, date, pair_number)
);

-- Примеры данных для группы "И-22"
INSERT INTO schedule (date, pair_number, time_start, time_end, subject, session_type, room, teacher, group_name) VALUES
('2025-10-10', 1, '08:30', '10:00', 'Математика', 'лекция', 'А-101', 'Иванова И.И.', 'И-22'),
('2025-10-10', 2, '10:10', '11:40', 'Программирование', 'практика', 'Б-202', 'Петров П.П.', 'И-22'),
('2025-10-10', 3, '12:20', '13:50', 'Физика', 'лекция', 'А-103', 'Сидоров С.С.', 'И-22'),

('2025-10-12', 1, '09:00', '10:30', 'Электроника', 'лекция', 'В-301', 'Кузнецов К.К.', 'И-22'),
('2025-10-12', 2, '10:40', '12:10', 'История', 'семинар', 'С-110', 'Смирнова А.А.', 'И-22');