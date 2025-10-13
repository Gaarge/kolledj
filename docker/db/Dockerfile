FROM postgres:15

# Ставим системный Python + venv (pip отдельно не нужен — venv подтянет ensurepip)
RUN apt-get update && apt-get install -y --no-install-recommends \
      python3 python3-venv \
    && rm -rf /var/lib/apt/lists/*

# Создаем виртуальное окружение и ставим зависимости туда
RUN python3 -m venv /opt/venv
ENV PATH="/opt/venv/bin:$PATH"
RUN pip install --upgrade pip \
 && pip install --no-cache-dir pandas openpyxl psycopg2-binary

# Кладём импортёр и Excel
WORKDIR /app
COPY import_from_excel.py /app/import_from_excel.py
COPY exel/schedule.xlsx /app/excel/schedule.xlsx
