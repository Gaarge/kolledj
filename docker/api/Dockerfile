FROM python:3.11-slim

ENV PYTHONDONTWRITEBYTECODE=1 PYTHONUNBUFFERED=1 PIP_NO_CACHE_DIR=1
WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY main.py ./

# безопасность: запускаем не от root
RUN useradd -m appuser
USER appuser

EXPOSE 8000
# gunicorn + uvicorn workers (подберите workers под CPU pod'а)
CMD ["gunicorn","-k","uvicorn.workers.UvicornWorker","main:app","--bind","0.0.0.0:8000","--workers","3","--timeout","30","--keep-alive","5"]
