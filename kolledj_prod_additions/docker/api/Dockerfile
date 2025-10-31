FROM python:3.11-slim

# Security: non-root
RUN useradd -m appuser
WORKDIR /app

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PIP_NO_CACHE_DIR=1

# System deps
RUN apt-get update && apt-get install -y --no-install-recommends build-essential \
    && rm -rf /var/lib/apt/lists/*

# Dependencies
COPY requirements.txt .
RUN pip install --upgrade pip && pip install -r requirements.txt

# Copy app
COPY main.py ./

# Drop privileges
USER appuser

EXPOSE 8000
# Use uvloop/httptools, allow workers via env
ENV UVICORN_WORKERS=1
CMD ["/bin/sh","-lc","uvicorn main:app --host 0.0.0.0 --port 8000 --loop uvloop --http httptools --proxy-headers --workers ${UVICORN_WORKERS}"]
