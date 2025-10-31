# Production hardening for *kolledj*

## 1) PgBouncer
- Apply `k8s/pgbouncer.yaml` to deploy PgBouncer with **transaction pooling**.
- Make sure the API uses `DATABASE_URL=postgresql://schedule_user:schedule_pass@pgbouncer:6432/schedule_db`.

## 2) FastAPI + asyncpg (PgBouncer-safe)
- Replace your `get_pool()` in `docker/api/main.py` with the snippet in `snippets/api_pool_patch.txt`.
- This disables server-side prepared statements (`statement_cache_size=0`) which break with PgBouncer transaction pooling.
- Also adds connection + command timeouts and a graceful pool shutdown handler.

## 3) API container
- Use the optimized `docker/api/Dockerfile`. It runs Uvicorn with `uvloop`/`httptools`. Control worker count with `UVICORN_WORKERS` env (e.g. 2-4).

## 4) Frontend Nginx
- Replace `docker/frontend/nginx.conf` with the optimized one (gzip + long-lived caching for static assets).

## 5) Kubernetes
- In your `k8s/api.yaml`:
  - Ensure `DATABASE_URL` points to `pgbouncer:6432/schedule_db` (not `/postgres`).
  - Add resource requests/limits and keep your HPA (already provided).

## 6) Load testing
- Run `k6 run snippets/k6_load_test.js -e BASE_URL=http://<your-host>` and check p90/p99.

## 7) Security
- Rotate the Telegram bot token in `k8s/secret.yaml` (never commit real tokens). Use a sealed secret or external secret manager.

