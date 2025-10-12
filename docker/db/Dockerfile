FROM postgres:15
# Кладем SQL в место, откуда официальный entrypoint выполнит его при первом старте
COPY init/*.sql /docker-entrypoint-initdb.d/