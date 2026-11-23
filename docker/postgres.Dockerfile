# PostgreSQL 16 with PL/Python and causilo-db, for trying causilo.predict locally.
# Local trials only. Build from the repository root:
#   docker build -f docker/postgres.Dockerfile -t causilo-postgres .
#   docker run -e POSTGRES_PASSWORD=pw -p 127.0.0.1:5432:5432 causilo-postgres
#   psql -h 127.0.0.1 -U postgres, then: SET causilo.api_key = 'csl_...';
FROM postgres:16-bookworm
RUN apt-get update && apt-get install -y --no-install-recommends postgresql-plpython3-16 python3-pip \
    && rm -rf /var/lib/apt/lists/*
COPY pyproject.toml README.md LICENSE /src/
COPY causilo_db /src/causilo_db
RUN pip3 install --no-cache-dir --break-system-packages /src
COPY sql/postgres/install.sql /docker-entrypoint-initdb.d/10-causilo.sql
