# PostgreSQL 17 with the http extension (pgsql-http 1.6.1), for trying
# sql/postgres/install_http.sql locally: no Python in the database.
# 1.6.1 is the oldest version the installer accepts, and the one hosted
# services commonly ship. Local trials only. Build from the repository root:
#   docker build -f docker/postgres-http.Dockerfile -t causilo-postgres-http .
#   docker run -e POSTGRES_PASSWORD=pw -p 127.0.0.1:5432:5432 causilo-postgres-http
#   psql -h 127.0.0.1 -U postgres, then: SET causilo.api_key = 'csl_...';
FROM postgres:17-bookworm
ARG PGSQL_HTTP_VERSION=1.6.1
RUN apt-get update \
    && apt-get install -y --no-install-recommends ca-certificates curl libcurl4 \
       build-essential libcurl4-openssl-dev postgresql-server-dev-17 \
    && curl -fsSL "https://github.com/pramsey/pgsql-http/archive/refs/tags/v${PGSQL_HTTP_VERSION}.tar.gz" \
       | tar -xz -C /tmp \
    && make -C "/tmp/pgsql-http-${PGSQL_HTTP_VERSION}" \
    && make -C "/tmp/pgsql-http-${PGSQL_HTTP_VERSION}" install \
    && rm -rf "/tmp/pgsql-http-${PGSQL_HTTP_VERSION}" \
    && apt-get purge -y --auto-remove build-essential libcurl4-openssl-dev postgresql-server-dev-17 \
    && rm -rf /var/lib/apt/lists/*
COPY sql/postgres/install_http.sql /docker-entrypoint-initdb.d/10-causilo.sql
