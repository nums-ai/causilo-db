# The Cloud Run service behind the BigQuery remote function.
# Build from the repository root: docker build -f bigquery/Dockerfile .
FROM python:3.12-slim

ENV PYTHONUNBUFFERED=1 PIP_NO_CACHE_DIR=1 PIP_DISABLE_PIP_VERSION_CHECK=1
WORKDIR /app

COPY pyproject.toml README.md LICENSE /src/
COPY causilo_db /src/causilo_db
COPY bigquery/requirements.txt /app/requirements.txt
RUN pip install /src -r /app/requirements.txt && rm -rf /src

COPY bigquery/main.py /app/main.py
# gunicorn keeps a control socket under $HOME, so the user needs one.
RUN useradd --uid 10001 --create-home app
USER app

# One request at a time: each call holds a whole table in memory. gunicorn's
# own timeout is off; Cloud Run's --timeout ends a request.
CMD exec gunicorn --bind ":${PORT:-8080}" --workers 1 --threads 1 --timeout 0 main:app
