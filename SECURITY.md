# Security

## Reporting a vulnerability

Email api@nums.world with "causilo-db security" in the subject, and please do
not open a public issue. Include the version, the database and the path you used
(command line, PostgreSQL function, Snowflake procedure, DuckDB or Spark) and the
steps to reproduce. Never send a real API key or database password; if one was
exposed, revoke it first.

We aim to acknowledge a report within 3 business days and will tell you when a
fix is released. Only the latest release receives security fixes.

## What to know when deploying

- The target and feature columns of the rows being predicted, and of up to
  50,000 filled rows, are sent over HTTPS to `api.nums.world`. Do not point
  causilo-db at data you may not send to a third party.
- `--where` and the DuckDB `source` argument are passed to the database as SQL.
  Do not build them from untrusted input.
- PostgreSQL: `causilo.predict` is written in the untrusted language
  `plpython3u` and must be installed by a superuser. It reads tables with the
  caller's privileges. `SET causilo.api_key` shows in `pg_stat_activity` and,
  with statement logging on, in the server log.
- Snowflake: the procedure runs with the caller's rights. Every role granted
  USAGE on it spends the key held in the `CAUSILO.API_KEY` secret.
- Snowflake Native App: `CORE.PREDICT` runs with the app's rights and reads
  every table granted to the app, so every role granted `APP_USER` or
  `APP_ADMIN` can predict from those tables and spends the key held in the
  app's `CAUSILO_API_KEY` configuration.
- The `CAUSILO_ENDPOINT` environment variable sends calls, and the key, to
  another host. Leave it unset in production.
