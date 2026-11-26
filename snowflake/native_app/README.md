# Causilo for Snowflake

This app predicts the blank cells of a column in one of your tables. Rows where
the column has a value are the examples, rows where it is blank are predicted,
and the predictions come back as the result of a procedure call. Your table is
never modified.

The predictions are made by Causilo, the hosted tabular prediction API from
Nums AI (`https://api.nums.world`, documentation at
[docs.nums.world](https://docs.nums.world)). You need your own Causilo API key,
and every call counts against that key's account.

What leaves your account: the target and feature columns of the blank rows and
of up to 50,000 filled rows, sent over HTTPS to `api.nums.world`. The id column
is not sent. The app stores neither the data nor the predictions.

## Set it up once

The examples call the installed app `CAUSILO`; use the name you installed it
under.

1. Approve the app's external access. When installed, the app requests the
   account-level CREATE EXTERNAL ACCESS INTEGRATION privilege, which allows it
   to create external access integrations in your account. It uses it to
   create one integration, whose network rule allows `api.nums.world` and no
   other host, so the app can reach that host only. Until the `CAUSILO_API`
   specification is approved, every call fails.
   In Snowsight, open the app and approve the request, or as a role with
   MANAGE APPLICATION SPECIFICATIONS:

   ```sql
   SHOW SPECIFICATIONS IN APPLICATION CAUSILO;
   ALTER APPLICATION CAUSILO APPROVE SPECIFICATION CAUSILO_API SEQUENCE_NUMBER = 1;  -- the number SHOW gave
   ```

2. Set your API key. It is a sensitive value: Snowflake redacts it from query
   history and from `SHOW CONFIGURATIONS`, and only the app can read it.

   ```sql
   GRANT APPLICATION ROLE CAUSILO.APP_ADMIN TO ROLE <admin_role>;
   ALTER APPLICATION CAUSILO SET CONFIGURATION CAUSILO_API_KEY VALUE = 'csl_...';
   ```

3. Let the app read the tables or views you want to predict. The app reads
   with its own rights, so it sees only what you grant it here:

   ```sql
   GRANT USAGE ON DATABASE SALES TO APPLICATION CAUSILO;
   GRANT USAGE ON SCHEMA SALES.PUBLIC TO APPLICATION CAUSILO;
   GRANT SELECT ON TABLE SALES.PUBLIC.CUSTOMERS TO APPLICATION CAUSILO;
   ```

4. Let other roles call it:

   ```sql
   GRANT APPLICATION ROLE CAUSILO.APP_USER TO ROLE <analyst_role>;
   ```

## Predict

```sql
CALL CAUSILO.CORE.PREDICT('SALES.PUBLIC.CUSTOMERS', 'CHURN', 'CUSTOMER_ID');
CREATE TABLE SALES.PUBLIC.CHURN_PREDICTION AS SELECT * FROM TABLE(RESULT_SCAN(LAST_QUERY_ID()));
```

Name the table in full, as `DATABASE.SCHEMA.TABLE`. The third argument, the id
column, is optional; without it each row is identified by `ROW_NUMBER`, its
0-based position in what was read.

Every column except the target and the id is a feature. To choose the columns
or the rows, create a view and grant SELECT on the view instead.

## The result

One row per blank cell, with the id column:

- a number column (regression): the target column holds the predicted value
  (the median), and `<COLUMN>_P25`, `<COLUMN>_P50`, `<COLUMN>_P75` hold the
  predicted quartiles;
- anything else (classification): the most likely class,
  `<COLUMN>_PROBABILITY` for it, and `<COLUMN>_PROB_<CLASS>` for every class.

A numeric column with 20 or fewer distinct whole values is read as classes. A
text column with more than 200 distinct values is refused, since it is more
likely a name than a class. Dates and timestamps are sent as seconds since 1970.
The filled rows are sampled down to at most 50,000, fewer for a wide table, and
a large table is sent in several calls. Each call is charged when it
completes; if a later call fails, the predictions of the calls before it are
not returned, and the error says how many calls had been charged.

## Who can do what

- `APP_ADMIN` sets the API key and can call `CORE.PREDICT`.
- `APP_USER` can call `CORE.PREDICT`.

Both roles can predict from every table you granted to the app, and every call
spends the key. Grant the application roles only to roles that may read those
tables.

## Help

Email api@nums.world. Never send your API key; if it was exposed, revoke it
first. The source is at
[github.com/nums-ai/causilo-db](https://github.com/nums-ai/causilo-db).
