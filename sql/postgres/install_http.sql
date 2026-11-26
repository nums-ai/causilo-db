-- Causilo for PostgreSQL without Python in the database:
--   SELECT * FROM causilo.predict('customers', 'churn', 'customer_id');
--
-- The same function as install.sql, written in PL/pgSQL on the http extension
-- (pgsql-http, 1.6 or later) instead of PL/Python. For a server where
-- plpython3u is not allowed but the http extension is. Where the extension
-- must live in a particular schema, create it there first, for example
--   CREATE EXTENSION http WITH SCHEMA extensions;
-- and this file leaves it where it is.
--
-- The key is per session and never stored in a table:
--   SET causilo.api_key = 'csl_...';
-- The SET statement is plain SQL text, so it shows in pg_stat_activity and,
-- with log_statement on, in the server log. Optional settings:
--   causilo.endpoint    default https://api.nums.world
--   causilo.timeout_ms  how long one call may take, default 900000 (15 minutes)
--
-- What differs from install.sql is listed in the README, under
-- "PostgreSQL function without Python".

CREATE EXTENSION IF NOT EXISTS http;

DO $$
DECLARE
    v text := (SELECT extversion FROM pg_extension WHERE extname = 'http');
BEGIN
    IF string_to_array(regexp_replace(v, '[^0-9.].*$', ''), '.')::int[] < ARRAY[1, 6] THEN
        RAISE EXCEPTION 'causilo.predict needs the http extension 1.6 or later, which can send a binary body; '
                        'version % is installed. ALTER EXTENSION http UPDATE, then run this file again.', v;
    END IF;
END $$;

CREATE SCHEMA IF NOT EXISTS causilo;

-- Byte helpers. The request carries two Parquet files, built here: one row
-- group, one uncompressed PLAIN page per column, every column OPTIONAL.

CREATE OR REPLACE FUNCTION causilo._b(n int) RETURNS bytea
LANGUAGE sql IMMUTABLE STRICT AS $$ SELECT set_byte('\x00'::bytea, 0, n) $$;

-- Unsigned LEB128, the varint of Thrift's compact protocol.
CREATE OR REPLACE FUNCTION causilo._varint(n bigint) RETURNS bytea
LANGUAGE plpgsql IMMUTABLE STRICT AS $$
DECLARE
    out bytea := ''::bytea;
BEGIN
    IF n < 0 THEN
        RAISE EXCEPTION 'causilo._varint: % is negative', n;
    END IF;
    LOOP
        IF n < 128 THEN
            RETURN out || causilo._b(n::int);
        END IF;
        out := out || causilo._b(((n & 127) | 128)::int);
        n := n >> 7;
    END LOOP;
END $$;

-- A Thrift i32 or i64: zigzag, then varint. Only non-negative values are written.
CREATE OR REPLACE FUNCTION causilo._ti(n bigint) RETURNS bytea
LANGUAGE sql IMMUTABLE STRICT AS $$ SELECT causilo._varint(n * 2) $$;

-- A Thrift string.
CREATE OR REPLACE FUNCTION causilo._ts(s text) RETURNS bytea
LANGUAGE sql IMMUTABLE STRICT AS $$
    SELECT causilo._varint(octet_length(convert_to(s, 'UTF8'))) || convert_to(s, 'UTF8')
$$;

-- A Thrift list header.
CREATE OR REPLACE FUNCTION causilo._tl(size int, elem int) RETURNS bytea
LANGUAGE sql IMMUTABLE STRICT AS $$
    SELECT CASE WHEN size < 15 THEN causilo._b((size << 4) | elem)
                ELSE causilo._b(240 | elem) || causilo._varint(size) END
$$;

-- Little-endian values, as Parquet's PLAIN encoding stores them.
CREATE OR REPLACE FUNCTION causilo._le4(n int) RETURNS bytea
LANGUAGE sql IMMUTABLE STRICT AS $$
    SELECT substr(int4send(n), 4, 1) || substr(int4send(n), 3, 1)
        || substr(int4send(n), 2, 1) || substr(int4send(n), 1, 1)
$$;

CREATE OR REPLACE FUNCTION causilo._i8(n bigint) RETURNS bytea
LANGUAGE sql IMMUTABLE STRICT AS $$
    SELECT substr(int8send(n), 8, 1) || substr(int8send(n), 7, 1) || substr(int8send(n), 6, 1)
        || substr(int8send(n), 5, 1) || substr(int8send(n), 4, 1) || substr(int8send(n), 3, 1)
        || substr(int8send(n), 2, 1) || substr(int8send(n), 1, 1)
$$;

CREATE OR REPLACE FUNCTION causilo._f8(x double precision) RETURNS bytea
LANGUAGE sql IMMUTABLE STRICT AS $$
    SELECT substr(float8send(x), 8, 1) || substr(float8send(x), 7, 1) || substr(float8send(x), 6, 1)
        || substr(float8send(x), 5, 1) || substr(float8send(x), 4, 1) || substr(float8send(x), 3, 1)
        || substr(float8send(x), 2, 1) || substr(float8send(x), 1, 1)
$$;

CREATE OR REPLACE FUNCTION causilo._str(s text) RETURNS bytea
LANGUAGE sql IMMUTABLE STRICT AS $$
    SELECT causilo._le4(octet_length(convert_to(s, 'UTF8'))) || convert_to(s, 'UTF8')
$$;

-- A number as Python prints it: 33.0 rather than 33.
CREATE OR REPLACE FUNCTION causilo._pyfloat(x double precision) RETURNS text
LANGUAGE sql IMMUTABLE STRICT AS $$
    SELECT CASE WHEN x::text ~ '^-?[0-9]+$' THEN x::text || '.0' ELSE x::text END
$$;

-- One Parquet file. Per column: its name, physical type (0 BOOLEAN, 2 INT64,
-- 5 DOUBLE, 6 BYTE_ARRAY as UTF-8 text), the definition levels as bit-packed
-- bytes (one bit per row) and the PLAIN values of its non-blank rows.
CREATE OR REPLACE FUNCTION causilo._parquet(names text[], ptypes int[], n_rows bigint, defs bytea[], vals bytea[])
RETURNS bytea LANGUAGE plpgsql IMMUTABLE AS $$
DECLARE
    k int := array_length(names, 1);
    pages bytea[] := '{}';
    schema bytea;
    chunks bytea := ''::bytea;
    levels bytea;
    data bytea;
    header bytea;
    offset_ bigint := 4;
    total bigint := 0;
    size bigint;
    footer bytea;
BEGIN
    schema := causilo._b(72) || causilo._ts('schema') || causilo._b(21) || causilo._ti(k) || causilo._b(0);
    FOR i IN 1..k LOOP
        levels := causilo._varint(((octet_length(defs[i])::bigint) << 1) | 1) || defs[i];
        data := causilo._le4(octet_length(levels)) || levels || coalesce(vals[i], ''::bytea);
        size := octet_length(data);
        -- PageHeader: DATA_PAGE, sizes, DataPageHeader(num_values, PLAIN, RLE, RLE).
        header := causilo._b(21) || causilo._ti(0) || causilo._b(21) || causilo._ti(size)
               || causilo._b(21) || causilo._ti(size)
               || causilo._b(44) || causilo._b(21) || causilo._ti(n_rows) || causilo._b(21) || causilo._ti(0)
               || causilo._b(21) || causilo._ti(3) || causilo._b(21) || causilo._ti(3) || causilo._b(0)
               || causilo._b(0);
        pages := array_append(pages, header || data);
        size := size + octet_length(header);
        -- ColumnChunk(file_offset, ColumnMetaData(type, [PLAIN, RLE], [name],
        -- UNCOMPRESSED, num_values, sizes, data_page_offset)).
        chunks := chunks || causilo._b(38) || causilo._ti(offset_) || causilo._b(28)
               || causilo._b(21) || causilo._ti(ptypes[i])
               || causilo._b(25) || causilo._tl(2, 5) || causilo._ti(0) || causilo._ti(3)
               || causilo._b(25) || causilo._tl(1, 8) || causilo._ts(names[i])
               || causilo._b(21) || causilo._ti(0)
               || causilo._b(22) || causilo._ti(n_rows)
               || causilo._b(22) || causilo._ti(size) || causilo._b(22) || causilo._ti(size)
               || causilo._b(38) || causilo._ti(offset_)
               || causilo._b(0) || causilo._b(0);
        -- SchemaElement(type, OPTIONAL, name[, UTF8]).
        schema := schema || causilo._b(21) || causilo._ti(ptypes[i]) || causilo._b(37) || causilo._ti(1)
               || causilo._b(24) || causilo._ts(names[i])
               || CASE WHEN ptypes[i] = 6 THEN causilo._b(37) || causilo._ti(0) ELSE ''::bytea END
               || causilo._b(0);
        offset_ := offset_ + size;
        total := total + size;
    END LOOP;
    -- FileMetaData(version 1, schema, num_rows, [RowGroup(columns, total_byte_size, num_rows)]).
    footer := causilo._b(21) || causilo._ti(1)
           || causilo._b(25) || causilo._tl(k + 1, 12) || schema
           || causilo._b(22) || causilo._ti(n_rows)
           || causilo._b(25) || causilo._tl(1, 12)
           || causilo._b(25) || causilo._tl(k, 12) || chunks
           || causilo._b(22) || causilo._ti(total) || causilo._b(22) || causilo._ti(n_rows) || causilo._b(0)
           || causilo._b(0);
    RETURN (SELECT 'PAR1'::bytea || string_agg(p, ''::bytea ORDER BY i) || footer
                   || causilo._le4(octet_length(footer)) || 'PAR1'::bytea
            FROM unnest(pages) WITH ORDINALITY AS u(p, i));
END $$;

-- POST /v1/cancel for the call sent with cancel_key, waiting at most
-- timeout_ms (5 seconds when NULL) for the answer. An error of the request is
-- swallowed; a cancel from the client or statement_timeout is not, so the
-- caller is never kept waiting past it.
CREATE OR REPLACE FUNCTION causilo._cancel(ext text, endpoint text, api_key text, cancel_key text, timeout_ms int)
RETURNS void LANGUAGE plpgsql AS $$
BEGIN
    PERFORM set_config('http.timeout_msec', coalesce(timeout_ms, 5000)::text, true);
    EXECUTE format(
        'SELECT 1 FROM %1$I.http(ROW(''POST'', $1,
                                 ARRAY[%1$I.http_header(''Authorization'', ''Bearer '' || $2),
                                       %1$I.http_header(''X-Causilo-Cancel-Key'', $3)],
                                 ''text/plain'', '''')::%1$I.http_request)', ext)
    USING endpoint || '/v1/cancel', api_key, cancel_key;
EXCEPTION WHEN OTHERS THEN
    NULL;
END $$;

-- One POST /v1/predict. Returns the parsed answer of a 200; raises otherwise.
-- Retries as the Python client does: a failure the server marks retryable, or
-- a 429, 500 or 503 when it does not say, up to three more times after 1, 3
-- and 8 seconds or the Retry-After it sends (at most 30). A 502 or 504 is not
-- retried, and neither is a call that timed out, since the server may have
-- run it and a retry is charged again; a timed-out call is cancelled, which
-- refunds it if it had not started. So is a call cut off by statement_timeout
-- or a cancel from the client.
CREATE OR REPLACE FUNCTION causilo._call(endpoint text, api_key text, body bytea, content_type text)
RETURNS jsonb LANGUAGE plpgsql AS $$
DECLARE
    ext text;
    status int;
    content text;
    retry_after text;
    doc jsonb;
    cancel_key text;
    wait double precision;
    backoff double precision[] := ARRAY[1, 3, 8];
    flag jsonb;
    msg text;
    failed text;
BEGIN
    SELECT n.nspname INTO ext
    FROM pg_extension e JOIN pg_namespace n ON n.oid = e.extnamespace WHERE e.extname = 'http';
    IF ext IS NULL THEN
        RAISE EXCEPTION 'The http extension is not installed: CREATE EXTENSION http;';
    END IF;
    PERFORM set_config('http.timeout_msec',
                       coalesce(nullif(current_setting('causilo.timeout_ms', true), ''), '900000'), true);
    FOR attempt IN 0..3 LOOP
        cancel_key := md5(random()::text || clock_timestamp()::text);
        failed := NULL;
        -- statement_timeout, or a cancel from the client, while the API may
        -- still be running the call: ask it to stop (a refund if it had not
        -- started), waiting at most a few seconds, then go on with the cancel.
        -- A cancel that arrives during http() can make it fail with an error
        -- of its own and be raised only at the next interrupt check, so the
        -- outer block catches it wherever it surfaces, and pg_sleep(0) makes
        -- that check before the block ends.
        BEGIN
            BEGIN
                EXECUTE format(
                    'SELECT r.status, r.content,
                            (SELECT h.value FROM unnest(r.headers) h WHERE lower(h.field) = ''retry-after'' LIMIT 1)
                     FROM %1$I.http(ROW(''POST'', $1,
                                        ARRAY[%1$I.http_header(''Authorization'', ''Bearer '' || $2),
                                              %1$I.http_header(''X-Causilo-Cancel-Key'', $3),
                                              %1$I.http_header(''Accept'', ''application/json'')],
                                        $4, %1$I.bytea_to_text($5))::%1$I.http_request) r', ext)
                INTO status, content, retry_after
                USING endpoint || '/v1/predict', api_key, cancel_key, content_type, body;
            EXCEPTION WHEN OTHERS THEN
                failed := SQLERRM;
            END;
            PERFORM pg_sleep(0);
        EXCEPTION WHEN query_canceled THEN
            PERFORM causilo._cancel(ext, endpoint, api_key, cancel_key, 5000);
            RAISE;
        END;
        IF failed IS NOT NULL THEN
            PERFORM causilo._cancel(ext, endpoint, api_key, cancel_key, 5000);
            RAISE EXCEPTION 'The Causilo API call did not complete (%). It was not retried, since the server may '
                            'have run it and a retry is charged again; it was cancelled, which refunds it if it '
                            'had not started.', failed;
        END IF;
        IF status = 200 THEN
            RETURN content::jsonb;
        END IF;
        BEGIN
            doc := content::jsonb;
            IF jsonb_typeof(doc) <> 'object' THEN
                doc := '{}';
            END IF;
        EXCEPTION WHEN OTHERS THEN
            doc := '{}';
        END;
        msg := format('Causilo API error %s%s: %s%s', status,
                      coalesce(' ' || (doc->>'code'), ''),
                      coalesce(doc->>'detail', doc->>'title', left(content, 300), 'no detail'),
                      coalesce(' (request ' || (doc->>'request_id') || ')', ''));
        flag := doc->'retryable';
        IF status IN (502, 504) OR attempt = 3
           OR (jsonb_typeof(flag) = 'boolean' AND NOT flag::text::boolean)
           OR (jsonb_typeof(flag) IS DISTINCT FROM 'boolean' AND status NOT IN (429, 500, 503)) THEN
            RAISE EXCEPTION '%', msg;
        END IF;
        wait := NULL;
        IF retry_after ~ '^\s*[0-9]+(\.[0-9]+)?\s*$' THEN
            wait := retry_after::double precision;
        ELSIF jsonb_typeof(doc->'retry_after_s') = 'number' THEN
            wait := (doc->>'retry_after_s')::double precision;
        END IF;
        IF wait IS NULL OR wait <= 0 THEN
            wait := backoff[attempt + 1];
        END IF;
        RAISE NOTICE 'causilo: %; trying again in % s', msg, least(wait, 30);
        PERFORM pg_sleep(least(wait, 30));
    END LOOP;
    RAISE EXCEPTION 'The Causilo API call failed.';
END $$;

-- One row per blank cell of target_column in tbl.
--   row_id       the id_column value, or the row's ctid when no id is given
--                (a ctid changes when the row is updated and is not unique across
--                partitions; pass an id to join back)
--   prediction   the predicted value as text (a class, or the median for a number)
--   probability  classification: probability of the predicted class
--   p25/p50/p75  regression: the predicted quantiles
CREATE OR REPLACE FUNCTION causilo.predict(
    tbl regclass,
    target_column text,
    id_column text DEFAULT NULL,
    task text DEFAULT NULL
) RETURNS TABLE (row_id text, prediction text, probability double precision,
                 p25 double precision, p50 double precision, p75 double precision)
LANGUAGE plpgsql
SET extra_float_digits = 1
AS $fn$
DECLARE
    -- The limits of causilo_db/core.py.
    max_cells constant bigint := 7000000;
    max_context constant bigint := 50000;
    class_limit constant int := 20;
    max_inferred_classes constant int := 200;

    api_key text := current_setting('causilo.api_key', true);
    endpoint text := rtrim(coalesce(nullif(current_setting('causilo.endpoint', true), ''),
                                    'https://api.nums.world'), '/');
    asked text := task;
    t_oid oid;
    t_kind text;
    t_col text;
    t_val text;      -- the target as a value: NaN read as blank, like pandas
    t_label text;    -- the target as a class label, the way Python prints it
    t_num text;      -- the target as a number, for a regression
    id_oid oid;
    col record;
    f_names text[] := '{}';
    f_kinds text[] := '{}';
    f_exprs text[] := '{}';
    counts bigint[];
    n_rows bigint;
    n_query bigint;
    n_filled bigint;
    n_classes bigint;
    n_distinct bigint;
    whole boolean;
    notes text[] := '{}';
    names text[] := '{}';
    ptypes int[] := '{}';
    enc text[] := '{}';
    k int;
    budget bigint;
    keep bigint;
    n_context bigint;
    chunk bigint;
    sql text;
    rec record;
    cur bigint := NULL;
    part_n bigint;
    defs bytea[];
    vals bytea[];
    ids text[];
    context_file bytea;
    query_file bytea;
    boundary text;
    params text;
    body bytea;
    answer jsonb;
    classes text[];
    got text[];
    sent bigint := 0;
    n_out bigint;
    calls_done int := 0;
    -- The API answered the current call, which is charged, and its answer is
    -- not yet returned.
    answered boolean := false;
    calls_planned bigint;
    err_state text;
    err_msg text;
BEGIN
    IF coalesce(api_key, '') = '' THEN
        RAISE EXCEPTION 'Set a Causilo API key first: SET causilo.api_key = ''csl_...''';
    END IF;

    -- check_id_column in causilo_db/core.py.
    IF id_column IS NOT NULL THEN
        IF lower(id_column) = lower(target_column) THEN
            RAISE EXCEPTION 'The id column cannot be the target column.';
        END IF;
        IF lower(id_column) = 'row_number'
           OR (left(lower(id_column), length(target_column) + 1) = lower(target_column) || '_'
               AND substr(lower(id_column), length(target_column) + 2) ~ '^(p[0-9]+|probability|prob_.*)$') THEN
            RAISE EXCEPTION 'An id column named % clashes with a result column; use another one.', quote_literal(id_column);
        END IF;
    END IF;

    -- Column types, the way PL/Python and pandas see them.
    FOR col IN
        SELECT a.attname::text AS name,
               CASE WHEN ty.typtype = 'd' THEN ty.typbasetype ELSE a.atttypid END AS oid
        FROM pg_attribute a JOIN pg_type ty ON ty.oid = a.atttypid
        WHERE a.attrelid = tbl AND a.attnum > 0 AND NOT a.attisdropped
        ORDER BY a.attnum
    LOOP
        IF col.name = target_column THEN
            t_oid := col.oid;
        ELSIF col.name = id_column THEN
            id_oid := col.oid;
        ELSE
            f_names := f_names || col.name;
            f_kinds := f_kinds || CASE
                WHEN col.oid IN (20, 21, 23) THEN 'int'
                WHEN col.oid IN (700, 701, 1700) THEN 'float'
                WHEN col.oid = 16 THEN 'bool'
                WHEN col.oid IN (1082, 1114, 1184) THEN 'date'
                ELSE 'text' END;
        END IF;
    END LOOP;
    IF t_oid IS NULL THEN
        RAISE EXCEPTION 'No column named % in %', target_column, tbl;
    END IF;
    IF id_column IS NOT NULL AND id_oid IS NULL THEN
        RAISE EXCEPTION 'No column named % in %', id_column, tbl;
    END IF;
    IF lower(target_column) = 'row_number' THEN
        RAISE EXCEPTION 'A target named ''row_number'' clashes with the result''s row_number column; rename it.';
    END IF;
    IF asked IS NOT NULL AND asked NOT IN ('clf', 'reg') THEN
        RAISE EXCEPTION 'task must be ''clf'' or ''reg''.';
    END IF;

    t_col := 't.' || quote_ident(target_column);
    t_kind := CASE
        WHEN t_oid IN (20, 21, 23) THEN 'int'
        WHEN t_oid IN (700, 701, 1700) THEN 'float'
        WHEN t_oid = 16 THEN 'bool'
        WHEN t_oid IN (1082, 1114, 1184) THEN 'date'
        ELSE 'text' END;
    t_val := CASE t_kind WHEN 'float' THEN format('NULLIF(%s::float8, ''NaN'')', t_col) ELSE t_col END;
    t_label := CASE t_kind
        WHEN 'int' THEN t_val || '::text'
        WHEN 'float' THEN format('CASE WHEN %1$s = trunc(%1$s) AND abs(%1$s) < 1e15 THEN trunc(%1$s)::bigint::text '
                                 'WHEN %1$s = trunc(%1$s) THEN trunc(%1$s)::numeric::text ELSE %1$s::text END', t_val)
        WHEN 'bool' THEN format('CASE WHEN %s THEN ''True'' ELSE ''False'' END', t_val)
        ELSE t_val || '::text' END;

    -- Each feature as a value: NaN and infinite dates read as blank, dates as
    -- seconds since 1970.
    FOR i IN 1..coalesce(array_length(f_names, 1), 0) LOOP
        f_exprs := f_exprs || CASE f_kinds[i]
            WHEN 'float' THEN format('NULLIF(t.%I::float8, ''NaN'')', f_names[i])
            WHEN 'date' THEN format('CASE WHEN isfinite(t.%1$I) THEN extract(epoch FROM t.%1$I)::float8 END', f_names[i])
            ELSE format('t.%I', f_names[i]) END;
    END LOOP;

    -- First read: how many rows, blanks and values each column has, and what
    -- the target looks like.
    sql := format('SELECT ARRAY[count(*), count(%1$s), count(DISTINCT %2$s)%3$s]::bigint[] FROM %4$s t',
                  t_val, t_label,
                  (SELECT coalesce(string_agg(', count(' || e || ')', '' ORDER BY i), '')
                   FROM unnest(f_exprs) WITH ORDINALITY u(e, i)),
                  tbl);
    EXECUTE sql INTO counts;
    n_rows := counts[1];
    n_filled := counts[2];
    n_classes := counts[3];
    n_query := n_rows - n_filled;
    IF n_query = 0 THEN
        RAISE EXCEPTION 'Column % has no blank cells, so there is nothing to predict.', quote_literal(target_column);
    END IF;
    IF n_filled < 2 THEN
        RAISE EXCEPTION 'Column % has % filled rows; at least 2 are needed.', quote_literal(target_column), n_filled;
    END IF;

    -- infer_task in causilo_db/core.py.
    IF asked IS NULL THEN
        IF t_kind IN ('int', 'float') THEN
            EXECUTE format('SELECT count(DISTINCT %1$s), bool_and(abs(%1$s - round(%1$s)) <= 1e-8 + 1e-5 * abs(round(%1$s))) '
                           'FROM %2$s t WHERE %1$s IS NOT NULL', t_val || '::float8', tbl)
            INTO n_distinct, whole;
            task := CASE WHEN n_distinct > class_limit OR NOT whole THEN 'reg' ELSE 'clf' END;
        ELSE
            task := 'clf';
        END IF;
    END IF;

    -- _usable in causilo_db/core.py: which features go, and in what type.
    -- An integer column with a blank arrives in pandas as float, and a boolean
    -- column with a blank as text.
    FOR i IN 1..coalesce(array_length(f_names, 1), 0) LOOP
        IF counts[3 + i] = 0 THEN
            IF f_kinds[i] = 'date' THEN
                notes := notes || format('%s: dates sent as seconds since 1970-01-01', f_names[i]);
            END IF;
            notes := notes || format('%s: dropped, every value is blank', f_names[i]);
            CONTINUE;
        END IF;
        names := names || f_names[i];
        IF f_kinds[i] = 'int' AND counts[3 + i] = n_rows THEN
            ptypes := ptypes || 2;
            enc := enc || format('causilo._i8(%s::int8)', f_exprs[i]);
        ELSIF f_kinds[i] IN ('int', 'float', 'date') THEN
            ptypes := ptypes || 5;
            enc := enc || format('causilo._f8(%s::float8)', f_exprs[i]);
            IF f_kinds[i] = 'date' THEN
                notes := notes || format('%s: dates sent as seconds since 1970-01-01', f_names[i]);
            END IF;
        ELSIF f_kinds[i] = 'bool' AND counts[3 + i] = n_rows THEN
            ptypes := ptypes || 0;
            enc := enc || format('CASE WHEN %1$s THEN ''\x01''::bytea WHEN NOT %1$s THEN ''\x00''::bytea END', f_exprs[i]);
        ELSIF f_kinds[i] = 'bool' THEN
            ptypes := ptypes || 6;
            enc := enc || format('causilo._str(CASE WHEN %1$s THEN ''True'' WHEN NOT %1$s THEN ''False'' END)', f_exprs[i]);
        ELSE
            ptypes := ptypes || 6;
            enc := enc || format('causilo._str(%s::text)', f_exprs[i]);
        END IF;
    END LOOP;
    k := coalesce(array_length(names, 1), 0);
    IF k = 0 THEN
        RAISE EXCEPTION 'No feature columns are left to predict from.';
    END IF;

    IF task = 'clf' THEN
        IF asked IS NULL AND n_classes > max_inferred_classes THEN
            RAISE EXCEPTION 'Column % has % distinct values, which looks more like a name or a code than a class; '
                            'pass task=''clf'' if it really is one.', quote_literal(target_column), n_classes;
        END IF;
        IF asked IS NULL AND t_kind IN ('int', 'float') THEN
            notes := notes || format('%s: whole numbers with %s distinct values, read as classes; '
                                     'pass task=''reg'' if it is a quantity', target_column, n_classes);
        END IF;
        ptypes := ptypes || 6;
        enc := enc || format('causilo._str(%s)', t_label);
    ELSE
        IF t_kind = 'date' THEN
            RAISE EXCEPTION 'Column % is not numeric; task=''reg'' needs numbers.', quote_literal(target_column);
        END IF;
        t_num := CASE t_kind WHEN 'bool' THEN t_val || '::int::float8'
                             WHEN 'text' THEN t_val || '::float8'
                             ELSE t_val || '::float8' END;
        IF t_kind = 'text' THEN
            BEGIN
                EXECUTE format('SELECT count(%s) FROM %s t', t_num, tbl);
            EXCEPTION WHEN data_exception THEN
                RAISE EXCEPTION 'Column % is not numeric; task=''reg'' needs numbers.', quote_literal(target_column);
            END;
        END IF;
        ptypes := ptypes || 5;
        enc := enc || format('causilo._f8(%s)', t_num);
    END IF;
    names := names || target_column;

    -- Context sampling and splitting, as in predict_frame.
    budget := max_cells / (k + 1);
    keep := least(max_context, greatest(budget / 2, budget - n_query));
    n_context := least(n_filled, keep);
    IF n_filled > keep THEN
        notes := notes || format('context sampled from %s to %s rows', to_char(n_filled, 'FM999,999,999,999'),
                                 to_char(keep, 'FM999,999,999,999'));
    END IF;
    chunk := greatest(least(budget - n_context, n_query), 1);
    calls_planned := (n_query + chunk - 1) / chunk;
    FOR i IN 1..coalesce(array_length(notes, 1), 0) LOOP
        RAISE NOTICE 'causilo: %', notes[i];
    END LOOP;

    -- Second read: every column of the rows that are sent, as Parquet column
    -- pages. Part -1 is the context; parts 0, 1, ... are the calls.
    sql := format($q$
        WITH s AS MATERIALIZED (
            SELECT row_number() OVER () AS rn, (%1$s IS NULL) AS b, %2$s AS id,
                   ARRAY[%3$s]::bytea[] AS e
            FROM %4$s t
        ),
        r AS (
            SELECT s.b, s.id, s.e, row_number() OVER (PARTITION BY s.b ORDER BY s.rn) - 1 AS o
            FROM s
            WHERE s.b %5$s
        ),
        q AS (
            SELECT b, id, e,
                   CASE WHEN b THEN o / %6$s ELSE -1 END AS part,
                   CASE WHEN b THEN o %% %6$s ELSE o END AS po
            FROM r
        ),
        v AS (
            SELECT q.part, q.po, x.j, x.val
            FROM q, unnest(q.e) WITH ORDINALITY AS x(val, j)
            WHERE NOT q.b OR x.j <= %7$s
        ),
        g AS (
            SELECT part, j, po / 8 AS grp, count(*) AS n, count(val) AS nn,
                   sum(CASE WHEN val IS NULL THEN 0 ELSE 1 << (po %% 8)::int END) AS d,
                   sum(CASE WHEN val = '\x01'::bytea THEN 1 << (po %% 8)::int ELSE 0 END) AS bits,
                   string_agg(val, ''::bytea ORDER BY po) AS vals
            FROM v GROUP BY part, j, po / 8
        )
        SELECT part, j::int AS j, sum(n)::bigint AS n, sum(nn)::bigint AS nn,
               string_agg(causilo._b(d::int), ''::bytea ORDER BY grp) AS defs,
               string_agg(causilo._b(bits::int), ''::bytea ORDER BY grp) AS bits,
               string_agg(vals, ''::bytea ORDER BY grp) AS vals,
               NULL::text[] AS ids
        FROM g GROUP BY part, j
        UNION ALL
        SELECT part, 0, count(*), 0, NULL, NULL, NULL, array_agg(id ORDER BY po)
        FROM q WHERE b GROUP BY part
        ORDER BY 1, 2
    $q$,
        t_val,
        CASE WHEN id_column IS NULL THEN 't.ctid::text' ELSE format('t.%I::text', id_column) END,
        array_to_string(enc, ', '),
        tbl,
        CASE WHEN n_filled > keep
             THEN format('OR s.rn IN (SELECT rn FROM s WHERE NOT b ORDER BY md5(rn::text), rn LIMIT %s)', keep)
             ELSE 'OR true' END,
        chunk,
        k);

    -- A call that fails after earlier ones completed, or whose answer cannot
    -- be used, loses rows that were charged; the error says how many.
    BEGIN
        FOR rec IN EXECUTE sql LOOP
            IF cur IS DISTINCT FROM rec.part THEN
                IF cur IS NOT NULL THEN
                    RAISE EXCEPTION 'causilo: a part of the request is incomplete; this is a bug';
                END IF;
                cur := rec.part;
                defs := '{}';
                vals := '{}';
                part_n := NULL;
            END IF;
            IF rec.j = 0 THEN
                ids := rec.ids;
                CONTINUE;
            END IF;
            IF part_n IS NULL THEN
                part_n := rec.n;
            END IF;
            IF rec.n <> part_n OR (ptypes[rec.j] = 0 AND rec.nn <> rec.n) THEN
                RAISE EXCEPTION 'The table changed while it was being read; run the prediction again.';
            END IF;
            defs := array_append(defs, rec.defs);
            vals := array_append(vals, CASE WHEN ptypes[rec.j] = 0 THEN rec.bits ELSE rec.vals END);

            IF cur = -1 AND rec.j = k + 1 THEN
                IF part_n <> n_context THEN
                    RAISE EXCEPTION 'The table changed while it was being read; run the prediction again.';
                END IF;
                context_file := causilo._parquet(names, ptypes, part_n, defs, vals);
                cur := NULL;
            ELSIF cur >= 0 AND rec.j = k THEN
                query_file := causilo._parquet(names[1:k], ptypes[1:k], part_n, defs, vals);
                boundary := md5(random()::text || clock_timestamp()::text);
                params := CASE WHEN task = 'clf'
                    THEN jsonb_build_object('model', 'causilo-clf', 'target', target_column)::text
                    ELSE jsonb_build_object('model', 'causilo-reg', 'target', target_column,
                                            'output_type', 'quantiles', 'quantiles', jsonb_build_array(0.25, 0.5, 0.75))::text
                    END;
                body := convert_to('--' || boundary || E'\r\nContent-Disposition: form-data; name="context"; '
                                   'filename="context.parquet"' || E'\r\nContent-Type: application/vnd.apache.parquet\r\n\r\n', 'UTF8')
                     || context_file
                     || convert_to(E'\r\n--' || boundary || E'\r\nContent-Disposition: form-data; name="query"; '
                                   'filename="query.parquet"' || E'\r\nContent-Type: application/vnd.apache.parquet\r\n\r\n', 'UTF8')
                     || query_file
                     || convert_to(E'\r\n--' || boundary || E'\r\nContent-Disposition: form-data; name="params"'
                                   || E'\r\nContent-Type: application/json\r\n\r\n' || params
                                   || E'\r\n--' || boundary || E'--\r\n', 'UTF8');
                IF array_length(ids, 1) IS DISTINCT FROM part_n::int THEN
                    RAISE EXCEPTION 'The table changed while it was being read; run the prediction again.';
                END IF;
                answer := causilo._call(endpoint, api_key, body, 'multipart/form-data; boundary=' || boundary);
                -- Charged from here on, whatever the checks below find.
                calls_done := calls_done + 1;
                answered := true;
                IF jsonb_typeof(answer->'prediction') IS DISTINCT FROM 'array'
                   OR jsonb_array_length(answer->'prediction') <> part_n THEN
                    RAISE EXCEPTION 'The Causilo API answered % rows for % sent.',
                        coalesce(jsonb_array_length(answer->'prediction')::text, 'no'), part_n;
                END IF;
                IF task = 'clf' THEN
                    got := ARRAY(SELECT jsonb_array_elements_text(answer->'metadata'->'classes'));
                    IF classes IS NOT NULL AND got IS DISTINCT FROM classes THEN
                        RAISE EXCEPTION 'The class order changed between calls; run again with fewer rows.';
                    END IF;
                    classes := got;
                    RETURN QUERY
                        SELECT z.id, classes[best.k::int], best.p, NULL::float8, NULL::float8, NULL::float8
                        FROM ROWS FROM (jsonb_array_elements(answer->'prediction'), unnest(ids)) WITH ORDINALITY AS z(p, id, i)
                        CROSS JOIN LATERAL (
                            SELECT e.k, e.v::float8 AS p
                            FROM jsonb_array_elements_text(z.p) WITH ORDINALITY AS e(v, k)
                            ORDER BY e.v::float8 DESC NULLS LAST, e.k
                            LIMIT 1) best
                        ORDER BY z.i;
                ELSE
                    RETURN QUERY
                        SELECT z.id, causilo._pyfloat((z.p->>1)::float8), NULL::float8,
                               (z.p->>0)::float8, (z.p->>1)::float8, (z.p->>2)::float8
                        FROM ROWS FROM (jsonb_array_elements(answer->'prediction'), unnest(ids)) WITH ORDINALITY AS z(p, id, i)
                        ORDER BY z.i;
                END IF;
                sent := sent + part_n;
                answered := false;
                cur := NULL;
            END IF;
        END LOOP;
    EXCEPTION WHEN OTHERS OR query_canceled THEN
        IF calls_done = 0 THEN
            RAISE;
        END IF;
        GET STACKED DIAGNOSTICS err_state = RETURNED_SQLSTATE, err_msg = MESSAGE_TEXT;
        IF answered THEN
            RAISE EXCEPTION USING ERRCODE = err_state, MESSAGE = format(
                '%s (call %s of %s was answered but its answer could not be used; the %s call%s up to and '
                'including it %s charged, %s of %s rows; no predictions are returned)', err_msg, calls_done,
                calls_planned, calls_done, CASE WHEN calls_done = 1 THEN '' ELSE 's' END,
                CASE WHEN calls_done = 1 THEN 'was' ELSE 'were' END, sent + part_n, n_query);
        END IF;
        RAISE EXCEPTION USING ERRCODE = err_state, MESSAGE = format(
            '%s (call %s of %s; the %s call%s before it completed and %s charged, %s of %s rows; '
            'those predictions are not returned)', err_msg, calls_done + 1, calls_planned, calls_done,
            CASE WHEN calls_done = 1 THEN '' ELSE 's' END, CASE WHEN calls_done = 1 THEN 'was' ELSE 'were' END,
            sent, n_query);
    END;
    -- Every call completed, but the table gave a different number of blank
    -- cells than it was counted with.
    IF sent <> n_query AND calls_done = 0 THEN
        RAISE EXCEPTION 'The table changed while it was being read; run the prediction again.';
    ELSIF sent <> n_query THEN
        RAISE EXCEPTION 'The table changed while it was being read; run the prediction again (the % call% made '
                        '% charged, % of % rows; those predictions are not returned).', calls_done,
                        CASE WHEN calls_done = 1 THEN '' ELSE 's' END,
                        CASE WHEN calls_done = 1 THEN 'was' ELSE 'were' END, sent, n_query;
    END IF;
END
$fn$;

COMMENT ON FUNCTION causilo.predict(regclass, text, text, text) IS
  'Predict the blank cells of a column with Causilo, over the http extension. SET causilo.api_key first.';
