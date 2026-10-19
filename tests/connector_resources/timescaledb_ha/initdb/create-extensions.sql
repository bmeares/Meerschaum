-- Meerschaum stack: database setup.
--
-- `mrsm stack` writes this file once, when it is absent (your edits are kept), and mounts it into
-- the `db` container as `/docker-entrypoint-initdb.d/100_meerschaum.sql`. The image's entrypoint
-- runs it with `psql` only when the data volume is empty (the first start of a new stack), after
-- the image's own setup scripts (TimescaleDB, the telemetry opt-out, `timescaledb-tune`, the
-- TimescaleDB Toolkit). It is safe to run more than once and with any PostgreSQL image: an
-- extension the image does not ship is skipped with a NOTICE.
--
-- To change an existing database, see "Upgrading the stack" in the Meerschaum documentation
-- (`mrsm upgrade extensions`).

-- Create each extension the server offers. TimescaleDB must be preloaded to be created.
DO $mrsm$
DECLARE
    ext TEXT;
BEGIN
    FOREACH ext IN ARRAY ARRAY['timescaledb', 'postgis', 'timescaledb_toolkit', 'pg_stat_statements'] LOOP
        IF NOT EXISTS (SELECT 1 FROM pg_available_extensions WHERE name = ext) THEN
            RAISE NOTICE 'Meerschaum: skipping the extension "%": this image does not provide it.', ext;
        ELSIF ext = 'timescaledb' AND NOT (
            'timescaledb' = ANY (
                SELECT btrim(lib, ' "')
                FROM unnest(string_to_array(current_setting('shared_preload_libraries'), ',')) AS l(lib)
            )
        ) THEN
            RAISE NOTICE 'Meerschaum: skipping the extension "timescaledb": it is not in shared_preload_libraries.';
        ELSE
            EXECUTE format('CREATE EXTENSION IF NOT EXISTS %I CASCADE', ext);
        END IF;
    END LOOP;
END
$mrsm$;

-- `pg_stat_statements` can only be queried when the server preloads it: append it to
-- `shared_preload_libraries` (keeping the current entries) with `ALTER SYSTEM`, which writes
-- `postgresql.auto.conf` and takes effect at the server's next start (the first real start, after
-- this setup). `ALTER SYSTEM` cannot run inside a `DO` block, so psql runs the generated
-- statement (`\gexec`); nothing is generated when it is unavailable or already listed.
WITH current_libs AS (
    SELECT btrim(lib, ' "') AS lib, ord
    FROM unnest(string_to_array(current_setting('shared_preload_libraries'), ','))
        WITH ORDINALITY AS l(lib, ord)
), wanted AS (
    SELECT lib, ord FROM current_libs WHERE lib <> ''
    UNION ALL
    SELECT 'pg_stat_statements', 2147483647
)
SELECT 'ALTER SYSTEM SET shared_preload_libraries = '
    || string_agg(quote_literal(lib), ', ' ORDER BY ord)
FROM wanted
WHERE EXISTS (SELECT 1 FROM pg_available_extensions WHERE name = 'pg_stat_statements')
    AND NOT EXISTS (SELECT 1 FROM current_libs WHERE lib = 'pg_stat_statements')
HAVING count(*) > 0
\gexec
