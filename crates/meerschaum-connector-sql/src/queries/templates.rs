//! SQL templates copied verbatim from `meerschaum/utils/sql.py` by a generator script
//! (see `tests/fixtures/gen_queries.py`, which re-checks them). Do not edit by hand.
#![allow(missing_docs)]

/// `UPDATE_QUERIES` (`utils/sql.py:74`): key → list of templates.
pub const UPDATE_QUERIES: &[(&str, &[&str])] = &[
    (
        "default",
        &[
            "\n        UPDATE {target_table_name} AS f\n        {sets_subquery_none}\n        FROM {target_table_name} AS t\n        INNER JOIN (SELECT {patch_cols_str} FROM {patch_table_name}) AS p\n            ON\n                {and_subquery_t}\n        WHERE\n            {and_subquery_f}\n            AND\n            {date_bounds_subquery}\n    ",
        ],
    ),
    (
        "timescaledb-upsert",
        &[
            "\n        INSERT INTO {target_table_name} ({patch_cols_str})\n        SELECT {patch_cols_str}\n        FROM {patch_table_name}\n        ON CONFLICT ({join_cols_str}) DO {update_or_nothing} {sets_subquery_none_excluded}\n    ",
        ],
    ),
    (
        "timescaledb-ha-upsert",
        &[
            "\n        INSERT INTO {target_table_name} ({patch_cols_str})\n        SELECT {patch_cols_str}\n        FROM {patch_table_name}\n        ON CONFLICT ({join_cols_str}) DO {update_or_nothing} {sets_subquery_none_excluded}\n    ",
        ],
    ),
    (
        "postgresql-upsert",
        &[
            "\n        INSERT INTO {target_table_name} ({patch_cols_str})\n        SELECT {patch_cols_str}\n        FROM {patch_table_name}\n        ON CONFLICT ({join_cols_str}) DO {update_or_nothing} {sets_subquery_none_excluded}\n    ",
        ],
    ),
    (
        "postgis-upsert",
        &[
            "\n        INSERT INTO {target_table_name} ({patch_cols_str})\n        SELECT {patch_cols_str}\n        FROM {patch_table_name}\n        ON CONFLICT ({join_cols_str}) DO {update_or_nothing} {sets_subquery_none_excluded}\n    ",
        ],
    ),
    (
        "citus-upsert",
        &[
            "\n        INSERT INTO {target_table_name} ({patch_cols_str})\n        SELECT {patch_cols_str}\n        FROM {patch_table_name}\n        ON CONFLICT ({join_cols_str}) DO {update_or_nothing} {sets_subquery_none_excluded}\n    ",
        ],
    ),
    (
        "cockroachdb-upsert",
        &[
            "\n        INSERT INTO {target_table_name} ({patch_cols_str})\n        SELECT {patch_cols_str}\n        FROM {patch_table_name}\n        ON CONFLICT ({join_cols_str}) DO {update_or_nothing} {sets_subquery_none_excluded}\n    ",
        ],
    ),
    (
        "mysql",
        &[
            "\n        UPDATE {target_table_name} AS f\n        JOIN (SELECT {patch_cols_str} FROM {patch_table_name}) AS p\n        ON\n            {and_subquery_f}\n        {sets_subquery_f}\n        WHERE\n            {date_bounds_subquery}\n    ",
        ],
    ),
    (
        "mysql-upsert",
        &[
            "\n        INSERT {ignore}INTO {target_table_name} ({patch_cols_str})\n        SELECT {patch_cols_str}\n        FROM {patch_table_name}\n        {on_duplicate_key_update}\n            {cols_equal_values}\n    ",
        ],
    ),
    (
        "mariadb",
        &[
            "\n        UPDATE {target_table_name} AS f\n        JOIN (SELECT {patch_cols_str} FROM {patch_table_name}) AS p\n        ON\n            {and_subquery_f}\n        {sets_subquery_f}\n        WHERE\n            {date_bounds_subquery}\n    ",
        ],
    ),
    (
        "mariadb-upsert",
        &[
            "\n        INSERT {ignore}INTO {target_table_name} ({patch_cols_str})\n        SELECT {patch_cols_str}\n        FROM {patch_table_name}\n        {on_duplicate_key_update}\n            {cols_equal_values}\n    ",
        ],
    ),
    (
        "mssql",
        &[
            "\n        {with_temp_date_bounds}\n        MERGE {target_table_name} f\n            USING (SELECT {patch_cols_str} FROM {patch_table_name}) p\n            ON\n                {and_subquery_f}\n            AND\n                {date_bounds_subquery}\n        WHEN MATCHED THEN\n            UPDATE\n            {sets_subquery_none};\n    ",
        ],
    ),
    (
        "mssql-upsert",
        &[
            "{identity_insert_on}",
            "\n        {with_temp_date_bounds}\n        MERGE {target_table_name} f\n            USING (SELECT {patch_cols_str} FROM {patch_table_name}) p\n            ON\n                {and_subquery_f}\n            AND\n                {date_bounds_subquery}{when_matched_update_sets_subquery_none}\n        WHEN NOT MATCHED THEN\n            INSERT ({patch_cols_str})\n            VALUES ({patch_cols_prefixed_str});\n        ",
            "{identity_insert_off}",
        ],
    ),
    (
        "oracle",
        &[
            "\n        MERGE INTO {target_table_name} f\n            USING (SELECT {patch_cols_str} FROM {patch_table_name}) p\n            ON (\n                {and_subquery_f}\n                AND\n                {date_bounds_subquery}\n            )\n            WHEN MATCHED THEN\n                UPDATE\n                {sets_subquery_none}\n    ",
        ],
    ),
    (
        "oracle-upsert",
        &[
            "\n        MERGE INTO {target_table_name} f\n            USING (SELECT {patch_cols_str} FROM {patch_table_name}) p\n            ON (\n                {and_subquery_f}\n                AND\n                {date_bounds_subquery}\n            ){when_matched_update_sets_subquery_none}\n            WHEN NOT MATCHED THEN\n                INSERT ({patch_cols_str})\n                VALUES ({patch_cols_prefixed_str})\n    ",
        ],
    ),
    (
        "sqlite-upsert",
        &[
            "\n        INSERT INTO {target_table_name} ({patch_cols_str})\n        SELECT {patch_cols_str}\n        FROM {patch_table_name}\n        WHERE true\n        ON CONFLICT ({join_cols_str}) DO {update_or_nothing} {sets_subquery_none_excluded}\n    ",
        ],
    ),
    (
        "sqlite_delete_insert",
        &[
            "\n        DELETE FROM {target_table_name} AS f\n        WHERE ROWID IN (\n            SELECT t.ROWID\n            FROM {target_table_name} AS t\n            INNER JOIN (SELECT * FROM {patch_table_name}) AS p\n                ON {and_subquery_t}\n        );\n        ",
            "\n        INSERT INTO {target_table_name} AS f\n        SELECT {patch_cols_str} FROM {patch_table_name} AS p\n        ",
        ],
    ),
    (
        "geopackage-upsert",
        &[
            "\n        INSERT INTO {target_table_name} ({patch_cols_str})\n        SELECT {patch_cols_str}\n        FROM {patch_table_name}\n        WHERE true\n        ON CONFLICT ({join_cols_str}) DO {update_or_nothing} {sets_subquery_none_excluded}\n    ",
        ],
    ),
];

/// `columns_types_queries` (`utils/sql.py`).
pub const COLUMNS_TYPES_QUERIES: &[(&str, &str)] = &[
    (
        "default",
        "\n        SELECT\n            table_catalog AS database,\n            table_schema AS schema,\n            table_name AS table,\n            column_name AS column,\n            data_type AS type,\n            numeric_precision,\n            numeric_scale\n        FROM information_schema.columns\n        WHERE table_name IN ('{table}', '{table_trunc}')\n    ",
    ),
    (
        "sqlite",
        "\n        SELECT\n            '' \"database\",\n            '' \"schema\",\n            m.name \"table\",\n            p.name \"column\",\n            p.type \"type\"\n        FROM sqlite_master m\n        LEFT OUTER JOIN pragma_table_info(m.name) p\n            ON m.name <> p.name\n        WHERE m.type = 'table'\n            AND m.name IN ('{table}', '{table_trunc}')\n    ",
    ),
    (
        "geopackage",
        "\n        SELECT\n            '' \"database\",\n            '' \"schema\",\n            m.name \"table\",\n            p.name \"column\",\n            p.type \"type\"\n        FROM sqlite_master m\n        LEFT OUTER JOIN pragma_table_info(m.name) p\n            ON m.name <> p.name\n        WHERE m.type = 'table'\n            AND m.name IN ('{table}', '{table_trunc}')\n    ",
    ),
    (
        "mssql",
        "\n        SELECT\n            TABLE_CATALOG AS [database],\n            TABLE_SCHEMA AS [schema],\n            TABLE_NAME AS [table],\n            COLUMN_NAME AS [column],\n            CASE WHEN CHARACTER_MAXIMUM_LENGTH = -1 THEN DATA_TYPE + '(max)' ELSE DATA_TYPE END AS [type],\n            NUMERIC_PRECISION AS [numeric_precision],\n            NUMERIC_SCALE AS [numeric_scale]\n        FROM {db_prefix}INFORMATION_SCHEMA.COLUMNS WITH (NOLOCK)\n        WHERE TABLE_NAME IN (\n            '{table}',\n            '{table_trunc}'\n        )\n\n    ",
    ),
    (
        "mysql",
        "\n        SELECT\n            TABLE_SCHEMA `database`,\n            TABLE_SCHEMA `schema`,\n            TABLE_NAME `table`,\n            COLUMN_NAME `column`,\n            DATA_TYPE `type`,\n            NUMERIC_PRECISION `numeric_precision`,\n            NUMERIC_SCALE `numeric_scale`\n        FROM INFORMATION_SCHEMA.COLUMNS\n        WHERE TABLE_NAME IN ('{table}', '{table_trunc}')\n    ",
    ),
    (
        "mariadb",
        "\n        SELECT\n            TABLE_SCHEMA `database`,\n            TABLE_SCHEMA `schema`,\n            TABLE_NAME `table`,\n            COLUMN_NAME `column`,\n            DATA_TYPE `type`,\n            NUMERIC_PRECISION `numeric_precision`,\n            NUMERIC_SCALE `numeric_scale`\n        FROM INFORMATION_SCHEMA.COLUMNS\n        WHERE TABLE_NAME IN ('{table}', '{table_trunc}')\n    ",
    ),
    (
        "oracle",
        "\n        SELECT\n            NULL AS \"database\",\n            NULL AS \"schema\",\n            TABLE_NAME AS \"table\",\n            COLUMN_NAME AS \"column\",\n            DATA_TYPE AS \"type\",\n            DATA_PRECISION AS \"numeric_precision\",\n            DATA_SCALE AS \"numeric_scale\"\n        FROM all_tab_columns\n        WHERE TABLE_NAME IN (\n            '{table}',\n            '{table_trunc}',\n            '{table_lower}',\n            '{table_lower_trunc}',\n            '{table_upper}',\n            '{table_upper_trunc}'\n        )\n    ",
    ),
];

/// `columns_indices_queries` (`utils/sql.py`).
pub const COLUMNS_INDICES_QUERIES: &[(&str, &str)] = &[
    (
        "default",
        "\n        SELECT\n            current_database() AS \"database\",\n            n.nspname AS \"schema\",\n            t.relname AS \"table\",\n            c.column_name AS \"column\",\n            i.relname AS \"index\",\n            CASE WHEN con.contype = 'p' THEN 'PRIMARY KEY' ELSE 'INDEX' END AS \"index_type\"\n        FROM pg_class t\n        INNER JOIN pg_index AS ix\n            ON t.oid = ix.indrelid\n        INNER JOIN pg_class AS i\n            ON i.oid = ix.indexrelid\n        INNER JOIN pg_namespace AS n\n            ON n.oid = t.relnamespace\n        INNER JOIN pg_attribute AS a\n            ON a.attnum = ANY(ix.indkey)\n            AND a.attrelid = t.oid\n        INNER JOIN information_schema.columns AS c\n            ON c.column_name = a.attname\n            AND c.table_name = t.relname\n            AND c.table_schema = n.nspname\n        LEFT JOIN pg_constraint AS con\n            ON con.conindid = i.oid\n            AND con.contype = 'p'\n        WHERE\n            t.relname IN ('{table}', '{table_trunc}')\n            AND n.nspname = '{schema}'\n    ",
    ),
    (
        "sqlite",
        "\n        WITH indexed_columns AS (\n            SELECT\n                '{table}' AS table_name,\n                pi.name AS column_name,\n                i.name AS index_name,\n                'INDEX' AS index_type\n            FROM\n                sqlite_master AS i,\n                pragma_index_info(i.name) AS pi\n            WHERE\n                i.type = 'index'\n                AND i.tbl_name = '{table}'\n        ),\n        primary_key_columns AS (\n            SELECT\n                '{table}' AS table_name,\n                ti.name AS column_name,\n                'PRIMARY_KEY' AS index_name,\n                'PRIMARY KEY' AS index_type\n            FROM\n                pragma_table_info('{table}') AS ti\n            WHERE\n                ti.pk > 0\n        )\n        SELECT\n            NULL AS \"database\",\n            NULL AS \"schema\",\n            \"table_name\" AS \"table\",\n            \"column_name\" AS \"column\",\n            \"index_name\" AS \"index\",\n            \"index_type\"\n        FROM indexed_columns\n        UNION ALL\n        SELECT\n            NULL AS \"database\",\n            NULL AS \"schema\",\n            table_name AS \"table\",\n            column_name AS \"column\",\n            index_name AS \"index\",\n            index_type\n        FROM primary_key_columns\n    ",
    ),
    (
        "geopackage",
        "\n        WITH indexed_columns AS (\n            SELECT\n                '{table}' AS table_name,\n                pi.name AS column_name,\n                i.name AS index_name,\n                'INDEX' AS index_type\n            FROM\n                sqlite_master AS i,\n                pragma_index_info(i.name) AS pi\n            WHERE\n                i.type = 'index'\n                AND i.tbl_name = '{table}'\n        ),\n        primary_key_columns AS (\n            SELECT\n                '{table}' AS table_name,\n                ti.name AS column_name,\n                'PRIMARY_KEY' AS index_name,\n                'PRIMARY KEY' AS index_type\n            FROM\n                pragma_table_info('{table}') AS ti\n            WHERE\n                ti.pk > 0\n        )\n        SELECT\n            NULL AS \"database\",\n            NULL AS \"schema\",\n            \"table_name\" AS \"table\",\n            \"column_name\" AS \"column\",\n            \"index_name\" AS \"index\",\n            \"index_type\"\n        FROM indexed_columns\n        UNION ALL\n        SELECT\n            NULL AS \"database\",\n            NULL AS \"schema\",\n            table_name AS \"table\",\n            column_name AS \"column\",\n            index_name AS \"index\",\n            index_type\n        FROM primary_key_columns\n    ",
    ),
    (
        "mssql",
        "\n        SELECT\n            NULL AS [database],\n            s.name AS [schema],\n            t.name AS [table],\n            c.name AS [column],\n            i.name AS [index],\n            CASE\n                WHEN kc.type = 'PK' THEN 'PRIMARY KEY'\n                ELSE 'INDEX'\n            END AS [index_type],\n            CASE\n                WHEN i.type = 1 THEN CAST(1 AS BIT)\n                ELSE CAST(0 AS BIT)\n            END AS [clustered]\n        FROM\n            sys.schemas s WITH (NOLOCK)\n        INNER JOIN sys.tables t WITH (NOLOCK)\n            ON s.schema_id = t.schema_id\n        INNER JOIN sys.indexes i WITH (NOLOCK)\n            ON t.object_id = i.object_id\n        INNER JOIN sys.index_columns ic WITH (NOLOCK)\n            ON i.object_id = ic.object_id\n            AND i.index_id = ic.index_id\n        INNER JOIN sys.columns c WITH (NOLOCK)\n            ON ic.object_id = c.object_id\n            AND ic.column_id = c.column_id\n        LEFT JOIN sys.key_constraints kc WITH (NOLOCK)\n            ON kc.parent_object_id = i.object_id\n            AND kc.type = 'PK'\n            AND kc.name = i.name\n        WHERE\n            t.name IN ('{table}', '{table_trunc}')\n            AND s.name = '{schema}'\n            AND i.type IN (1, 2)\n    ",
    ),
    (
        "oracle",
        "\n        SELECT\n            NULL AS \"database\",\n            ic.table_owner AS \"schema\",\n            ic.table_name AS \"table\",\n            ic.column_name AS \"column\",\n            i.index_name AS \"index\",\n            CASE\n                WHEN c.constraint_type = 'P' THEN 'PRIMARY KEY'\n                WHEN i.uniqueness = 'UNIQUE' THEN 'UNIQUE INDEX'\n                ELSE 'INDEX'\n            END AS index_type\n        FROM\n            all_ind_columns ic\n        INNER JOIN all_indexes i\n            ON ic.index_name = i.index_name\n            AND ic.table_owner = i.owner\n        LEFT JOIN all_constraints c\n            ON i.index_name = c.constraint_name\n            AND i.table_owner = c.owner\n            AND c.constraint_type = 'P'\n        WHERE ic.table_name IN (\n            '{table}',\n            '{table_trunc}',\n            '{table_upper}',\n            '{table_upper_trunc}'\n        )\n    ",
    ),
    (
        "mysql",
        "\n        SELECT\n            TABLE_SCHEMA AS `database`,\n            TABLE_SCHEMA AS `schema`,\n            TABLE_NAME AS `table`,\n            COLUMN_NAME AS `column`,\n            INDEX_NAME AS `index`,\n            CASE\n                WHEN NON_UNIQUE = 0 THEN 'PRIMARY KEY'\n                ELSE 'INDEX'\n            END AS `index_type`\n        FROM\n            information_schema.STATISTICS\n        WHERE\n            TABLE_NAME IN ('{table}', '{table_trunc}')\n    ",
    ),
    (
        "mariadb",
        "\n        SELECT\n            TABLE_SCHEMA AS `database`,\n            TABLE_SCHEMA AS `schema`,\n            TABLE_NAME AS `table`,\n            COLUMN_NAME AS `column`,\n            INDEX_NAME AS `index`,\n            CASE\n                WHEN NON_UNIQUE = 0 THEN 'PRIMARY KEY'\n                ELSE 'INDEX'\n            END AS `index_type`\n        FROM\n            information_schema.STATISTICS\n        WHERE\n            TABLE_NAME IN ('{table}', '{table_trunc}')\n    ",
    ),
];

/// `reset_autoincrement_queries` (`utils/sql.py:554`).
pub const RESET_AUTOINCREMENT_QUERIES: &[(&str, &str)] = &[
    (
        "default",
        "\n        SELECT SETVAL(pg_get_serial_sequence('{table_name}', '{column}'), {val})\n        FROM {table_name}\n    ",
    ),
    (
        "mssql",
        "\n        DBCC CHECKIDENT ('{table_name}', RESEED, {val})\n    ",
    ),
    (
        "mysql",
        "\n        ALTER TABLE {table_name} AUTO_INCREMENT = {val}\n    ",
    ),
    (
        "mariadb",
        "\n        ALTER TABLE {table_name} AUTO_INCREMENT = {val}\n    ",
    ),
    (
        "sqlite",
        "\n        UPDATE sqlite_sequence\n        SET seq = {val}\n        WHERE name = '{table}'\n    ",
    ),
    (
        "geopackage",
        "\n        UPDATE sqlite_sequence\n        SET seq = {val}\n        WHERE name = '{table}'\n    ",
    ),
    (
        "oracle",
        "ALTER TABLE {table_name} MODIFY {column_name} GENERATED BY DEFAULT ON NULL AS IDENTITY (START WITH {val_plus_1})",
    ),
];

/// Look up a template table entry, falling back to `'default'`.
pub fn lookup<'a>(table: &'a [(&'a str, &'a str)], key: &str) -> &'a str {
    table
        .iter()
        .find(|(k, _)| *k == key)
        .or_else(|| table.iter().find(|(k, _)| *k == "default"))
        .map(|(_, v)| *v)
        .expect("default template")
}

/// Look up `UPDATE_QUERIES[key]`, falling back to `'default'`.
pub fn update_queries(key: &str) -> &'static [&'static str] {
    UPDATE_QUERIES
        .iter()
        .find(|(k, _)| *k == key)
        .or_else(|| UPDATE_QUERIES.iter().find(|(k, _)| *k == "default"))
        .map(|(_, v)| *v)
        .expect("default update template")
}

/// Whether `key in UPDATE_QUERIES`.
pub fn has_update_query(key: &str) -> bool {
    UPDATE_QUERIES.iter().any(|(k, _)| *k == key)
}
