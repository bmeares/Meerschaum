use super::*;

fn col(name: &str, db_type: &str) -> ColumnInfo {
    ColumnInfo {
        name: name.into(),
        db_type: db_type.into(),
        nullable: true,
        ..Default::default()
    }
}

fn declared(flavor: Flavor, items: &[(&str, &str)]) -> IndexMap<String, (String, String)> {
    items
        .iter()
        .map(|(c, d)| {
            (
                c.to_string(),
                (
                    d.to_string(),
                    crate::dtypes::get_db_type_from_pd_type(d, Some(flavor)),
                ),
            )
        })
        .collect()
}

fn plan(
    flavor: Flavor,
    columns: &[ColumnInfo],
    items: &[(&str, &str)],
    facts: TableFacts,
) -> SchemaPlan {
    let declared = declared(flavor, items);
    let table = sql_item_name("t", flavor, None);
    build_schema_plan(&PlanInput {
        flavor,
        table: &table,
        target: "t",
        columns,
        declared: &declared,
        facts: &facts,
        notes: Vec::new(),
    })
}

#[test]
fn classifies_types() {
    let pg = Flavor::Postgresql;
    assert_eq!(classify(pg, "INTEGER"), TypeClass::Int(32));
    assert_eq!(classify(pg, "BIGINT"), TypeClass::Int(64));
    assert_eq!(classify(pg, "DOUBLE PRECISION"), TypeClass::Float(64));
    assert_eq!(classify(pg, "REAL"), TypeClass::Float(32));
    assert_eq!(classify(pg, "VARCHAR(255)"), TypeClass::Text(Some(255)));
    assert_eq!(classify(pg, "TEXT"), TypeClass::Text(None));
    assert_eq!(
        classify(pg, "NUMERIC(10,2)"),
        TypeClass::Numeric(Some((10, 2)))
    );
    assert_eq!(
        classify(pg, "TIMESTAMP WITH TIME ZONE"),
        TypeClass::Datetime { tz: true }
    );
    assert_eq!(
        classify(pg, "TIMESTAMPTZ(6)"),
        TypeClass::Datetime { tz: true }
    );
    assert_eq!(
        classify(pg, "TIMESTAMP(6) WITHOUT TIME ZONE"),
        TypeClass::Datetime { tz: false }
    );
    assert_eq!(
        classify(Flavor::Postgis, "GEOMETRY(POINT, 4326)"),
        TypeClass::Geometry {
            geography: false,
            spec: Some("POINT, 4326".into())
        }
    );
    assert_eq!(classify(Flavor::Mysql, "TINYINT(1)"), TypeClass::Bool);
    assert_eq!(classify(Flavor::Mysql, "INT(11)"), TypeClass::Int(32));
    assert_eq!(classify(Flavor::Mysql, "FLOAT"), TypeClass::Float(32));
    assert_eq!(classify(Flavor::Mssql, "FLOAT"), TypeClass::Float(64));
    assert_eq!(
        classify(Flavor::Mssql, "NVARCHAR(MAX)"),
        TypeClass::Text(None)
    );
    assert_eq!(classify(Flavor::Oracle, "INTEGER"), TypeClass::Int(128));
    assert_eq!(classify(Flavor::Oracle, "NUMBER(10,0)"), TypeClass::Int(64));
    assert_eq!(classify(Flavor::Oracle, "NUMBER"), TypeClass::Numeric(None));
    assert_eq!(
        classify(Flavor::Oracle, "DATE"),
        TypeClass::Datetime { tz: false }
    );
    assert_eq!(classify(Flavor::Sqlite, "BIGINT"), TypeClass::Int(64));
    assert_eq!(classify(Flavor::Sqlite, ""), TypeClass::Untyped);
    assert_eq!(
        classify(Flavor::Sqlite, "TIMESTAMP"),
        TypeClass::Datetime { tz: false }
    );
    assert_eq!(classify(Flavor::Duckdb, "FLOAT"), TypeClass::Float(32));
}

#[test]
fn matching_tables_need_nothing() {
    let cols = [
        col("ts", "TIMESTAMP WITH TIME ZONE"),
        col("id", "BIGINT"),
        col("v", "DOUBLE PRECISION"),
        col("j", "JSONB"),
        col("n", "NUMERIC"),
        col("s", "TEXT"),
    ];
    let p = plan(
        Flavor::Postgresql,
        &cols,
        &[
            ("ts", "datetime64[ns, UTC]"),
            ("id", "int"),
            ("v", "float"),
            ("j", "json"),
            ("n", "numeric"),
            ("s", "string"),
        ],
        TableFacts::default(),
    );
    assert!(p.is_empty(), "{p:?}");
    assert!(p.queries.is_empty());
}

#[test]
fn postgres_plan_adds_widens_and_changes_in_one_statement() {
    let cols = [
        col("id", "INTEGER"),
        col("v", "INTEGER"),
        col("s", "VARCHAR(20)"),
        col("j", "TEXT"),
        col("ts", "TIMESTAMP WITHOUT TIME ZONE"),
        col("code", "TEXT"),
    ];
    let p = plan(
        Flavor::Postgresql,
        &cols,
        &[
            ("id", "int"),
            ("v", "float"),
            ("s", "string"),
            ("j", "json"),
            ("ts", "datetime64[ns, UTC]"),
            ("code", "int"),
            ("new_col", "numeric"),
        ],
        TableFacts::default(),
    );
    let kinds: Vec<(&str, SchemaChangeKind)> = p
        .changes
        .iter()
        .map(|c| (c.column.as_str(), c.kind))
        .collect();
    assert_eq!(
        kinds,
        vec![
            ("id", SchemaChangeKind::Widen),
            ("v", SchemaChangeKind::Widen),
            ("s", SchemaChangeKind::Widen),
            ("j", SchemaChangeKind::Widen),
            ("ts", SchemaChangeKind::Change),
            ("code", SchemaChangeKind::Change),
            ("new_col", SchemaChangeKind::AddColumn),
        ]
    );
    assert_eq!(p.queries.len(), 1);
    let q = &p.queries[0];
    assert!(
        q.starts_with("ALTER TABLE \"t\"\n    ADD COLUMN \"new_col\" NUMERIC"),
        "{q}"
    );
    assert!(
        q.contains("ALTER COLUMN \"id\" TYPE BIGINT USING CAST(\"id\" AS BIGINT)"),
        "{q}"
    );
    assert!(
        q.contains("ALTER COLUMN \"ts\" TYPE TIMESTAMPTZ(9) USING \"ts\" AT TIME ZONE 'UTC'"),
        "{q}"
    );
    assert!(
        q.contains("ALTER COLUMN \"j\" TYPE JSONB USING CAST(\"j\" AS JSONB)"),
        "{q}"
    );
    // The safe statement leaves the two changes out.
    assert_eq!(p.safe_queries.len(), 1);
    assert!(!p.safe_queries[0].contains("\"ts\""));
    assert!(!p.safe_queries[0].contains("\"code\""));
    assert!(p.safe_queries[0].contains("\"new_col\""));
    // Never a drop.
    assert!(!q.contains("DROP"));
}

#[test]
fn native_types_are_not_downgraded_to_fallbacks() {
    // MySQL keeps `json` as `TEXT` and `uuid` as `CHAR(36)`, MariaDB has native types.
    let cols = [col("j", "JSON"), col("u", "UUID"), col("b", "TINYINT(1)")];
    let p = plan(
        Flavor::Mariadb,
        &cols,
        &[("j", "json"), ("u", "uuid"), ("b", "bool")],
        TableFacts::default(),
    );
    assert!(p.is_empty(), "{p:?}");
    // Plain PostgreSQL stores geometries as text, but a PostGIS column stays.
    let cols = [col("g", "GEOMETRY(POINT, 4326)"), col("j", "JSON")];
    let p = plan(
        Flavor::Postgresql,
        &cols,
        &[("g", "geometry[Point, 4326]"), ("j", "json")],
        TableFacts::default(),
    );
    assert_eq!(p.changes.len(), 1, "{p:?}");
    assert_eq!(p.changes[0].column, "j");
    assert_eq!(p.changes[0].kind, SchemaChangeKind::Widen);
}

#[test]
fn never_narrows() {
    let cols = [
        col("big", "BIGINT"),
        col("dbl", "DOUBLE PRECISION"),
        col("txt", "TEXT"),
        col("num", "NUMERIC(20,6)"),
    ];
    let p = plan(
        Flavor::Postgresql,
        &cols,
        &[
            ("big", "int32"),
            ("dbl", "float"),
            ("txt", "string"),
            ("num", "numeric[10,2]"),
        ],
        TableFacts::default(),
    );
    assert!(p.is_empty(), "{p:?}");
}

#[test]
fn compressed_hypertables_block_type_changes_but_not_adds() {
    let cols = [col("ts", "TIMESTAMP WITH TIME ZONE"), col("v", "INTEGER")];
    let p = plan(
        Flavor::Timescaledb,
        &cols,
        &[("v", "float"), ("extra", "string")],
        TableFacts {
            compressed_hypertable: true,
            decompress_command: Some("mrsm decompress pipes -c a -m b".into()),
            ..Default::default()
        },
    );
    assert_eq!(p.changes.len(), 2);
    assert!(
        p.blocked
            .as_deref()
            .unwrap()
            .contains("mrsm decompress pipes -c a -m b")
    );
    assert_eq!(p.queries.len(), 1);
    assert!(p.queries[0].contains("ADD COLUMN \"extra\""));
    assert!(!p.queries[0].contains("ALTER COLUMN"));
}

fn partitioned(keys: &[(&str, PartitionKey)]) -> TableFacts {
    TableFacts {
        partitioned_by: keys.iter().map(|(c, k)| (c.to_string(), *k)).collect(),
        ..Default::default()
    }
}

#[test]
fn partition_keys_keep_their_type_and_the_rest_still_applies() {
    // PostgreSQL / PostGIS: a partition key never changes type in place.
    let mut ts = col("ts", "TIMESTAMP WITH TIME ZONE");
    ts.nullable = false;
    let cols = [ts, col("v", "INTEGER")];
    for flavor in [Flavor::Postgresql, Flavor::Postgis] {
        let p = plan(
            flavor,
            &cols,
            &[("ts", "datetime64[ns]"), ("v", "int"), ("extra", "string")],
            partitioned(&[("ts", PartitionKey::Fixed)]),
        );
        assert_eq!(p.changes.len(), 3, "{flavor} {:?}", p.changes);
        let ts = p.changes.iter().find(|c| c.column == "ts").unwrap();
        assert!(!ts.is_applicable());
        let reason = p.blocked.as_deref().unwrap();
        assert!(
            reason.contains(
                "cannot change the type of column 'ts' because the table is partitioned by it"
            ),
            "{reason}"
        );
        assert!(
            reason.contains("drop the pipe and sync it again"),
            "{reason}"
        );
        assert_eq!(p.queries.len(), 1, "{:?}", p.queries);
        assert!(
            p.queries[0].contains("ADD COLUMN \"extra\""),
            "{}",
            p.queries[0]
        );
        assert!(
            p.queries[0].contains("ALTER COLUMN \"v\""),
            "{}",
            p.queries[0]
        );
        assert!(!p.queries[0].contains("\"ts\""), "{}", p.queries[0]);
    }
}

#[test]
fn timescaledb_time_dimensions_change_within_their_kind() {
    let cols = [col("ts", "TIMESTAMP WITHOUT TIME ZONE"), col("n", "BIGINT")];
    let facts = || {
        partitioned(&[
            ("ts", PartitionKey::TimeDimension),
            ("n", PartitionKey::TimeDimension),
        ])
    };
    // Timestamp to timestamp with a time zone: allowed.
    let p = plan(
        Flavor::Timescaledb,
        &cols,
        &[("ts", "datetime64[ns, UTC]")],
        facts(),
    );
    assert!(p.blocked.is_none(), "{:?}", p.blocked);
    assert!(
        p.queries[0].contains("AT TIME ZONE 'UTC'"),
        "{:?}",
        p.queries
    );
    // Timestamp to text, integer to float: refused by TimescaleDB.
    let p = plan(
        Flavor::Timescaledb,
        &cols,
        &[("ts", "string"), ("n", "float")],
        facts(),
    );
    assert!(p.queries.is_empty(), "{:?}", p.queries);
    let reason = p.blocked.as_deref().unwrap();
    assert!(
        reason.contains(
            "cannot change hypertable column 'ts' from TIMESTAMP WITHOUT TIME ZONE to TEXT"
        ),
        "{reason}"
    );
    assert!(reason.contains("hypertable column 'n'"), "{reason}");
    // A space (hash) dimension never changes.
    let p = plan(
        Flavor::Timescaledb,
        &[col("id", "INTEGER")],
        &[("id", "int")],
        partitioned(&[("id", PartitionKey::Fixed)]),
    );
    assert!(p.queries.is_empty(), "{:?}", p.queries);
    assert!(
        p.blocked
            .as_deref()
            .unwrap()
            .starts_with("TimescaleDB cannot change")
    );
}

#[test]
fn mysql_range_columns_stay_dates_or_integers() {
    let cols = [col("ts", "DATETIME"), col("v", "INT")];
    let facts = || partitioned(&[("ts", PartitionKey::RangeColumns)]);
    // `DATETIME` to `DATE`: allowed (it needs confirmation, as any type change).
    let p = plan(Flavor::Mysql, &cols, &[("ts", "date")], facts());
    assert!(p.blocked.is_none(), "{:?}", p.blocked);
    assert!(
        p.queries[0].contains("MODIFY COLUMN `ts` DATE"),
        "{:?}",
        p.queries
    );
    // To an integer (the bounds are dates): refused, and the widening of `v` still applies.
    let p = plan(
        Flavor::Mariadb,
        &cols,
        &[("ts", "int"), ("v", "int")],
        facts(),
    );
    let reason = p.blocked.as_deref().unwrap();
    assert!(
        reason.starts_with(
            "MariaDB cannot change column 'ts' to BIGINT because the table is partitioned by it"
        ),
        "{reason}"
    );
    assert_eq!(p.queries.len(), 1, "{:?}", p.queries);
    assert!(
        p.queries[0].contains("MODIFY COLUMN `v` BIGINT"),
        "{:?}",
        p.queries
    );
    assert!(!p.queries[0].contains("`ts`"), "{:?}", p.queries);
}

#[test]
fn mssql_partitioning_columns_are_blocked_as_partitioned_not_indexed() {
    let cols = [col("ts", "DATETIME2"), col("v", "INT")];
    let mut facts = partitioned(&[("ts", PartitionKey::Fixed)]);
    facts.indexed.insert("ts".into());
    facts.drop_indices_command = Some("mrsm drop indices -c a -m b".into());
    let p = plan(
        Flavor::Mssql,
        &cols,
        &[("ts", "datetime64[ns, UTC]"), ("v", "int")],
        facts,
    );
    let reason = p.blocked.as_deref().unwrap();
    assert!(
        reason.starts_with(
            "MSSQL cannot change the type of column 'ts' because the table is partitioned by it."
        ),
        "{reason}"
    );
    assert!(!reason.contains("drop indices"), "{reason}");
    assert_eq!(
        p.queries,
        vec!["ALTER TABLE [t] ALTER COLUMN [v] BIGINT NULL".to_string()]
    );
}

#[test]
fn partition_keys_from_catalog_rows() {
    let rows = |r: &[(&str, &str)]| -> Vec<(String, String)> {
        r.iter()
            .map(|(a, b)| (a.to_string(), b.to_string()))
            .collect()
    };
    let keys = partition_keys_from_rows(
        Flavor::Mysql,
        &rows(&[("`ts`", "RANGE COLUMNS"), ("unix_timestamp(`at`)", "RANGE")]),
    );
    assert_eq!(keys.get("ts"), Some(&PartitionKey::RangeColumns));
    assert_eq!(keys.get("at"), Some(&PartitionKey::Fixed));
    let keys = partition_keys_from_rows(
        Flavor::Mariadb,
        &rows(&[("`a`,`b`", "LIST COLUMNS"), ("`b`", "fixed")]),
    );
    assert_eq!(keys.get("a"), Some(&PartitionKey::RangeColumns));
    assert_eq!(keys.get("b"), Some(&PartitionKey::Fixed));
    let keys = partition_keys_from_rows(
        Flavor::Timescaledb,
        &rows(&[("ts", "time"), ("id", "fixed")]),
    );
    assert_eq!(keys.get("ts"), Some(&PartitionKey::TimeDimension));
    assert_eq!(keys.get("id"), Some(&PartitionKey::Fixed));
    let keys = partition_keys_from_rows(Flavor::Oracle, &rows(&[("TS", "fixed")]));
    assert_eq!(keys.get("ts"), Some(&PartitionKey::Fixed));
    assert!(get_partition_keys_queries(Flavor::Sqlite, "t", None).is_empty());
    assert!(get_partition_keys_queries(Flavor::Duckdb, "t", None).is_empty());
    assert_eq!(
        get_partition_keys_queries(Flavor::Timescaledb, "t", None).len(),
        2
    );
    assert!(
        get_partition_keys_queries(Flavor::Mssql, "t", None)[0].contains("OBJECT_ID(N'[dbo].[t]')")
    );
}

#[test]
fn mysql_modify_keeps_nullability_and_auto_increment() {
    let mut id = col("id", "INT");
    id.nullable = false;
    id.auto_increment = true;
    let cols = [id, col("s", "VARCHAR(10)")];
    let p = plan(
        Flavor::Mysql,
        &cols,
        &[("id", "int"), ("s", "string")],
        TableFacts::default(),
    );
    assert_eq!(p.queries.len(), 1);
    let q = &p.queries[0];
    assert!(
        q.contains("MODIFY COLUMN `id` BIGINT NOT NULL AUTO_INCREMENT"),
        "{q}"
    );
    assert!(q.contains("MODIFY COLUMN `s` TEXT"), "{q}");
    // An indexed bounded text column keeps its type on MySQL.
    let cols = [col("s", "VARCHAR(10)")];
    let mut facts = TableFacts::default();
    facts.indexed.insert("s".into());
    let p = plan(Flavor::Mysql, &cols, &[("s", "string")], facts);
    assert!(p.is_empty());
    assert_eq!(p.notes.len(), 1);
}

#[test]
fn mssql_alters_one_column_per_statement_and_blocks_indexed_columns() {
    let mut v = col("v", "INT");
    v.nullable = false;
    let cols = [v, col("w", "INT")];
    let mut facts = TableFacts::default();
    facts.indexed.insert("w".into());
    let p = plan(
        Flavor::Mssql,
        &cols,
        &[("v", "int"), ("w", "int"), ("n", "string")],
        facts,
    );
    assert_eq!(
        p.queries,
        vec![
            "ALTER TABLE [t] ADD [n] NVARCHAR(MAX)".to_string(),
            "ALTER TABLE [t] ALTER COLUMN [v] BIGINT NOT NULL".to_string(),
        ]
    );
    assert!(p.blocked.as_deref().unwrap().contains("indexed column 'w'"));
}

#[test]
fn oracle_changes_go_through_a_temporary_column() {
    let cols = [col("v", "NVARCHAR2(100)"), col("n", "NUMBER(10,0)")];
    let p = plan(
        Flavor::Oracle,
        &cols,
        &[("v", "string"), ("n", "float")],
        TableFacts::default(),
    );
    assert_eq!(p.changes[0].kind, SchemaChangeKind::Widen);
    assert_eq!(p.queries[0], "ALTER TABLE t MODIFY (v NVARCHAR2(2000))");
    assert!(p.queries.iter().any(|q| q == "UPDATE t SET n_mrsm_tmp = n"));
    assert_eq!(
        p.queries.last().unwrap(),
        "ALTER TABLE t DROP COLUMN n_mrsm_tmp"
    );
}

#[test]
fn sqlite_only_rebuilds_when_affinity_changes() {
    // Same affinities: nothing to do (INT and BIGINT are both INTEGER affinity).
    let cols = [
        col("id", "INT"),
        col("v", "REAL"),
        col("ts", "DATETIME"),
        col("j", "TEXT"),
        col("n", "TEXT"),
        col("b", "INTEGER"),
    ];
    let p = plan(
        Flavor::Sqlite,
        &cols,
        &[
            ("id", "int"),
            ("v", "float"),
            ("ts", "datetime64[ns, UTC]"),
            ("j", "json"),
            ("n", "numeric"),
            ("b", "bool"),
        ],
        TableFacts::default(),
    );
    assert!(p.is_empty(), "{p:?}");

    // An untyped column (a `CREATE TABLE AS SELECT` expression) is rebuilt with its type.
    let mut id = col("id", "INTEGER");
    id.pk = 1;
    id.nullable = false;
    let cols = [id, col("day", ""), col("v", "TEXT")];
    let p = plan(
        Flavor::Sqlite,
        &cols,
        &[("day", "datetime64[ns]"), ("v", "float"), ("new", "string")],
        TableFacts {
            sqlite_indexes: vec!["CREATE INDEX \"IX_t_day\" ON \"t\" (\"day\")".into()],
            sqlite_autoincrement: true,
            ..Default::default()
        },
    );
    assert!(
        p.changes
            .iter()
            .all(|c| c.column == "new" || c.note.as_deref() == Some("rebuilds the table"))
    );
    assert_eq!(p.queries[0], "DROP TABLE IF EXISTS \"t_mrsm_verify\"");
    assert!(
        p.queries[1].contains("\"id\" INTEGER PRIMARY KEY AUTOINCREMENT NOT NULL"),
        "{}",
        p.queries[1]
    );
    assert!(
        p.queries[1].contains("\"day\" DATETIME"),
        "{}",
        p.queries[1]
    );
    assert!(p.queries[1].contains("\"v\" FLOAT"), "{}", p.queries[1]);
    assert!(p.queries[1].contains("\"new\" TEXT"), "{}", p.queries[1]);
    assert_eq!(
        p.queries[2],
        "INSERT INTO \"t_mrsm_verify\" (\"id\", \"day\", \"v\")\nSELECT \"id\", \"day\", \"v\"\nFROM \"t\""
    );
    assert_eq!(p.queries[3], "DROP TABLE \"t\"");
    assert_eq!(
        p.queries[4],
        "ALTER TABLE \"t_mrsm_verify\" RENAME TO \"t\""
    );
    assert_eq!(p.queries[5], "CREATE INDEX \"IX_t_day\" ON \"t\" (\"day\")");
    // `--noask` (safe only) adds the column without rebuilding.
    assert_eq!(
        p.safe_queries,
        vec!["ALTER TABLE \"t\" ADD COLUMN \"new\" TEXT".to_string()]
    );
}

#[test]
fn geometry_changes_use_postgis_functions() {
    let cols = [col("geom", "GEOMETRY"), col("pt", "GEOMETRY(POINT, 4326)")];
    let p = plan(
        Flavor::Postgis,
        &cols,
        &[("geom", "geometry[Point, 4326]"), ("pt", "geometry")],
        TableFacts::default(),
    );
    assert_eq!(p.changes.len(), 1);
    assert!(
        p.queries[0].contains("USING ST_SetSRID(CAST(\"geom\" AS GEOMETRY), 4326)"),
        "{}",
        p.queries[0]
    );
}

#[test]
fn catalog_rows_compose_full_types() {
    let rows = [
        ColumnRow {
            column: "s".into(),
            type_: "character varying".into(),
            length: Some(20),
            nullable: Some("YES".into()),
            ..Default::default()
        },
        ColumnRow {
            column: "n".into(),
            type_: "numeric".into(),
            precision: Some(10),
            scale: Some(2),
            nullable: Some("NO".into()),
            ..Default::default()
        },
        ColumnRow {
            column: "g".into(),
            type_: "USER-DEFINED".into(),
            udt: Some("geometry".into()),
            ..Default::default()
        },
    ];
    let mut geometry = IndexMap::new();
    geometry.insert("g".to_string(), "GEOMETRY(POINT, 4326)".to_string());
    let cols = columns_from_rows(Flavor::Postgresql, &rows, &geometry);
    assert_eq!(cols[0].db_type, "VARCHAR(20)");
    assert!(cols[0].nullable);
    assert_eq!(cols[1].db_type, "NUMERIC(10,2)");
    assert!(!cols[1].nullable);
    assert_eq!(cols[2].db_type, "GEOMETRY(POINT, 4326)");
    let mssql = columns_from_rows(
        Flavor::Mssql,
        &[ColumnRow {
            column: "s".into(),
            type_: "nvarchar".into(),
            length: Some(-1),
            ..Default::default()
        }],
        &IndexMap::new(),
    );
    assert_eq!(mssql[0].db_type, "NVARCHAR(MAX)");
    let oracle = columns_from_rows(
        Flavor::Oracle,
        &[ColumnRow {
            column: "ID".into(),
            type_: "NUMBER".into(),
            scale: Some(0),
            nullable: Some("N".into()),
            ..Default::default()
        }],
        &IndexMap::new(),
    );
    assert_eq!(oracle[0].name, "id");
    assert_eq!(oracle[0].db_type, "INTEGER");
    assert!(!oracle[0].nullable);
}

#[test]
fn catalog_queries_per_flavor() {
    let q = get_schema_columns_query(Flavor::Postgresql, "t'x", Some("public"));
    assert!(q.contains("table_name = 't''x'"), "{q}");
    assert!(q.contains("table_schema = 'public'"), "{q}");
    assert!(get_schema_columns_query(Flavor::Sqlite, "t", None).contains("pragma_table_info('t')"));
    assert!(get_schema_columns_query(Flavor::Mysql, "t", None).contains("DATABASE()"));
    assert!(get_schema_columns_query(Flavor::Mssql, "t", None).contains("N'dbo'"));
    assert!(get_schema_columns_query(Flavor::Oracle, "t", None).contains("('t', 'T')"));
}
