//! `get_update_queries` and `get_null_replacement` (`meerschaum/utils/sql.py:1667-2044`).

use indexmap::IndexMap;

use crate::dateadd::{DateArg, Number, dateadd_str};
use crate::dtypes::{
    are_dtypes_equal, cast_dtype, get_pd_type_from_db_type, has_cast_dtypes, pd_to_db_entry,
};
use crate::flavor::Flavor;
use crate::ident::sql_item_name_opt;
use crate::queries::templates::update_queries;
use meerschaum_core::pyfmt::{dedent_strip, py_format, py_isupper};

/// `get_null_replacement(typ, flavor)`: a stand-in literal for `NULL` when joining.
///
/// `flavor = None` reproduces a non-flavor key (e.g. `'sqlite_delete_insert'`).
pub fn get_null_replacement(typ: &str, flavor: Option<Flavor>) -> String {
    let lower = typ.to_lowercase();
    if lower.contains("geometry") {
        return "'010100000000008058346FCDC100008058346FCDC1'".to_string();
    }
    if lower.contains("int") || lower == "numeric" || lower == "number" {
        return "-987654321".to_string();
    }
    if lower.contains("bool") || lower == "bit" {
        let mut bool_typ = pd_to_db_entry("bool", flavor).unwrap_or("BOOLEAN");
        if has_cast_dtypes(flavor) {
            bool_typ = cast_dtype(flavor, bool_typ).unwrap_or(bool_typ);
        }
        let val_to_cast = if matches!(flavor, Some(Flavor::Mysql | Flavor::Mariadb)) {
            -987654321
        } else {
            0
        };
        return format!("CAST({val_to_cast} AS {bool_typ})");
    }
    if lower.contains("time") || lower.contains("date") {
        let db_type = if py_isupper(typ) { Some(typ) } else { None };
        return dateadd_str(
            flavor,
            Some("day"),
            Number::Int(0),
            &DateArg::Str("1900-01-01".to_string()),
            db_type,
        )
        .unwrap_or_default();
    }
    if lower.contains("float") || lower.contains("double") || lower == "decimal" {
        return "-987654321.0".to_string();
    }
    if flavor == Some(Flavor::Oracle) && lower.split('(').next() == Some("char") {
        return "'-987654321'".to_string();
    }
    if flavor == Some(Flavor::Oracle) && (lower == "blob" || lower == "bytes") {
        return "00".to_string();
    }
    if matches!(lower.as_str(), "uniqueidentifier" | "guid" | "uuid") {
        let magic_val = "DEADBEEF-ABBA-BABE-CAFE-DECAFC0FFEE5";
        if flavor == Some(Flavor::Mssql) {
            return format!("CAST('{magic_val}' AS UNIQUEIDENTIFIER)");
        }
        return format!("'{magic_val}'");
    }
    format!(
        "{}'-987654321'",
        if flavor == Some(Flavor::Oracle) {
            "n"
        } else {
            ""
        }
    )
}

/// `expression` (a value of PostgreSQL type `db_type`) in a form `IS DISTINCT FROM` compares
/// so that "not distinct" means the stored value and the assigned one are interchangeable.
///
/// A type keeps its own equality only where equal values are interchangeable (numbers compare by
/// value: `1.50` and `1.5`, `-0.0` and `0.0` are unchanged). PostGIS types compare their EWKB
/// bytes, SRID included (`=` compares bounding boxes). Every other type compares its text: `json`
/// (it has no equality; a `jsonb` cast would hide key order and formatting and can fail on
/// input `json` accepts, such as `\u0000`), `interval` (`'1 day' = '24 hours'`), `xml`, geometric
/// types, extension types.
pub(crate) fn changed_only_comparable(expression: &str, db_type: &str) -> String {
    let upper = db_type.trim().to_uppercase();
    let (element, array) = match upper.strip_suffix("[]") {
        Some(element) => (element, true),
        None => match upper.strip_prefix('_') {
            Some(element) => (element, true),
            None => (upper.as_str(), upper == "ARRAY"),
        },
    };
    let element = element.split('(').next().unwrap_or_default().trim();
    if !array && matches!(element, "GEOMETRY" | "GEOGRAPHY") {
        return format!("CAST({expression} AS BYTEA)");
    }
    let equality = matches!(
        element,
        "SMALLINT"
            | "INTEGER"
            | "INT"
            | "BIGINT"
            | "INT2"
            | "INT4"
            | "INT8"
            | "NUMERIC"
            | "DECIMAL"
            | "REAL"
            | "FLOAT"
            | "FLOAT4"
            | "FLOAT8"
            | "DOUBLE PRECISION"
            | "BOOLEAN"
            | "BOOL"
            | "TEXT"
            | "VARCHAR"
            | "CHARACTER VARYING"
            | "CHAR"
            | "CHARACTER"
            | "BPCHAR"
            | "NAME"
            | "UUID"
            | "BYTEA"
            | "JSONB"
            | "DATE"
            | "TIME"
            | "TIMETZ"
            | "TIME WITHOUT TIME ZONE"
            | "TIME WITH TIME ZONE"
            | "TIMESTAMP"
            | "TIMESTAMPTZ"
            | "TIMESTAMP WITHOUT TIME ZONE"
            | "TIMESTAMP WITH TIME ZONE"
            | "INET"
            | "CIDR"
            | "MACADDR"
            | "MACADDR8"
            | "MONEY"
            | "BIT"
            | "VARBIT"
            | "BIT VARYING"
    );
    if equality {
        expression.to_string()
    } else {
        format!("CAST({expression} AS TEXT)")
    }
}

/// Arguments of `get_update_queries` (`utils/sql.py:1667`).
///
/// Python fetches `target_cols_types` / `patch_cols_types` from the database when they are not
/// given; here the caller supplies them (use the introspection queries in
/// [`crate::queries::table`]).
#[derive(Debug, Clone, PartialEq)]
pub struct UpdateQueriesArgs<'a> {
    /// `target`.
    pub target: &'a str,
    /// `patch`.
    pub patch: &'a str,
    /// `join_cols`.
    pub join_cols: Vec<String>,
    /// `flavor`.
    pub flavor: Flavor,
    /// `upsert`.
    pub upsert: bool,
    /// `datetime_col`.
    pub datetime_col: Option<&'a str>,
    /// `schema`.
    pub schema: Option<&'a str>,
    /// `patch_schema` (defaults to `schema`).
    pub patch_schema: Option<&'a str>,
    /// `target_cols_types`.
    pub target_cols_types: IndexMap<String, String>,
    /// `patch_cols_types`.
    pub patch_cols_types: IndexMap<String, String>,
    /// `identity_insert`.
    pub identity_insert: bool,
    /// `null_indices`.
    pub null_indices: bool,
    /// `cast_columns`.
    pub cast_columns: bool,
    /// The connector's `db_version` (SQLite only: versions `< '3.33.0'` use DELETE + INSERT).
    pub db_version: Option<&'a str>,
    /// The conflict target of the `ON CONFLICT (...)` upserts (PostgreSQL family, SQLite,
    /// GeoPackage): the arbiter unique index's own key columns and expressions, e.g.
    /// `"ts", (COALESCE("id", -987654321))`, so that the database matches the index that makes
    /// NULL keys equal. `None` renders the join columns, as v4 does.
    pub conflict_target: Option<String>,
    /// Upserts that match NULL keys although the table's unique key treats NULLs as distinct
    /// (`false` keeps v4's single statement). Set for `null_indices` pipes.
    ///
    /// - MySQL / MariaDB (always): an `UPDATE ... JOIN` and an `INSERT ... WHERE NOT EXISTS`
    ///   comparing the keys with `<=>` instead of `INSERT ... ON DUPLICATE KEY UPDATE`, which a
    ///   NULL key never triggers.
    /// - PostgreSQL family, SQLite, GeoPackage (when the conflict target's unique key is not
    ///   NULL-safe: a v4 / rc14 table, SQLite, PostgreSQL before 15): the rows whose keys are all
    ///   set take the usual `ON CONFLICT`, and the rows with a NULL key (the datetime column
    ///   aside) are updated where they match (`COALESCE(...)` equality, `IS` on SQLite) and
    ///   inserted where they do not.
    ///
    /// The patch must hold one row per key (the caller keeps a repeated key's last row).
    pub null_safe_upsert: bool,
    /// Upserts that leave a conflicting row alone when none of its value columns would change
    /// (the `upsert_changed_only` pipe parameter): `ON CONFLICT ... DO UPDATE ... WHERE (f.cols)
    /// IS DISTINCT FROM (new values)`. PostgreSQL, PostGIS, TimescaleDB and Citus only; other
    /// flavors ignore it.
    pub changed_only: bool,
}

impl<'a> UpdateQueriesArgs<'a> {
    /// Defaults matching the Python signature.
    pub fn new(target: &'a str, patch: &'a str, join_cols: Vec<String>, flavor: Flavor) -> Self {
        UpdateQueriesArgs {
            target,
            patch,
            join_cols,
            flavor,
            upsert: false,
            datetime_col: None,
            schema: None,
            patch_schema: None,
            target_cols_types: IndexMap::new(),
            patch_cols_types: IndexMap::new(),
            identity_insert: false,
            null_indices: true,
            cast_columns: true,
            db_version: None,
            conflict_target: None,
            null_safe_upsert: false,
            changed_only: false,
        }
    }
}

/// Whether `get_update_queries(args)` renders the `upsert_changed_only` guard: an upsert with
/// `changed_only` on the PostgreSQL family that has value columns to compare (an upsert of key
/// columns only is `DO NOTHING`). The statements' summed row counts are then the rows written.
pub fn changed_only_applies(args: &UpdateQueriesArgs<'_>) -> bool {
    args.upsert
        && args.changed_only
        && matches!(
            args.flavor,
            Flavor::Postgresql
                | Flavor::Postgis
                | Flavor::Timescaledb
                | Flavor::TimescaledbHa
                | Flavor::Citus
        )
        && args
            .target_cols_types
            .keys()
            .any(|c| args.patch_cols_types.contains_key(c) && !args.join_cols.contains(c))
}

/// `get_update_queries(...)`: the `MERGE` / `UPDATE` / upsert queries applying `patch` to `target`.
pub fn get_update_queries(args: &UpdateQueriesArgs<'_>) -> Vec<String> {
    let mut flavor_name = args.flavor.as_str().to_string();
    // PARITY: string comparison of versions (`connectable.db_version < '3.33.0'`,
    // `utils/sql.py:1758`), so `'3.9.0'` is *not* considered older.
    let old_sqlite = matches!(args.flavor, Flavor::Sqlite | Flavor::Geopackage)
        && args.db_version.is_some_and(|v| v < "3.33.0");
    // PARITY: on old SQLite the flavor key becomes `'sqlite_delete_insert'`, which is also passed
    // to `sql_item_name`, `get_null_replacement` and friends as if it were a flavor (`None`).
    let flavor: Option<Flavor> = if old_sqlite {
        flavor_name = "sqlite_delete_insert".to_string();
        None
    } else {
        Some(args.flavor)
    };
    let flavor_key = if args.upsert {
        format!("{flavor_name}-upsert")
    } else {
        flavor_name.clone()
    };
    let base_queries = update_queries(&flavor_key);
    let schema = args.schema;
    let patch_schema = args.patch_schema.or(schema);
    let target_table_columns = &args.target_cols_types;
    let patch_table_columns = &args.patch_cols_types;

    let q = |item: &str| sql_item_name_opt(item, flavor, None);

    let patch_cols_str = patch_table_columns
        .keys()
        .map(|c| q(c))
        .collect::<Vec<_>>()
        .join(", ");
    let patch_cols_prefixed_str = patch_table_columns
        .keys()
        .map(|c| format!("p.{}", q(c)))
        .collect::<Vec<_>>()
        .join(", ");
    let join_cols_str = match args.conflict_target.as_deref().filter(|t| !t.is_empty()) {
        Some(target) => target.to_string(),
        None => args
            .join_cols
            .iter()
            .map(|c| q(c))
            .collect::<Vec<_>>()
            .join(", "),
    };

    let mut value_cols: Vec<(String, String)> = Vec::new();
    let mut join_cols_types: Vec<(String, String)> = Vec::new();
    for (c_name, c_type) in target_table_columns {
        if !patch_table_columns.contains_key(c_name) {
            continue;
        }
        let mut c_type = c_type.clone();
        if has_cast_dtypes(flavor)
            && let Some(cast) = cast_dtype(flavor, &c_type.to_uppercase())
        {
            c_type = cast.to_string();
        }
        if args.join_cols.contains(c_name) {
            join_cols_types.push((c_name.clone(), c_type));
        } else {
            value_cols.push((c_name.clone(), c_type));
        }
    }

    if join_cols_types.is_empty() {
        return Vec::new();
    }
    if value_cols.is_empty() && !args.upsert {
        return Vec::new();
    }

    let coalesce_join_cols_str = join_cols_types
        .iter()
        .map(|(c_name, c_type)| {
            if args.null_indices {
                format!(
                    "COALESCE({}, {})",
                    q(c_name),
                    get_null_replacement(c_type, flavor)
                )
            } else {
                q(c_name)
            }
        })
        .collect::<Vec<_>>()
        .join(", ");

    let update_or_nothing = if value_cols.is_empty() {
        "NOTHING"
    } else {
        "UPDATE"
    };

    let tz_naive = flavor.is_some_and(Flavor::timezone_naive);
    // The value assigned to a value column (`r_prefix` is the patch or `EXCLUDED.`).
    let set_value = |c_name: &str, c_type: &str, r_prefix: &str| -> String {
        let utc = !tz_naive
            && get_pd_type_from_db_type(c_type, true)
                .to_lowercase()
                .contains("utc");
        let skip_cast = !args.cast_columns
            || (flavor == Some(Flavor::Oracle)
                && are_dtypes_equal(&get_pd_type_from_db_type(c_type, true), "bytes"));
        if skip_cast || matches!(flavor, Some(Flavor::Sqlite | Flavor::Geopackage)) {
            return format!("{r_prefix}{}", q(c_name));
        }
        format!(
            "CAST({r_prefix}{} AS {}){}",
            q(c_name),
            c_type.replace('_', " "),
            if utc { " AT TIME ZONE 'UTC'" } else { "" }
        )
    };
    let sets_subquery = |l_prefix: &str, r_prefix: &str| -> String {
        if value_cols.is_empty() {
            return String::new();
        }
        let parts: Vec<String> = value_cols
            .iter()
            .map(|(c_name, c_type)| {
                format!(
                    "{l_prefix}{} = {}",
                    q(c_name),
                    set_value(c_name, c_type, r_prefix)
                )
            })
            .collect();
        format!("SET {}", parts.join(",\n"))
    };
    let changed_only = changed_only_applies(args);
    // `(f.a, f.b) IS DISTINCT FROM (<new a>, <new b>)`: whether a stored row `f` would change.
    let distinct_subquery = |r_prefix: &str| -> String {
        let (old, new): (Vec<String>, Vec<String>) = value_cols
            .iter()
            .map(|(c_name, c_type)| {
                (
                    changed_only_comparable(&format!("f.{}", q(c_name)), c_type),
                    changed_only_comparable(&set_value(c_name, c_type, r_prefix), c_type),
                )
            })
            .unzip();
        format!("({}) IS DISTINCT FROM ({})", old.join(", "), new.join(", "))
    };

    let and_subquery = |l_prefix: &str, r_prefix: &str| -> String {
        join_cols_types
            .iter()
            .map(|(c_name, c_type)| {
                if args.null_indices {
                    let null = get_null_replacement(c_type, flavor);
                    format!(
                        "COALESCE({l_prefix}{}, {null})\n                =\n                COALESCE({r_prefix}{}, {null})",
                        q(c_name),
                        q(c_name)
                    )
                } else {
                    format!("{l_prefix}{} = {r_prefix}{}", q(c_name), q(c_name))
                }
            })
            .collect::<Vec<_>>()
            .join("\n            AND\n                ")
    };

    let is_mssql = flavor == Some(Flavor::Mssql);
    let target_table_name = sql_item_name_opt(args.target, flavor, schema);
    let patch_table_name = sql_item_name_opt(args.patch, flavor, patch_schema);
    let dt_col_name = args.datetime_col.filter(|d| !d.is_empty()).map(q);
    let dt = dt_col_name.clone().unwrap_or_else(|| "None".to_string());
    let date_bounds_table = if is_mssql {
        "[date_bounds]".to_string()
    } else {
        patch_table_name.clone()
    };
    let min_dt_col_name = if is_mssql {
        "[Min_dt]".to_string()
    } else {
        format!("MIN({dt})")
    };
    let max_dt_col_name = if is_mssql {
        "[Max_dt]".to_string()
    } else {
        format!("MAX({dt})")
    };
    let has_dt = dt_col_name.is_some();
    let date_bounds_subquery = if has_dt {
        format!(
            "f.{dt} >= (SELECT {min_dt_col_name} FROM {date_bounds_table})\n            AND\n                f.{dt} <= (SELECT {max_dt_col_name} FROM {date_bounds_table})"
        )
    } else {
        "1 = 1".to_string()
    };
    let with_temp_date_bounds = if has_dt {
        format!(
            "WITH [date_bounds] AS (\n        SELECT MIN({dt}) AS {min_dt_col_name}, MAX({dt}) AS {max_dt_col_name}\n        FROM {patch_table_name}\n    )"
        )
    } else {
        String::new()
    };
    let identity_insert_on = if args.identity_insert {
        format!("SET IDENTITY_INSERT {target_table_name} ON")
    } else {
        String::new()
    };
    let identity_insert_off = if args.identity_insert {
        format!("SET IDENTITY_INSERT {target_table_name} OFF")
    } else {
        String::new()
    };
    // NOTE: MSSQL upserts must exclude the update portion if only upserting indices.
    let when_matched_update_sets_subquery_none = if value_cols.is_empty() {
        String::new()
    } else {
        format!(
            "\n        WHEN MATCHED THEN\n            UPDATE {}",
            sets_subquery("", "p.")
        )
    };
    let cols_equal_values = value_cols
        .iter()
        .map(|(c_name, _)| format!("{} = VALUES({})", q(c_name), q(c_name)))
        .collect::<Vec<_>>()
        .join("\n,");
    let on_duplicate_key_update = if value_cols.is_empty() {
        ""
    } else {
        "ON DUPLICATE KEY UPDATE"
    };
    let ignore = if value_cols.is_empty() { "IGNORE " } else { "" };

    if args.upsert
        && args.null_safe_upsert
        && matches!(flavor, Some(Flavor::Mysql | Flavor::Mariadb))
    {
        // NULL-safe MySQL upsert: update the rows whose keys match (`<=>` treats NULLs as
        // equal and can use the key index), then insert the rest. The patch holds one row per
        // key (the caller keeps the last row of a key, as `ON DUPLICATE KEY UPDATE` resolves a
        // batch), and the insert keeps v4's `ON DUPLICATE KEY UPDATE` (`INSERT IGNORE` without
        // value columns) so that a non-NULL key another writer inserted in between updates
        // instead of failing on the unique key.
        let null_safe_on = join_cols_types
            .iter()
            .map(|(c_name, _)| format!("f.{} <=> p.{}", q(c_name), q(c_name)))
            .collect::<Vec<_>>()
            .join("\n    AND ");
        let mut queries = Vec::new();
        if !value_cols.is_empty() {
            // Uncast, as `ON DUPLICATE KEY UPDATE col = VALUES(col)` assigns (MySQL cannot
            // `CAST(... AS BLOB)`).
            let sets = value_cols
                .iter()
                .map(|(c_name, _)| format!("f.{} = p.{}", q(c_name), q(c_name)))
                .collect::<Vec<_>>()
                .join(",\n    ");
            queries.push(format!(
                "UPDATE {target_table_name} AS f\nJOIN (SELECT {patch_cols_str} FROM {patch_table_name}) AS p\n    ON {null_safe_on}\nSET {sets}"
            ));
        }
        let on_duplicate = if value_cols.is_empty() {
            String::new()
        } else {
            format!("\n{on_duplicate_key_update}\n    {cols_equal_values}")
        };
        queries.push(format!(
            "INSERT {ignore}INTO {target_table_name} ({patch_cols_str})\nSELECT {patch_cols_prefixed_str}\nFROM {patch_table_name} AS p\nWHERE NOT EXISTS (\n    SELECT 1\n    FROM {target_table_name} AS f\n    WHERE {null_safe_on}\n){on_duplicate}"
        ));
        return queries;
    }

    let sets_none = sets_subquery("", "p.");
    let sets_excluded = sets_subquery("", "EXCLUDED.");
    // The `upsert_changed_only` form of the `ON CONFLICT` upsert (`filter` restricts the patch
    // rows): the target is aliased `f` so the condition can name the stored row.
    let changed_only_upsert = |filter: &str| -> String {
        format!(
            "INSERT INTO {target_table_name} AS f ({patch_cols_str})\nSELECT {patch_cols_str}\nFROM {patch_table_name}{filter}\nON CONFLICT ({join_cols_str}) DO UPDATE {sets_excluded}\nWHERE {}",
            distinct_subquery("EXCLUDED.")
        )
    };

    let null_safe_family = matches!(
        flavor,
        Some(
            Flavor::Postgresql
                | Flavor::Postgis
                | Flavor::Timescaledb
                | Flavor::TimescaledbHa
                | Flavor::Sqlite
                | Flavor::Geopackage
        )
    );
    // The key columns that may be NULL (the datetime column is never wrapped, as in the
    // NULL-safe unique keys; a NULL datetime keeps v4's behavior).
    let nullable_keys: Vec<&(String, String)> = join_cols_types
        .iter()
        .filter(|(c, _)| Some(c.as_str()) != args.datetime_col)
        .collect();
    if args.upsert && args.null_safe_upsert && null_safe_family && !nullable_keys.is_empty() {
        let sqlite = matches!(flavor, Some(Flavor::Sqlite | Flavor::Geopackage));
        let any_null = |prefix: &str| {
            nullable_keys
                .iter()
                .map(|(c, _)| format!("{prefix}{} IS NULL", q(c)))
                .collect::<Vec<_>>()
                .join(" OR ")
        };
        let none_null = nullable_keys
            .iter()
            .map(|(c, _)| format!("{} IS NOT NULL", q(c)))
            .collect::<Vec<_>>()
            .join(" AND ");
        // NULL-safe key equality: `IS` on SQLite (it can use the key index), `COALESCE(...)`
        // equality elsewhere (hashable); the datetime column compares with `=` (indexed).
        let keys_match = join_cols_types
            .iter()
            .map(|(c_name, c_type)| {
                if Some(c_name.as_str()) == args.datetime_col {
                    format!("f.{} = p.{}", q(c_name), q(c_name))
                } else if sqlite {
                    format!("f.{} IS p.{}", q(c_name), q(c_name))
                } else {
                    let null = get_null_replacement(c_type, flavor);
                    format!(
                        "COALESCE(f.{}, {null}) = COALESCE(p.{}, {null})",
                        q(c_name),
                        q(c_name)
                    )
                }
            })
            .collect::<Vec<_>>()
            .join("\n    AND ");
        let mut queries = Vec::new();
        // The rows whose keys are all set: the usual single statement.
        queries.push(if changed_only {
            changed_only_upsert(&format!("\nWHERE {none_null}"))
        } else {
            format!(
                "INSERT INTO {target_table_name} ({patch_cols_str})\nSELECT {patch_cols_str}\nFROM {patch_table_name}\nWHERE {none_null}\nON CONFLICT ({join_cols_str}) DO {update_or_nothing} {sets_excluded}"
            ).trim_end().to_string()
        });
        // The rows with a NULL key: update those that exist, then insert the others.
        if !value_cols.is_empty() {
            let changed = if changed_only {
                format!("\n    AND {}", distinct_subquery("p."))
            } else {
                String::new()
            };
            queries.push(format!(
                "UPDATE {target_table_name} AS f\n{sets_none}\nFROM (SELECT {patch_cols_str} FROM {patch_table_name} WHERE {}) AS p\nWHERE {keys_match}{changed}",
                any_null("")
            ));
        }
        queries.push(format!(
            "INSERT INTO {target_table_name} ({patch_cols_str})\nSELECT {patch_cols_prefixed_str}\nFROM {patch_table_name} AS p\nWHERE ({})\n    AND NOT EXISTS (\n        SELECT 1\n        FROM {target_table_name} AS f\n        WHERE {keys_match}\n    )",
            any_null("p.")
        ));
        return queries;
    }
    if changed_only {
        return vec![changed_only_upsert("")];
    }
    let sets_f = sets_subquery("f.", "p.");
    let and_f = and_subquery("p.", "f.");
    let and_t = and_subquery("p.", "t.");
    let fmt_args: [(&str, &str); 20] = [
        ("sets_subquery_none", &sets_none),
        ("sets_subquery_none_excluded", &sets_excluded),
        ("sets_subquery_f", &sets_f),
        ("and_subquery_f", &and_f),
        ("and_subquery_t", &and_t),
        ("target_table_name", &target_table_name),
        ("patch_table_name", &patch_table_name),
        ("patch_cols_str", &patch_cols_str),
        ("patch_cols_prefixed_str", &patch_cols_prefixed_str),
        ("date_bounds_subquery", &date_bounds_subquery),
        ("join_cols_str", &join_cols_str),
        ("coalesce_join_cols_str", &coalesce_join_cols_str),
        ("update_or_nothing", update_or_nothing),
        (
            "when_matched_update_sets_subquery_none",
            &when_matched_update_sets_subquery_none,
        ),
        ("cols_equal_values", &cols_equal_values),
        ("on_duplicate_key_update", on_duplicate_key_update),
        ("ignore", ignore),
        ("with_temp_date_bounds", &with_temp_date_bounds),
        ("identity_insert_on", &identity_insert_on),
        ("identity_insert_off", &identity_insert_off),
    ];

    base_queries
        .iter()
        .map(|base| dedent_strip(&py_format(base, &fmt_args)))
        .filter(|q| !q.is_empty())
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn null_replacements() {
        assert_eq!(
            get_null_replacement("BIGINT", Some(Flavor::Postgresql)),
            "-987654321"
        );
        assert_eq!(
            get_null_replacement("BOOLEAN", Some(Flavor::Mysql)),
            "CAST(-987654321 AS SIGNED INT)"
        );
        assert_eq!(
            get_null_replacement("TIMESTAMPTZ", Some(Flavor::Postgresql)),
            "CAST('1900-01-01' AS TIMESTAMPTZ)"
        );
        assert_eq!(
            get_null_replacement("TEXT", Some(Flavor::Oracle)),
            "n'-987654321'"
        );
    }

    #[test]
    fn upsert_conflict_target_matches_the_unique_index() {
        let mut args =
            UpdateQueriesArgs::new("t", "p", vec!["ts".into(), "id".into()], Flavor::Postgresql);
        args.upsert = true;
        args.target_cols_types = [("ts", "TIMESTAMP"), ("id", "BIGINT"), ("v", "TEXT")]
            .into_iter()
            .map(|(a, b)| (a.to_string(), b.to_string()))
            .collect();
        args.patch_cols_types = args.target_cols_types.clone();
        args.conflict_target = Some("ts, (COALESCE(id, '-987654321'::integer))".into());
        let qs = get_update_queries(&args);
        assert_eq!(qs.len(), 1);
        assert!(
            qs[0].contains("ON CONFLICT (ts, (COALESCE(id, '-987654321'::integer))) DO UPDATE"),
            "{}",
            qs[0]
        );
        // SQLite renders the same way.
        args.flavor = Flavor::Sqlite;
        args.conflict_target = Some("\"ts\", COALESCE(\"id\", -987654321)".into());
        let qs = get_update_queries(&args);
        assert!(
            qs[0].contains("ON CONFLICT (\"ts\", COALESCE(\"id\", -987654321)) DO UPDATE"),
            "{}",
            qs[0]
        );
    }

    #[test]
    fn mysql_null_safe_upsert_updates_then_inserts() {
        let mut args = UpdateQueriesArgs::new("t", "p", vec!["id".into()], Flavor::Mysql);
        args.upsert = true;
        args.null_safe_upsert = true;
        args.target_cols_types = [("id", "BIGINT"), ("v", "TEXT")]
            .into_iter()
            .map(|(a, b)| (a.to_string(), b.to_string()))
            .collect();
        args.patch_cols_types = args.target_cols_types.clone();
        let qs = get_update_queries(&args);
        assert_eq!(qs.len(), 2, "{qs:?}");
        assert!(qs[0].starts_with(
            "UPDATE `t` AS f\nJOIN (SELECT `id`, `v` FROM `p`) AS p\n    ON f.`id` <=> p.`id`"
        ));
        assert!(qs[0].ends_with("SET f.`v` = p.`v`"), "{}", qs[0]);
        assert!(qs[1].starts_with(
            "INSERT INTO `t` (`id`, `v`)\nSELECT p.`id`, p.`v`\nFROM `p` AS p\nWHERE NOT EXISTS"
        ));
        assert!(qs[1].contains("WHERE f.`id` <=> p.`id`"));
        // A key another writer inserted in between updates instead of failing.
        assert!(
            qs[1].ends_with(")\nON DUPLICATE KEY UPDATE\n    `v` = VALUES(`v`)"),
            "{}",
            qs[1]
        );
        // Only index columns: insert the missing keys.
        args.target_cols_types.shift_remove("v");
        args.patch_cols_types.shift_remove("v");
        let qs = get_update_queries(&args);
        assert_eq!(qs.len(), 1);
        assert!(qs[0].starts_with("INSERT IGNORE INTO `t` (`id`)"), "{qs:?}");
        assert!(qs[0].ends_with(')'), "{qs:?}");
        // v4's template without the flag.
        args.null_safe_upsert = false;
        let qs = get_update_queries(&args);
        assert!(qs[0].starts_with("INSERT IGNORE INTO"), "{qs:?}");
    }

    #[test]
    fn null_safe_upserts_split_rows_with_null_keys() {
        let mut args =
            UpdateQueriesArgs::new("t", "p", vec!["ts".into(), "id".into()], Flavor::Postgresql);
        args.upsert = true;
        args.null_safe_upsert = true;
        args.datetime_col = Some("ts");
        args.target_cols_types = [("ts", "TIMESTAMP"), ("id", "BIGINT"), ("v", "TEXT")]
            .into_iter()
            .map(|(a, b)| (a.to_string(), b.to_string()))
            .collect();
        args.patch_cols_types = args.target_cols_types.clone();
        let qs = get_update_queries(&args);
        assert_eq!(qs.len(), 3, "{qs:#?}");
        // Keys all set: v4's statement.
        assert_eq!(
            qs[0],
            "INSERT INTO \"t\" (\"ts\", \"id\", \"v\")\nSELECT \"ts\", \"id\", \"v\"\nFROM \"p\"\nWHERE \"id\" IS NOT NULL\nON CONFLICT (\"ts\", \"id\") DO UPDATE SET \"v\" = CAST(EXCLUDED.\"v\" AS TEXT)"
        );
        // NULL keys: update the matches, insert the rest (the datetime compares with `=`).
        assert!(
            qs[1].starts_with("UPDATE \"t\" AS f\nSET \"v\" = CAST(p.\"v\" AS TEXT)\nFROM (SELECT \"ts\", \"id\", \"v\" FROM \"p\" WHERE \"id\" IS NULL) AS p\nWHERE f.\"ts\" = p.\"ts\"\n    AND COALESCE(f.\"id\", -987654321) = COALESCE(p.\"id\", -987654321)"),
            "{}",
            qs[1]
        );
        assert!(
            qs[2].contains("WHERE (p.\"id\" IS NULL)\n    AND NOT EXISTS ("),
            "{}",
            qs[2]
        );
        // SQLite compares with `IS` (indexable).
        args.flavor = Flavor::Sqlite;
        let qs = get_update_queries(&args);
        assert!(qs[1].contains("AND f.\"id\" IS p.\"id\""), "{}", qs[1]);
        // Only the datetime is a key: nothing can be NULL-equal, v4's statement.
        args.join_cols = vec!["ts".into()];
        assert_eq!(get_update_queries(&args).len(), 1);
    }

    #[test]
    fn upsert_pg() {
        let mut args = UpdateQueriesArgs::new("t", "p", vec!["id".into()], Flavor::Postgresql);
        args.upsert = true;
        args.target_cols_types = [("id", "BIGINT"), ("v", "TEXT")]
            .into_iter()
            .map(|(a, b)| (a.to_string(), b.to_string()))
            .collect();
        args.patch_cols_types = args.target_cols_types.clone();
        let qs = get_update_queries(&args);
        assert_eq!(
            qs,
            vec![
                "INSERT INTO \"t\" (\"id\", \"v\")\nSELECT \"id\", \"v\"\nFROM \"p\"\nON CONFLICT (\"id\") DO UPDATE SET \"v\" = CAST(EXCLUDED.\"v\" AS TEXT)"
            ]
        );
    }

    fn changed_only_args(flavor: Flavor) -> UpdateQueriesArgs<'static> {
        let mut args = UpdateQueriesArgs::new("t", "p", vec!["ts".into(), "id".into()], flavor);
        args.upsert = true;
        args.changed_only = true;
        args.schema = Some("s");
        args.target_cols_types = [
            ("ts", "TIMESTAMP WITH TIME ZONE"),
            ("id", "BIGINT"),
            ("v", "DOUBLE PRECISION"),
            ("doc", "JSON"),
            ("geom", "GEOMETRY(Point,4326)"),
            ("tags", "TEXT[]"),
            ("vec", "VECTOR(3)"),
        ]
        .into_iter()
        .map(|(a, b)| (a.to_string(), b.to_string()))
        .collect();
        args.patch_cols_types = args.target_cols_types.clone();
        args
    }

    #[test]
    fn changed_only_upsert_skips_unchanged_rows_on_the_postgresql_family() {
        for flavor in [
            Flavor::Postgresql,
            Flavor::Postgis,
            Flavor::Timescaledb,
            Flavor::TimescaledbHa,
            Flavor::Citus,
        ] {
            let qs = get_update_queries(&changed_only_args(flavor));
            assert_eq!(
                qs,
                vec![
                    "INSERT INTO \"s\".\"t\" AS f (\"ts\", \"id\", \"v\", \"doc\", \"geom\", \"tags\", \"vec\")\n\
                     SELECT \"ts\", \"id\", \"v\", \"doc\", \"geom\", \"tags\", \"vec\"\n\
                     FROM \"s\".\"p\"\n\
                     ON CONFLICT (\"ts\", \"id\") DO UPDATE SET \"v\" = CAST(EXCLUDED.\"v\" AS DOUBLE PRECISION),\n\
                     \"doc\" = CAST(EXCLUDED.\"doc\" AS JSON),\n\
                     \"geom\" = CAST(EXCLUDED.\"geom\" AS GEOMETRY(Point,4326)),\n\
                     \"tags\" = CAST(EXCLUDED.\"tags\" AS TEXT[]),\n\
                     \"vec\" = CAST(EXCLUDED.\"vec\" AS VECTOR(3))\n\
                     WHERE (f.\"v\", CAST(f.\"doc\" AS TEXT), CAST(f.\"geom\" AS BYTEA), f.\"tags\", CAST(f.\"vec\" AS TEXT)) \
                     IS DISTINCT FROM (CAST(EXCLUDED.\"v\" AS DOUBLE PRECISION), \
                     CAST(CAST(EXCLUDED.\"doc\" AS JSON) AS TEXT), \
                     CAST(CAST(EXCLUDED.\"geom\" AS GEOMETRY(Point,4326)) AS BYTEA), \
                     CAST(EXCLUDED.\"tags\" AS TEXT[]), \
                     CAST(CAST(EXCLUDED.\"vec\" AS VECTOR(3)) AS TEXT))"
                ],
                "{flavor:?}"
            );
        }
    }

    #[test]
    fn changed_only_upsert_leaves_other_flavors_and_index_only_pipes_alone() {
        for flavor in [
            Flavor::Cockroachdb,
            Flavor::Sqlite,
            Flavor::Geopackage,
            Flavor::Mysql,
            Flavor::Mariadb,
            Flavor::Mssql,
            Flavor::Oracle,
            Flavor::Duckdb,
        ] {
            let mut args = changed_only_args(flavor);
            let with = get_update_queries(&args);
            args.changed_only = false;
            assert_eq!(with, get_update_queries(&args), "{flavor:?}");
        }
        // Plain updates (`upsert: false`) are unchanged.
        let mut args = changed_only_args(Flavor::Postgresql);
        args.upsert = false;
        let with = get_update_queries(&args);
        args.changed_only = false;
        assert_eq!(with, get_update_queries(&args));
        // Only index columns: `DO NOTHING` has nothing to compare.
        let mut args = changed_only_args(Flavor::Postgresql);
        args.target_cols_types.retain(|c, _| c == "ts" || c == "id");
        args.patch_cols_types = args.target_cols_types.clone();
        let qs = get_update_queries(&args);
        assert!(
            qs[0].ends_with("ON CONFLICT (\"ts\", \"id\") DO NOTHING"),
            "{qs:?}"
        );
        assert!(!qs[0].contains("AS f"), "{qs:?}");
    }

    #[test]
    fn changed_only_upsert_compares_the_assigned_value() {
        let mut args = UpdateQueriesArgs::new("t", "p", vec!["id".into()], Flavor::Postgresql);
        args.upsert = true;
        args.changed_only = true;
        args.target_cols_types = [("id", "BIGINT"), ("v", "TEXT")]
            .into_iter()
            .map(|(a, b)| (a.to_string(), b.to_string()))
            .collect();
        args.patch_cols_types = args.target_cols_types.clone();
        args.conflict_target = Some("(COALESCE(id, '-987654321'::bigint))".into());
        let qs = get_update_queries(&args);
        assert_eq!(
            qs,
            vec![
                "INSERT INTO \"t\" AS f (\"id\", \"v\")\nSELECT \"id\", \"v\"\nFROM \"p\"\n\
                 ON CONFLICT ((COALESCE(id, '-987654321'::bigint))) DO UPDATE SET \"v\" = CAST(EXCLUDED.\"v\" AS TEXT)\n\
                 WHERE (f.\"v\") IS DISTINCT FROM (CAST(EXCLUDED.\"v\" AS TEXT))"
            ]
        );
        // `enforce: false` assigns the value uncast.
        args.cast_columns = false;
        let qs = get_update_queries(&args);
        assert!(
            qs[0].ends_with(
                "SET \"v\" = EXCLUDED.\"v\"\nWHERE (f.\"v\") IS DISTINCT FROM (EXCLUDED.\"v\")"
            ),
            "{qs:?}"
        );
    }

    #[test]
    fn changed_only_null_safe_upsert_guards_both_writes() {
        let mut args =
            UpdateQueriesArgs::new("t", "p", vec!["ts".into(), "id".into()], Flavor::Postgresql);
        args.upsert = true;
        args.null_safe_upsert = true;
        args.changed_only = true;
        args.datetime_col = Some("ts");
        args.target_cols_types = [("ts", "TIMESTAMP"), ("id", "BIGINT"), ("v", "TEXT")]
            .into_iter()
            .map(|(a, b)| (a.to_string(), b.to_string()))
            .collect();
        args.patch_cols_types = args.target_cols_types.clone();
        let qs = get_update_queries(&args);
        assert_eq!(qs.len(), 3, "{qs:#?}");
        assert_eq!(
            qs[0],
            "INSERT INTO \"t\" AS f (\"ts\", \"id\", \"v\")\nSELECT \"ts\", \"id\", \"v\"\nFROM \"p\"\nWHERE \"id\" IS NOT NULL\n\
             ON CONFLICT (\"ts\", \"id\") DO UPDATE SET \"v\" = CAST(EXCLUDED.\"v\" AS TEXT)\n\
             WHERE (f.\"v\") IS DISTINCT FROM (CAST(EXCLUDED.\"v\" AS TEXT))"
        );
        assert!(
            qs[1].ends_with(
                "AND COALESCE(f.\"id\", -987654321) = COALESCE(p.\"id\", -987654321)\n    AND (f.\"v\") IS DISTINCT FROM (CAST(p.\"v\" AS TEXT))"
            ),
            "{}",
            qs[1]
        );
        assert!(qs[2].contains("AND NOT EXISTS ("), "{}", qs[2]);
    }

    #[test]
    fn changed_only_comparable_types() {
        let c = |t: &str| changed_only_comparable("x", t);
        for t in [
            "NUMERIC(28,10)",
            "TIMESTAMP(6) WITH TIME ZONE",
            "timestamp without time zone",
            "BYTEA",
            "JSONB",
            "UUID",
            "INTEGER[]",
            "_int8",
            "BOOLEAN",
            "DATE",
        ] {
            assert_eq!(c(t), "x", "{t}");
        }
        assert_eq!(c("geometry"), "CAST(x AS BYTEA)");
        assert_eq!(c("GEOGRAPHY(Point,4326)"), "CAST(x AS BYTEA)");
        for t in [
            "JSON",
            "json",
            "JSON[]",
            "_json",
            "INTERVAL",
            "interval day to second",
            "INTERVAL[]",
            "GEOMETRY[]",
            "XML",
            "POINT",
            "ARRAY",
            "USER-DEFINED",
            "",
        ] {
            assert_eq!(c(t), "CAST(x AS TEXT)", "{t}");
        }
    }
}
