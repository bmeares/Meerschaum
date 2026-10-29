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
        }
    }
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
    let sets_subquery = |l_prefix: &str, r_prefix: &str| -> String {
        if value_cols.is_empty() {
            return String::new();
        }
        let parts: Vec<String> = value_cols
            .iter()
            .map(|(c_name, c_type)| {
                let utc = !tz_naive
                    && get_pd_type_from_db_type(c_type, true)
                        .to_lowercase()
                        .contains("utc");
                let skip_cast = !args.cast_columns
                    || (flavor == Some(Flavor::Oracle)
                        && are_dtypes_equal(&get_pd_type_from_db_type(c_type, true), "bytes"));
                let (open, mid, close) =
                    if skip_cast || matches!(flavor, Some(Flavor::Sqlite | Flavor::Geopackage)) {
                        (String::new(), String::new(), String::new())
                    } else {
                        (
                            "CAST(".to_string(),
                            format!(" AS {}", c_type.replace('_', " ")),
                            format!("){}", if utc { " AT TIME ZONE 'UTC'" } else { "" }),
                        )
                    };
                format!(
                    "{l_prefix}{} = {open}{r_prefix}{}{mid}{close}",
                    q(c_name),
                    q(c_name)
                )
            })
            .collect();
        format!("SET {}", parts.join(",\n"))
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
        queries.push(format!(
            "INSERT INTO {target_table_name} ({patch_cols_str})\nSELECT {patch_cols_str}\nFROM {patch_table_name}\nWHERE {none_null}\nON CONFLICT ({join_cols_str}) DO {update_or_nothing} {sets_excluded}"
        ).trim_end().to_string());
        // The rows with a NULL key: update those that exist, then insert the others.
        if !value_cols.is_empty() {
            queries.push(format!(
                "UPDATE {target_table_name} AS f\n{sets_none}\nFROM (SELECT {patch_cols_str} FROM {patch_table_name} WHERE {}) AS p\nWHERE {keys_match}",
                any_null("")
            ));
        }
        queries.push(format!(
            "INSERT INTO {target_table_name} ({patch_cols_str})\nSELECT {patch_cols_prefixed_str}\nFROM {patch_table_name} AS p\nWHERE ({})\n    AND NOT EXISTS (\n        SELECT 1\n        FROM {target_table_name} AS f\n        WHERE {keys_match}\n    )",
            any_null("p.")
        ));
        return queries;
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
}
