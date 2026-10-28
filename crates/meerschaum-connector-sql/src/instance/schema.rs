//! `verify schema` on a SQL instance (`InstanceConnector::get_pipe_schema_plan` /
//! `apply_pipe_schema`): read the table's columns and facts from the catalog, plan with
//! [`crate::queries::schema`], and apply a confirmed plan in one transaction where the database
//! allows it.

use indexmap::{IndexMap, IndexSet};
use meerschaum_connector::schema::{SchemaChangeKind, SchemaPlan};
use meerschaum_connector::{ConnectorResult, pipe_ops};
use meerschaum_core::{Pipe, SuccessTuple, Value};

use crate::dtypes::{
    PD_TO_DB_DTYPES_FLAVORS, alias_dtype, are_dtypes_equal, get_db_type_from_pd_type,
};
use crate::flavor::{DriverFamily, Flavor};
use crate::pipe::SqlPipe;
use crate::queries::schema::{
    ColumnInfo, ColumnRow, PlanInput, TableFacts, build_schema_plan, columns_from_rows,
    get_partition_keys_queries, get_schema_columns_query, partition_keys_from_rows,
};

use super::{SqlConnector, be, frame_docs, kv_int, kv_str};

/// Whether `get_db_type_from_pd_type` knows `dtype` (an unknown dtype falls back to `TEXT`
/// there, which a plan must not turn into a type change).
fn known_dtype(dtype: &str) -> bool {
    let resolved = alias_dtype(dtype).unwrap_or(dtype);
    if ["numeric[", "geometry[", "geography["]
        .iter()
        .any(|p| resolved.starts_with(p))
    {
        return true;
    }
    PD_TO_DB_DTYPES_FLAVORS
        .iter()
        .any(|(k, _)| *k == resolved || are_dtypes_equal(k, resolved))
}

/// The `mrsm` command for a pipe-scoped action (`decompress pipes -c ... -m ... -i ...`).
fn pipe_command(action: &str, pipe: &Pipe) -> String {
    let mut parts = vec![
        format!("mrsm {action}"),
        "-c".into(),
        pipe.connector_keys().to_string(),
        "-m".into(),
        pipe.metric_key().to_string(),
    ];
    if let Some(lk) = pipe.location_key() {
        parts.push("-l".into());
        parts.push(lk.to_string());
    }
    parts.push("-i".into());
    parts.push(pipe.instance_keys().to_string());
    parts.join(" ")
}

/// The columns a pipe declares: `dtypes` (a key naming a `columns` role resolves to the role's
/// column when the table has no column of that name), plus the `datetime` role and an
/// autoincrementing `primary` role when the table lacks them. Returns `{column: (dtype, database
/// type)}` and the notes about declarations the plan leaves alone.
fn declared_columns(
    flavor: Flavor,
    sp: &SqlPipe,
    columns: &[ColumnInfo],
) -> (IndexMap<String, (String, String)>, Vec<String>) {
    let has = |name: &str| {
        columns.iter().any(|c| {
            c.name == name
                || (flavor.driver_family() != DriverFamily::Postgres
                    && c.name.eq_ignore_ascii_case(name))
        })
    };
    let roles: IndexMap<String, String> = sp
        .pipe()
        .columns()
        .iter()
        .filter_map(|(role, v)| match v {
            Value::Str(s) if !s.is_empty() => Some((role.clone(), s.clone())),
            _ => None,
        })
        .collect();
    let mut notes = Vec::new();
    let mut out: IndexMap<String, (String, String)> = IndexMap::new();
    for (key, dtype) in pipe_ops::configured_dtypes(sp.pipe().parameters()) {
        // A null dtype (`dtypes: {col: null}`) declares nothing.
        if key.is_empty() || dtype.is_empty() || dtype == "None" {
            continue;
        }
        let column = match roles.get(&key) {
            Some(role_column) if !has(&key) => role_column.clone(),
            _ => key.clone(),
        };
        if !known_dtype(&dtype) {
            notes.push(format!(
                "Column '{column}' declares the unknown dtype '{dtype}': it is left alone."
            ));
            continue;
        }
        let db_type = get_db_type_from_pd_type(&dtype, Some(flavor));
        out.insert(column, (dtype, db_type));
    }
    for (role, column) in &roles {
        if out.contains_key(column) || has(column) {
            continue;
        }
        let dtype = match role.as_str() {
            "datetime" => "datetime",
            "primary" if sp.autoincrement() => "int",
            _ => {
                notes.push(format!(
                    "Column '{column}' ({role}) is missing: declare its dtype under `dtypes` \
                     to add it."
                ));
                continue;
            }
        };
        out.insert(
            column.clone(),
            (
                dtype.to_string(),
                get_db_type_from_pd_type(dtype, Some(flavor)),
            ),
        );
    }
    (out, notes)
}

impl SqlConnector {
    /// The table's columns as [`ColumnInfo`]s (full types, nullability, SQLite keys).
    async fn schema_columns(
        &self,
        sp: &SqlPipe,
        schema: Option<&str>,
    ) -> ConnectorResult<Vec<ColumnInfo>> {
        let query = get_schema_columns_query(self.flavor, &sp.target, schema);
        let df = self.read_catalog(&query).await.map_err(be)?;
        let rows: Vec<ColumnRow> = frame_docs(&df)
            .iter()
            .filter_map(|d| {
                Some(ColumnRow {
                    column: kv_str(d.get("column"))?,
                    type_: kv_str(d.get("type")).unwrap_or_default(),
                    udt: kv_str(d.get("udt")),
                    length: kv_int(d.get("length")),
                    precision: kv_int(d.get("precision")),
                    scale: kv_int(d.get("scale")),
                    nullable: kv_str(d.get("nullable")),
                    extra: kv_str(d.get("extra")),
                    pk: kv_int(d.get("pk")),
                    notnull: kv_int(d.get("notnull")),
                    default: kv_str(d.get("default")),
                })
            })
            .collect();
        let mut geometry = IndexMap::new();
        let spatial = rows.iter().any(|r| {
            r.type_.eq_ignore_ascii_case("USER-DEFINED")
                && r.udt
                    .as_deref()
                    .is_some_and(|u| matches!(u, "geometry" | "geography"))
        });
        if spatial
            && self.flavor.driver_family() == DriverFamily::Postgres
            && let Ok(q) =
                crate::queries::table::get_postgis_geo_columns_types_query(&sp.target, schema)
            && let Ok(gdf) = self.read(&q).await
        {
            let geo_rows: Vec<(String, String, String, Option<i64>)> = frame_docs(&gdf)
                .iter()
                .filter_map(|d| {
                    Some((
                        kv_str(d.get("column"))?,
                        kv_str(d.get("func"))?,
                        kv_str(d.get("type")).unwrap_or_default(),
                        kv_int(d.get("srid")),
                    ))
                })
                .collect();
            geometry = crate::queries::table::postgis_geo_columns_types_from_rows(&geo_rows);
        }
        Ok(columns_from_rows(self.flavor, &rows, &geometry))
    }

    /// What the planner needs to know about the table besides its columns.
    async fn schema_facts(&self, sp: &SqlPipe, schema: Option<&str>) -> TableFacts {
        let mut facts = TableFacts {
            decompress_command: Some(pipe_command("decompress pipes", sp.pipe())),
            drop_indices_command: Some(pipe_command("drop indices", sp.pipe())),
            ..Default::default()
        };
        let esc = |s: &str| s.replace('\'', "''");
        // The partition keys (best effort, like every fact: a key the catalog does not report
        // fails at apply time instead, with the database's message).
        let mut partition_rows: Vec<(String, String)> = Vec::new();
        for query in get_partition_keys_queries(self.flavor, &sp.target, schema) {
            if let Ok(df) = self.read(&query).await {
                partition_rows.extend(frame_docs(&df).iter().filter_map(|d| {
                    Some((
                        kv_str(d.get("column"))?,
                        kv_str(d.get("kind")).unwrap_or_default(),
                    ))
                }));
            }
        }
        facts.partitioned_by = partition_keys_from_rows(self.flavor, &partition_rows);
        if self.flavor.is_timescaledb() {
            let schema = schema.filter(|s| !s.is_empty()).unwrap_or("public");
            // Column types cannot change while any chunk is compressed (TimescaleDB: "operation
            // not supported on hypertables with compressed chunks"); an enabled columnstore
            // without compressed chunks is fine.
            let query = format!(
                "SELECT COUNT(*) FROM timescaledb_information.chunks\n\
                 WHERE hypertable_schema = '{}' AND hypertable_name = '{}' AND is_compressed",
                esc(schema),
                esc(&sp.target)
            );
            facts.compressed_hypertable = self
                .value(&query)
                .await
                .and_then(|v| v.as_i64())
                .is_some_and(|n| n > 0);
        }
        if matches!(
            self.flavor.driver_family(),
            DriverFamily::Mysql | DriverFamily::Mssql | DriverFamily::Oracle
        ) {
            facts.indexed = self
                .get_table_cols_indices(&sp.target, schema)
                .await
                .into_iter()
                .filter(|(_, docs)| !docs.is_empty())
                .map(|(c, _)| c)
                .collect::<IndexSet<String>>();
        }
        if self.flavor == Flavor::Sqlite {
            let t = esc(&sp.target);
            if let Ok(df) = self
                .read(&format!(
                    "SELECT sql FROM sqlite_master WHERE type = 'index' AND tbl_name = '{t}' \
                     AND sql IS NOT NULL"
                ))
                .await
            {
                facts.sqlite_indexes = frame_docs(&df)
                    .iter()
                    .filter_map(|d| kv_str(d.get("sql")))
                    .collect();
            }
            facts.sqlite_autoincrement = self
                .value(&format!(
                    "SELECT sql FROM sqlite_master WHERE type = 'table' AND name = '{t}'"
                ))
                .await
                .is_some_and(|v| v.to_string().to_uppercase().contains("AUTOINCREMENT"));
        }
        facts
    }

    /// `verify schema`: how the pipe's table differs from its declared dtypes (nothing is
    /// written). A missing table has an empty plan (`exists: false`).
    pub async fn get_pipe_schema_plan(&self, pipe: &Pipe) -> ConnectorResult<SchemaPlan> {
        let sp = self.sql_pipe(pipe)?;
        let schema = self.get_pipe_schema(&sp);
        let table = self.pipe_table_name(&sp);
        if !self
            .table_exists_checked(&sp.target, schema.as_deref())
            .await?
        {
            return Ok(SchemaPlan {
                table,
                exists: false,
                notes: vec![format!(
                    "{pipe} has no table yet: its first sync creates it with the declared dtypes."
                )],
                ..Default::default()
            });
        }
        let columns = self.schema_columns(&sp, schema.as_deref()).await?;
        let (declared, notes) = declared_columns(self.flavor, &sp, &columns);
        let facts = if declared.is_empty() {
            TableFacts::default()
        } else {
            self.schema_facts(&sp, schema.as_deref()).await
        };
        Ok(build_schema_plan(&PlanInput {
            flavor: self.flavor,
            table: &table,
            target: &sp.target,
            columns: &columns,
            declared: &declared,
            facts: &facts,
            notes,
        }))
    }

    /// Run a plan's statements: one transaction where the database allows it (PostgreSQL,
    /// SQLite, MSSQL, DuckDB roll back on failure; MySQL and Oracle commit each DDL statement).
    async fn run_schema_statements(&self, queries: &[String]) -> Result<(), String> {
        let mut tx = self
            .driver()
            .begin_write()
            .await
            .map_err(|e| e.to_string())?;
        for q in queries {
            self.log(q);
            if let Err(e) = tx.exec(q).await {
                let _ = tx.rollback().await;
                return Err(format!("{e}\n    while running:\n{q}"));
            }
        }
        tx.commit().await.map_err(|e| e.to_string())
    }

    /// Apply a plan from [`get_pipe_schema_plan`](Self::get_pipe_schema_plan): every change,
    /// or with `safe_only` only the safe ones. Plans again first and refuses when the table no
    /// longer matches `plan`. Refused on a protected instance unless writes are allowed.
    pub async fn apply_pipe_schema(
        &self,
        pipe: &Pipe,
        plan: &SchemaPlan,
        safe_only: bool,
    ) -> SuccessTuple {
        if let Some(refused) = self.protected_refusal(pipe, "change the table of") {
            return refused;
        }
        // A table schema change is `drop`-class: a `drop` lock refuses it.
        if let Some(refused) = self
            .lock_refusal(pipe, meerschaum_core::access::LockOp::Schema)
            .await
        {
            return refused;
        }
        let fresh = match self.get_pipe_schema_plan(pipe).await {
            Ok(p) => p,
            Err(e) => return SuccessTuple::fail(e.to_string()),
        };
        if !fresh.same_changes(plan) {
            return SuccessTuple::fail(format!(
                "The table of {pipe} changed since it was checked; run `verify schema` again."
            ));
        }
        if fresh.is_empty() {
            return SuccessTuple::ok(format!("The table of {pipe} already matches its dtypes."));
        }
        let queries = if safe_only {
            &fresh.safe_queries
        } else {
            &fresh.queries
        };
        let blocked = fresh.blocked_changes();
        let applied: Vec<_> = fresh
            .changes
            .iter()
            .filter(|c| c.is_applicable() && (!safe_only || c.is_safe()))
            .collect();
        if !queries.is_empty()
            && let Err(e) = self.run_schema_statements(queries).await
        {
            let atomic = !matches!(
                self.flavor.driver_family(),
                DriverFamily::Mysql | DriverFamily::Oracle
            );
            return SuccessTuple::fail(format!(
                "Failed to change the table of {pipe}{}:\n{e}",
                if atomic { " (nothing was changed)" } else { "" }
            ));
        }
        let mut lines: Vec<String> = Vec::new();
        if !queries.is_empty() {
            lines.push(format!(
                "Updated the table of {pipe} ({} change{}):",
                applied.len(),
                if applied.len() == 1 { "" } else { "s" }
            ));
            for c in &applied {
                lines.push(match c.kind {
                    SchemaChangeKind::AddColumn => {
                        format!("  - added column '{}' ({})", c.column, c.target)
                    }
                    _ => format!(
                        "  - {} '{}': {} -> {}",
                        if c.kind == SchemaChangeKind::Widen {
                            "widened"
                        } else {
                            "changed"
                        },
                        c.column,
                        c.current.as_deref().unwrap_or("?"),
                        c.target
                    ),
                });
            }
        }
        let skipped = if safe_only {
            fresh
                .changes
                .iter()
                .filter(|c| !c.is_safe() && c.is_applicable())
                .count()
        } else {
            0
        };
        if skipped > 0 {
            lines.push(format!(
                "Skipped {skipped} change{} that need{} confirmation.",
                if skipped == 1 { "" } else { "s" },
                if skipped == 1 { "s" } else { "" }
            ));
        }
        if let Some(reason) = fresh.blocked.as_ref().filter(|_| !blocked.is_empty()) {
            lines.push(reason.clone());
            return SuccessTuple::fail(lines.join("\n"));
        }
        SuccessTuple::ok(lines.join("\n"))
    }
}
