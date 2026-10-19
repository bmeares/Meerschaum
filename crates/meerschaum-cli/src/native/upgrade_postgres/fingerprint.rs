//! What the upgrade compares between the old and the new database: per database, its encoding
//! and locale, the extensions, the user relations (tables, views, indexes, sequences, ...), exact
//! row counts of every user table, sequence positions, user functions and triggers, large
//! objects, and with TimescaleDB its hypertables, dimensions, compression settings, chunks,
//! policies and continuous aggregates.
//!
//! "User" excludes the system schemas, TimescaleDB's internal schemas (the chunks are counted
//! through their hypertables) and every object an extension owns (PostGIS's `spatial_ref_sys`,
//! which a newer PostGIS fills differently).

use std::collections::BTreeMap;

use serde_json::{Map, Value as Json};

use super::sqltext::{quote_ident, quote_literal};

/// The extension-independent part of a database's fingerprint (one JSON object).
pub const BASE_QUERY: &str = r#"
WITH ns AS (
  SELECT oid, nspname FROM pg_catalog.pg_namespace
  WHERE nspname NOT IN ('pg_catalog', 'information_schema', 'timescaledb_information',
                        'timescaledb_experimental', 'toolkit_experimental')
    AND nspname !~ '^pg_toast' AND nspname !~ '^pg_temp_' AND nspname !~ '^_timescaledb_'
),
members AS (
  SELECT classid, objid FROM pg_catalog.pg_depend WHERE deptype = 'e'
),
rels AS (
  SELECT c.oid, ns.nspname, c.relname, c.relkind, c.relispartition
  FROM pg_catalog.pg_class c JOIN ns ON ns.oid = c.relnamespace
  WHERE c.relkind IN ('r', 'p', 'v', 'm', 'S', 'f', 'i', 'I', 'c')
    AND NOT EXISTS (
      SELECT 1 FROM members m
      WHERE m.classid = 'pg_catalog.pg_class'::pg_catalog.regclass AND m.objid = c.oid
    )
)
SELECT json_build_object(
  'database', (
    SELECT json_build_object(
      'encoding', pg_catalog.pg_encoding_to_char(d.encoding),
      'collate', d.datcollate, 'ctype', d.datctype,
      'provider', coalesce(to_jsonb(d) ->> 'datlocprovider', 'c'),
      'locale', coalesce(to_jsonb(d) ->> 'datlocale', to_jsonb(d) ->> 'daticulocale'),
      'icu_rules', to_jsonb(d) ->> 'daticurules')
    FROM pg_catalog.pg_database d WHERE d.datname = pg_catalog.current_database()
  ),
  'extensions', (
    SELECT coalesce(json_agg(json_build_object(
      'name', e.extname, 'version', e.extversion, 'schema', n.nspname) ORDER BY e.extname), '[]')
    FROM pg_catalog.pg_extension e JOIN pg_catalog.pg_namespace n ON n.oid = e.extnamespace
  ),
  'relations', (
    SELECT coalesce(json_agg(r.nspname || '.' || r.relname || ' (' || r.relkind::text || ')'
      ORDER BY r.nspname, r.relname), '[]')
    FROM rels r
  ),
  'tables', (
    SELECT coalesce(json_agg(json_build_array(r.nspname, r.relname)
      ORDER BY r.nspname, r.relname), '[]')
    FROM rels r WHERE r.relkind IN ('r', 'p') AND NOT r.relispartition
  ),
  'sequences', (
    SELECT coalesce(json_object_agg(s.schemaname || '.' || s.sequencename, s.last_value
      ORDER BY s.schemaname, s.sequencename), '{}')
    FROM pg_catalog.pg_sequences s
    JOIN rels r ON r.nspname = s.schemaname AND r.relname = s.sequencename AND r.relkind = 'S'
  ),
  'functions', (
    SELECT coalesce(json_agg(f.name ORDER BY f.name), '[]')
    FROM (
      SELECT ns.nspname || '.' || p.proname || '('
        || pg_catalog.pg_get_function_identity_arguments(p.oid) || ')' AS name
      FROM pg_catalog.pg_proc p JOIN ns ON ns.oid = p.pronamespace
      WHERE NOT EXISTS (
        SELECT 1 FROM members m
        WHERE m.classid = 'pg_catalog.pg_proc'::pg_catalog.regclass AND m.objid = p.oid
      )
    ) f
  ),
  'triggers', (
    SELECT coalesce(json_agg(r.nspname || '.' || r.relname || ': ' || t.tgname
      ORDER BY r.nspname, r.relname, t.tgname), '[]')
    FROM pg_catalog.pg_trigger t JOIN rels r ON r.oid = t.tgrelid
    WHERE NOT t.tgisinternal
  ),
  'large_objects', (SELECT count(*) FROM pg_catalog.pg_largeobject_metadata),
  'size', pg_catalog.pg_database_size(pg_catalog.current_database())
);
"#;

/// TimescaleDB's own view of the database (only where the extension is installed). Every view
/// and column used here exists in every TimescaleDB 2.x.
pub const TIMESCALE_QUERY: &str = r#"
SELECT json_build_object(
  'hypertables', (
    SELECT coalesce(json_agg(json_build_object(
      'hypertable', h.hypertable_schema || '.' || h.hypertable_name,
      'dimensions', h.num_dimensions, 'compression', h.compression_enabled)
      ORDER BY h.hypertable_schema, h.hypertable_name), '[]')
    FROM timescaledb_information.hypertables h
  ),
  'dimensions', (
    SELECT coalesce(json_agg(json_build_object(
      'hypertable', d.hypertable_schema || '.' || d.hypertable_name,
      'number', d.dimension_number, 'column', d.column_name, 'type', d.dimension_type,
      'time_interval', d.time_interval::text, 'integer_interval', d.integer_interval,
      'partitions', d.num_partitions)
      ORDER BY d.hypertable_schema, d.hypertable_name, d.dimension_number), '[]')
    FROM timescaledb_information.dimensions d
  ),
  'compression_settings', (
    SELECT coalesce(json_agg(json_build_object(
      'hypertable', c.hypertable_schema || '.' || c.hypertable_name, 'column', c.attname,
      'segmentby', c.segmentby_column_index, 'orderby', c.orderby_column_index,
      'asc', c.orderby_asc, 'nulls_first', c.orderby_nullsfirst)
      ORDER BY c.hypertable_schema, c.hypertable_name, c.attname), '[]')
    FROM timescaledb_information.compression_settings c
  ),
  'chunks', (
    SELECT coalesce(json_agg(json_build_object(
      'hypertable', c.hypertable_schema || '.' || c.hypertable_name,
      'chunk', c.chunk_schema || '.' || c.chunk_name, 'compressed', c.is_compressed)
      ORDER BY c.hypertable_schema, c.hypertable_name, c.chunk_schema, c.chunk_name), '[]')
    FROM timescaledb_information.chunks c
  ),
  'jobs', (
    SELECT coalesce(json_agg(json_build_object(
      'procedure', j.proc_schema || '.' || j.proc_name,
      'hypertable', j.hypertable_schema || '.' || j.hypertable_name,
      'schedule', j.schedule_interval::text, 'config', j.config, 'scheduled', j.scheduled)
      ORDER BY j.proc_schema, j.proc_name, j.hypertable_schema, j.hypertable_name, j.job_id),
      '[]')
    FROM timescaledb_information.jobs j WHERE j.job_id >= 1000
  ),
  'continuous_aggregates', (
    SELECT coalesce(json_agg(json_build_object(
      'view', a.view_schema || '.' || a.view_name, 'materialized_only', a.materialized_only)
      ORDER BY a.view_schema, a.view_name), '[]')
    FROM timescaledb_information.continuous_aggregates a
  )
);
"#;

/// The planner's estimate of a database's rows (`pg_class.reltuples` of every table, chunks
/// included; never-analyzed tables count 0): what an online backup records instead of exact
/// counts. Compressed TimescaleDB chunks hold batches, so it undercounts them.
pub const ESTIMATE_QUERY: &str = r#"
SELECT json_build_object('row_estimate', coalesce(sum(greatest(c.reltuples, 0)), 0)::bigint)
FROM pg_catalog.pg_class c JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace
WHERE c.relkind IN ('r', 'p') AND NOT c.relispartition
  AND n.nspname NOT IN ('pg_catalog', 'information_schema') AND n.nspname !~ '^pg_toast';
"#;

/// What writes leave behind in a database, read before its dump's snapshot and again before the
/// old server stops: the tuple counters (inserts, updates, deletes) of every user table, and the
/// tables and their files (`TRUNCATE` changes a table's file, `CREATE` / `DROP` the list).
pub const ACTIVITY_QUERY: &str = r#"
SELECT json_build_object(
  'tuples', coalesce(sum(s.n_tup_ins + s.n_tup_upd + s.n_tup_del), 0)::bigint,
  'tables', count(*),
  'files', coalesce(md5(string_agg(s.relid::text || ':'
    || coalesce(pg_catalog.pg_relation_filenode(s.relid)::text, '-'), ',' ORDER BY s.relid)), '')
)
FROM pg_catalog.pg_stat_user_tables s;
"#;

/// A database's encoding and locale (`pg_database`), as the fingerprint records it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DbLocale {
    pub encoding: String,
    pub collate: String,
    pub ctype: String,
    /// `c` (libc), `i` (ICU) or `b` (builtin); `c` before PostgreSQL 15.
    pub provider: String,
    /// The ICU or builtin locale.
    pub locale: Option<String>,
    pub icu_rules: Option<String>,
}

impl DbLocale {
    pub fn from_json(value: &Json) -> Option<DbLocale> {
        let text = |k: &str| value.get(k).and_then(Json::as_str).map(str::to_string);
        Some(DbLocale {
            encoding: text("encoding")?,
            collate: text("collate")?,
            ctype: text("ctype")?,
            provider: text("provider").unwrap_or_else(|| "c".into()),
            locale: text("locale").filter(|l| !l.is_empty()),
            icu_rules: text("icu_rules").filter(|r| !r.is_empty()),
        })
    }

    /// Whether two databases sort and classify text the same way. An ICU locale is compared in
    /// its canonical spelling (a newer server may spell it as a language tag).
    pub fn same_as(&self, other: &DbLocale) -> bool {
        let icu = |l: &Option<String>| l.as_ref().map(|l| l.to_ascii_lowercase().replace('_', "-"));
        self.encoding.eq_ignore_ascii_case(&other.encoding)
            && self.collate == other.collate
            && self.ctype == other.ctype
            && self.provider == other.provider
            && if self.provider == "i" {
                icu(&self.locale) == icu(&other.locale)
            } else {
                self.locale == other.locale
            }
            && self.icu_rules == other.icu_rules
    }

    /// `en_US.utf8`, `icu en-US`, ...
    pub fn describe(&self) -> String {
        match (self.provider.as_str(), &self.locale) {
            ("i", Some(l)) => format!("ICU '{l}' ({})", self.encoding),
            ("b", Some(l)) => format!("builtin '{l}' ({})", self.encoding),
            _ if self.collate == self.ctype => format!("'{}' ({})", self.collate, self.encoding),
            _ => format!(
                "collate '{}', ctype '{}' ({})",
                self.collate, self.ctype, self.encoding
            ),
        }
    }
}

/// The encoding and locale a fingerprint recorded (`None` in a fingerprint without them).
pub fn database_locale(fingerprint: &Json) -> Option<DbLocale> {
    DbLocale::from_json(fingerprint.get("database")?)
}

/// How many tables one counting query visits.
const COUNT_BATCH: usize = 200;

/// The exact row-count queries for the `tables` of a base fingerprint: each returns one JSON
/// object `{"schema.table": count}`.
pub fn count_queries(base: &Json) -> Vec<String> {
    let tables: Vec<(String, String)> = base
        .get("tables")
        .and_then(Json::as_array)
        .map(|a| {
            a.iter()
                .filter_map(|t| {
                    let pair = t.as_array()?;
                    Some((
                        pair.first()?.as_str()?.to_string(),
                        pair.get(1)?.as_str()?.to_string(),
                    ))
                })
                .collect()
        })
        .unwrap_or_default();
    tables
        .chunks(COUNT_BATCH)
        .map(|batch| {
            let rows: Vec<String> = batch
                .iter()
                .map(|(schema, table)| {
                    format!(
                        "({}, (SELECT count(*) FROM {}.{}))",
                        quote_literal(&format!("{schema}.{table}")),
                        quote_ident(schema),
                        quote_ident(table)
                    )
                })
                .collect();
            format!(
                "SELECT coalesce(json_object_agg(t.k, t.v ORDER BY t.k), '{{}}') FROM (VALUES {}) \
                 AS t(k, v);",
                rows.join(",\n")
            )
        })
        .collect()
}

/// Whether the fingerprint's database has TimescaleDB, and its version.
pub fn timescaledb_version(fingerprint: &Json) -> Option<String> {
    extension(fingerprint, "timescaledb").map(|(version, _)| version)
}

/// `(version, schema)` of an installed extension.
pub fn extension(fingerprint: &Json, name: &str) -> Option<(String, String)> {
    fingerprint
        .get("extensions")?
        .as_array()?
        .iter()
        .find(|e| e.get("name").and_then(Json::as_str) == Some(name))
        .map(|e| {
            (
                e.get("version")
                    .and_then(Json::as_str)
                    .unwrap_or_default()
                    .to_string(),
                e.get("schema")
                    .and_then(Json::as_str)
                    .unwrap_or_default()
                    .to_string(),
            )
        })
}

/// The installed extensions as `name -> (version, schema)`.
pub fn extensions(fingerprint: &Json) -> BTreeMap<String, (String, String)> {
    fingerprint
        .get("extensions")
        .and_then(Json::as_array)
        .map(|a| {
            a.iter()
                .filter_map(|e| {
                    Some((
                        e.get("name")?.as_str()?.to_string(),
                        (
                            e.get("version")?.as_str()?.to_string(),
                            e.get("schema")?.as_str()?.to_string(),
                        ),
                    ))
                })
                .collect()
        })
        .unwrap_or_default()
}

/// The total of the exact row counts.
pub fn total_rows(fingerprint: &Json) -> i64 {
    fingerprint
        .get("rowcounts")
        .and_then(Json::as_object)
        .map(|m| m.values().filter_map(Json::as_i64).sum())
        .unwrap_or(0)
}

/// Which parts of two fingerprints must be equal.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Check {
    /// Right after the restore, at the dump's TimescaleDB version: everything, with the
    /// extensions compared by name and schema and TimescaleDB's version as well.
    Restored,
    /// After the extensions were updated to the new server's versions: the structure (no row
    /// counts, no policy configurations or compression views whose layout an update may change).
    Updated,
    /// The old database in its dump's snapshot and right before it stops: everything but the row
    /// counts (its tuple counters stand in for them), the extensions exactly.
    Unchanged,
    /// A restore from an online backup: like [`Check::Restored`] without the row counts (an
    /// online backup records estimates) and the sequences' positions (sequences are not
    /// transactional, so the dump's snapshot does not pin them).
    RestoredFromBackup,
}

const RESTORED_KEYS: &[&str] = &[
    "relations",
    "rowcounts",
    "sequences",
    "functions",
    "triggers",
    "large_objects",
    "hypertables",
    "dimensions",
    "compression_settings",
    "chunks",
    "jobs",
    "continuous_aggregates",
];

const UPDATED_KEYS: &[&str] = &[
    "relations",
    "sequences",
    "functions",
    "large_objects",
    "hypertables",
    "dimensions",
    "chunks",
    "continuous_aggregates",
];

/// How many differences are listed per part.
const MAX_LISTED: usize = 8;

fn canonical(value: &Json) -> String {
    serde_json::to_string(value).unwrap_or_default()
}

fn diff_values(key: &str, old: &Json, new: &Json, out: &mut Vec<String>) {
    match (old, new) {
        (Json::Object(a), Json::Object(b)) => {
            let mut lines = Vec::new();
            for (k, va) in a {
                match b.get(k) {
                    None => lines.push(format!("{k}: {} -> (missing)", canonical(va))),
                    Some(vb) if vb != va => {
                        lines.push(format!("{k}: {} -> {}", canonical(va), canonical(vb)))
                    }
                    _ => {}
                }
            }
            for (k, vb) in b {
                if !a.contains_key(k) {
                    lines.push(format!("{k}: (missing) -> {}", canonical(vb)));
                }
            }
            push_lines(key, lines, out);
        }
        (Json::Array(a), Json::Array(b)) => {
            let mut left: Vec<String> = a.iter().map(canonical).collect();
            let mut extra = Vec::new();
            for item in b.iter().map(canonical) {
                match left.iter().position(|x| *x == item) {
                    Some(i) => {
                        left.remove(i);
                    }
                    None => extra.push(item),
                }
            }
            let mut lines: Vec<String> = left
                .into_iter()
                .map(|x| format!("only in the old database: {x}"))
                .collect();
            lines.extend(
                extra
                    .into_iter()
                    .map(|x| format!("only in the new database: {x}")),
            );
            if lines.is_empty() && a != b {
                lines.push("same entries in another order".into());
            }
            push_lines(key, lines, out);
        }
        _ if old != new => out.push(format!("{key}: {} -> {}", canonical(old), canonical(new))),
        _ => {}
    }
}

fn push_lines(key: &str, lines: Vec<String>, out: &mut Vec<String>) {
    let total = lines.len();
    for line in lines.into_iter().take(MAX_LISTED) {
        out.push(format!("{key}: {line}"));
    }
    if total > MAX_LISTED {
        out.push(format!("{key}: ... and {} more", total - MAX_LISTED));
    }
}

/// The differences between two fingerprints of one database (empty when they match).
pub fn differences(old: &Json, new: &Json, check: Check) -> Vec<String> {
    let mut out = Vec::new();
    let null = Json::Null;
    let keys: Vec<&str> = match check {
        Check::Restored => RESTORED_KEYS.to_vec(),
        Check::RestoredFromBackup => RESTORED_KEYS
            .iter()
            .copied()
            .filter(|k| *k != "rowcounts" && *k != "sequences")
            .collect(),
        Check::Updated => UPDATED_KEYS.to_vec(),
        Check::Unchanged => {
            let mut keys: Vec<&str> = RESTORED_KEYS
                .iter()
                .copied()
                .filter(|k| *k != "rowcounts")
                .collect();
            keys.push("extensions");
            keys
        }
    };
    for key in keys {
        let (a, b) = (old.get(key).unwrap_or(&null), new.get(key).unwrap_or(&null));
        diff_values(key, a, b, &mut out);
    }
    // The encoding and locale (a fingerprint without them is not compared on them).
    if let (Some(a), Some(b)) = (database_locale(old), database_locale(new))
        && !a.same_as(&b)
    {
        out.push(format!(
            "database: encoding and locale {} -> {}",
            a.describe(),
            b.describe()
        ));
    }
    if check != Check::Unchanged {
        // Extensions: the same set, in the same schemas; TimescaleDB at the same version until
        // the update (other extensions are created at the new server's default version).
        let (a, b) = (extensions(old), extensions(new));
        let names = |m: &BTreeMap<String, (String, String)>| -> Json {
            Json::Object(
                m.iter()
                    .map(|(n, (_, s))| (n.clone(), Json::String(s.clone())))
                    .collect::<Map<String, Json>>(),
            )
        };
        diff_values("extensions", &names(&a), &names(&b), &mut out);
        if matches!(check, Check::Restored | Check::RestoredFromBackup)
            && let (Some((va, _)), Some((vb, _))) = (a.get("timescaledb"), b.get("timescaledb"))
            && va != vb
        {
            out.push(format!("extensions: timescaledb version {va} -> {vb}"));
        }
    }
    out
}

/// Merge the row counts into a base fingerprint, and the TimescaleDB part when there is one.
pub fn assemble(mut base: Json, counts: Vec<Json>, timescale: Option<Json>) -> Json {
    let mut rowcounts = Map::new();
    for part in counts {
        if let Json::Object(m) = part {
            rowcounts.extend(m);
        }
    }
    if let Json::Object(m) = &mut base {
        m.remove("tables");
        m.insert("rowcounts".into(), Json::Object(rowcounts));
        if let Some(Json::Object(ts)) = timescale {
            m.extend(ts);
        }
    }
    base
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn sample() -> Json {
        json!({
            "extensions": [
                {"name": "plpgsql", "version": "1.0", "schema": "pg_catalog"},
                {"name": "postgis", "version": "3.5.2", "schema": "public"},
                {"name": "timescaledb", "version": "2.29.2", "schema": "public"}
            ],
            "relations": ["public.metrics (r)", "public.metrics_pkey (i)"],
            "rowcounts": {"public.metrics": 28801, "_mrsm_internal.runs": 3},
            "sequences": {"public.plain_id_seq": 1000},
            "functions": [],
            "triggers": ["public.metrics: ts_insert_blocker"],
            "large_objects": 0,
            "size": 123456,
            "hypertables": [{"hypertable": "public.metrics", "dimensions": 1, "compression": false}],
            "dimensions": [],
            "compression_settings": [],
            "chunks": [{"hypertable": "public.metrics", "chunk": "_timescaledb_internal._hyper_1_1_chunk", "compressed": false}],
            "jobs": [{"procedure": "_timescaledb_functions.policy_compression", "config": {"compress_after": "30 days"}}],
            "continuous_aggregates": []
        })
    }

    #[test]
    fn identical_fingerprints_have_no_differences() {
        let a = sample();
        for check in [Check::Restored, Check::Updated, Check::Unchanged] {
            assert!(differences(&a, &a, check).is_empty(), "{check:?}");
        }
    }

    #[test]
    fn a_changed_row_count_is_reported() {
        let a = sample();
        let mut b = sample();
        b["rowcounts"]["public.metrics"] = json!(28800);
        let diffs = differences(&a, &b, Check::Restored);
        assert_eq!(diffs, vec!["rowcounts: public.metrics: 28801 -> 28800"]);
        // Row counts are not compared after the extension update, nor on the old server before
        // it stops (its tuple counters stand in for them there).
        assert!(differences(&a, &b, Check::Updated).is_empty());
        assert!(differences(&a, &b, Check::Unchanged).is_empty());
        // A structural change of the old database is.
        b["relations"] = json!(["public.metrics (r)"]);
        assert_eq!(differences(&a, &b, Check::Unchanged).len(), 1);
    }

    #[test]
    fn the_encoding_and_locale_are_compared() {
        let mut a = sample();
        a["database"] = json!({"encoding": "UTF8", "collate": "en_US.utf8", "ctype": "en_US.utf8",
                               "provider": "c", "locale": null, "icu_rules": null});
        let mut b = a.clone();
        assert!(differences(&a, &b, Check::Restored).is_empty());
        b["database"]["collate"] = json!("C.UTF-8");
        b["database"]["ctype"] = json!("C.UTF-8");
        assert_eq!(
            differences(&a, &b, Check::Restored),
            vec!["database: encoding and locale 'en_US.utf8' (UTF8) -> 'C.UTF-8' (UTF8)"]
        );
        // Older fingerprints without them compare as before.
        let c = sample();
        assert!(differences(&a, &c, Check::RestoredFromBackup).is_empty());
        // ICU locales in another spelling are the same locale.
        let icu = |l: &str| DbLocale {
            encoding: "UTF8".into(),
            collate: "C".into(),
            ctype: "C".into(),
            provider: "i".into(),
            locale: Some(l.into()),
            icu_rules: None,
        };
        assert!(icu("en_US").same_as(&icu("en-US")));
        assert!(!icu("en-US").same_as(&icu("de-DE")));
    }

    #[test]
    fn extension_versions_matter_only_for_timescaledb_until_the_update() {
        let a = sample();
        let mut b = sample();
        b["extensions"][1]["version"] = json!("3.6.4");
        assert!(differences(&a, &b, Check::Restored).is_empty());
        b["extensions"][2]["version"] = json!("2.30.2");
        assert_eq!(
            differences(&a, &b, Check::Restored),
            vec!["extensions: timescaledb version 2.29.2 -> 2.30.2"]
        );
        assert!(differences(&a, &b, Check::Updated).is_empty());
        // The old database's extensions must not change at all.
        assert!(!differences(&a, &b, Check::Unchanged).is_empty());
        // A missing extension is always reported.
        let mut c = sample();
        c["extensions"].as_array_mut().unwrap().remove(1);
        assert_eq!(
            differences(&a, &c, Check::Updated),
            vec!["extensions: postgis: \"public\" -> (missing)"]
        );
    }

    #[test]
    fn a_backup_restore_skips_counts_and_sequences() {
        let a = sample();
        let mut b = sample();
        b["rowcounts"] = json!({});
        b["sequences"]["public.plain_id_seq"] = json!(1001);
        assert!(differences(&a, &b, Check::RestoredFromBackup).is_empty());
        b["relations"] = json!(["public.metrics (r)"]);
        assert_eq!(differences(&a, &b, Check::RestoredFromBackup).len(), 1);
        b["extensions"][2]["version"] = json!("2.30.2");
        assert!(
            differences(&a, &b, Check::RestoredFromBackup)
                .contains(&"extensions: timescaledb version 2.29.2 -> 2.30.2".to_string())
        );
    }

    #[test]
    fn missing_and_extra_list_entries_are_named() {
        let a = sample();
        let mut b = sample();
        b["relations"] = json!(["public.metrics (r)", "public.other (r)"]);
        let diffs = differences(&a, &b, Check::Restored);
        assert_eq!(
            diffs,
            vec![
                "relations: only in the old database: \"public.metrics_pkey (i)\"",
                "relations: only in the new database: \"public.other (r)\"",
            ]
        );
    }

    #[test]
    fn long_lists_of_differences_are_cut() {
        let a = json!({"rowcounts": (0..20).map(|i| (format!("t{i:02}"), json!(i))).collect::<Map<String, Json>>()});
        let b = json!({"rowcounts": {}});
        let diffs = differences(&a, &b, Check::Restored);
        assert_eq!(diffs.len(), MAX_LISTED + 1);
        assert_eq!(diffs.last().unwrap(), "rowcounts: ... and 12 more");
    }

    #[test]
    fn count_queries_quote_names_and_batch() {
        let base = json!({"tables": [["public", "a\"b"], ["_mrsm_internal", "runs"]]});
        let queries = count_queries(&base);
        assert_eq!(queries.len(), 1);
        assert!(
            queries[0].contains("('public.a\"b', (SELECT count(*) FROM \"public\".\"a\"\"b\"))")
        );
        assert!(queries[0].contains("FROM \"_mrsm_internal\".\"runs\""));
        let many =
            json!({"tables": (0..450).map(|i| json!(["s", format!("t{i}")])).collect::<Vec<_>>()});
        assert_eq!(count_queries(&many).len(), 3);
        assert!(count_queries(&json!({"tables": []})).is_empty());
    }

    #[test]
    fn assembled_fingerprints_hold_counts_instead_of_the_table_list() {
        let base = json!({"tables": [["public", "a"]], "extensions": []});
        let fp = assemble(
            base,
            vec![json!({"public.a": 3})],
            Some(json!({"hypertables": []})),
        );
        assert_eq!(fp["rowcounts"]["public.a"], json!(3));
        assert!(fp.get("tables").is_none());
        assert_eq!(fp["hypertables"], json!([]));
        assert_eq!(total_rows(&fp), 3);
    }
}
