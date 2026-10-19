//! The dump `backup postgres` and `upgrade postgres` share: `pg_dumpall --globals-only` (the
//! roles) and, per database, `pg_dump -Fc --create` of a snapshot that a session exports for the
//! purpose, with the database's fingerprint (relations, functions, hypertables, ... and row
//! counts or estimates) taken in the SAME snapshot, so the fingerprint describes exactly what the
//! dump holds even while clients write. Each file is hashed (SHA-256) while it is written and
//! listed with `pg_restore --list`.

use std::path::Path;

use serde_json::{Value as Json, json};

use crate::dispatch::Runtime;

use super::Steps;
use super::docker::{self, Input, Output, PgContainer, Res, StepError};
use super::fingerprint;
use super::server::Tools;
use super::sqltext::{dump_file_name, quote_literal};

/// What a fingerprint counts.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum Counts {
    /// Exact row counts of every table (`count(*)`).
    Exact,
    /// The planner's estimate of the database's rows (cheap; what an online backup records).
    Estimate,
    /// No counts (the structure only).
    Skip,
}

/// A file of a dump set.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct DumpedFile {
    pub file: String,
    pub bytes: u64,
    pub sha256: String,
}

impl DumpedFile {
    fn to_json(&self) -> Json {
        json!({"file": self.file, "bytes": self.bytes, "sha256": self.sha256})
    }

    fn from_json(value: &Json) -> Option<DumpedFile> {
        Some(DumpedFile {
            file: value.get("file")?.as_str()?.to_string(),
            bytes: value.get("bytes")?.as_u64()?,
            sha256: value.get("sha256")?.as_str()?.to_string(),
        })
    }
}

/// A dumped database.
#[derive(Debug, Clone)]
pub(crate) struct DumpedDb {
    pub name: String,
    pub dump: DumpedFile,
    /// The restore list's entries (`pg_restore --list`).
    pub entries: usize,
    /// The fingerprint in the dump's snapshot.
    pub fingerprint: Json,
    /// What writes leave behind ([`fingerprint::ACTIVITY_QUERY`]), read right before the dump's
    /// snapshot (not kept in the manifest).
    pub activity: Option<Json>,
}

impl DumpedDb {
    /// The TimescaleDB version the dump carries.
    pub fn timescaledb(&self) -> Option<String> {
        fingerprint::timescaledb_version(&self.fingerprint)
    }
}

/// The files of a dump.
#[derive(Debug, Clone)]
pub(crate) struct DumpSet {
    /// `zstd` or `gzip`.
    pub compression: String,
    /// Whether the fingerprints hold exact row counts.
    pub exact_counts: bool,
    pub globals: DumpedFile,
    pub databases: Vec<DumpedDb>,
}

impl DumpSet {
    pub fn to_json(&self) -> Json {
        json!({
            "compression": self.compression,
            "exact_counts": self.exact_counts,
            "globals": self.globals.to_json(),
            "databases": self.databases.iter().map(|d| json!({
                "name": d.name,
                "file": d.dump.file,
                "bytes": d.dump.bytes,
                "sha256": d.dump.sha256,
                "entries": d.entries,
                "timescaledb": d.timescaledb(),
                "extensions": d.fingerprint.get("extensions").cloned().unwrap_or(Json::Null),
                "row_estimate": d.fingerprint.get("row_estimate").cloned().unwrap_or(Json::Null),
                "rows": d.exact_rows(self.exact_counts),
                "fingerprint": d.fingerprint,
            })).collect::<Vec<_>>(),
        })
    }

    /// The dump set a manifest's `dumps` describes.
    pub fn from_json(value: &Json) -> Result<DumpSet, String> {
        let bad = || "The manifest does not describe its dumps.".to_string();
        let globals = value
            .get("globals")
            .and_then(DumpedFile::from_json)
            .ok_or_else(bad)?;
        let mut databases = Vec::new();
        for db in value
            .get("databases")
            .and_then(Json::as_array)
            .ok_or_else(bad)?
        {
            databases.push(DumpedDb {
                name: db
                    .get("name")
                    .and_then(Json::as_str)
                    .ok_or_else(bad)?
                    .to_string(),
                dump: DumpedFile::from_json(db).ok_or_else(bad)?,
                entries: db.get("entries").and_then(Json::as_u64).unwrap_or(0) as usize,
                fingerprint: db.get("fingerprint").cloned().ok_or_else(bad)?,
                activity: None,
            });
        }
        Ok(DumpSet {
            compression: value
                .get("compression")
                .and_then(Json::as_str)
                .unwrap_or("gzip")
                .to_string(),
            exact_counts: value
                .get("exact_counts")
                .and_then(Json::as_bool)
                .unwrap_or(false),
            globals,
            databases,
        })
    }

    pub fn total_bytes(&self) -> u64 {
        self.globals.bytes + self.databases.iter().map(|d| d.dump.bytes).sum::<u64>()
    }

    /// Every file is there, with its recorded size and SHA-256 (a damaged dump is refused before
    /// anything changes; the checksums are kept next to the dumps, so they detect damage, not a
    /// deliberate edit).
    pub fn verify_files(&self, dir: &Path) -> Result<(), String> {
        let files = std::iter::once(&self.globals).chain(self.databases.iter().map(|d| &d.dump));
        let mut bad = Vec::new();
        for f in files {
            let path = dir.join(&f.file);
            match docker::sha256_file(&path) {
                Ok((sha, bytes)) if sha == f.sha256 && bytes == f.bytes => {}
                Ok((sha, bytes)) => bad.push(format!(
                    "{}: {bytes} bytes, SHA-256 {sha} (the manifest says {} bytes, {})",
                    f.file, f.bytes, f.sha256
                )),
                Err(e) => bad.push(e.message),
            }
        }
        if bad.is_empty() {
            Ok(())
        } else {
            Err(format!(
                "The backup in {} is damaged or was changed:\n  - {}",
                dir.display(),
                bad.join("\n  - ")
            ))
        }
    }
}

impl DumpedDb {
    fn exact_rows(&self, exact: bool) -> Json {
        if exact {
            json!(fingerprint::total_rows(&self.fingerprint))
        } else {
            Json::Null
        }
    }
}

/// `sql` inside a read-only transaction on an exported snapshot.
fn in_snapshot(snapshot: Option<&str>, sql: &str) -> String {
    match snapshot {
        Some(id) => format!(
            "BEGIN ISOLATION LEVEL REPEATABLE READ READ ONLY;\nSET TRANSACTION SNAPSHOT {};\n{sql}\nCOMMIT;\n",
            quote_literal(id)
        ),
        None => sql.to_string(),
    }
}

/// What writes leave behind in a database ([`fingerprint::ACTIVITY_QUERY`]).
pub(crate) fn activity_of(pg: &PgContainer, database: &str) -> Res<Json> {
    pg.json(database, fingerprint::ACTIVITY_QUERY)
}

/// A database's fingerprint, in `snapshot` when given.
pub(crate) fn fingerprint_of(
    pg: &PgContainer,
    database: &str,
    snapshot: Option<&str>,
    counts: Counts,
) -> Res<Json> {
    let base = pg.json(database, &in_snapshot(snapshot, fingerprint::BASE_QUERY))?;
    let mut parts = Vec::new();
    if counts == Counts::Exact {
        for query in fingerprint::count_queries(&base) {
            parts.push(pg.json(database, &in_snapshot(snapshot, &query))?);
        }
    }
    let timescale = if fingerprint::timescaledb_version(&base).is_some() {
        Some(pg.json(
            database,
            &in_snapshot(snapshot, fingerprint::TIMESCALE_QUERY),
        )?)
    } else {
        None
    };
    let mut fp = fingerprint::assemble(base, parts, timescale);
    if counts == Counts::Estimate {
        let estimate = pg.json(
            database,
            &in_snapshot(snapshot, fingerprint::ESTIMATE_QUERY),
        )?;
        if let (Json::Object(m), Some(v)) = (&mut fp, estimate.get("row_estimate")) {
            m.insert("row_estimate".into(), v.clone());
        }
    }
    Ok(fp)
}

/// Dump the roles and `databases` of `server` into `dir` with `tools`.
pub(crate) fn dump(
    rt: &mut Runtime<'_>,
    steps: &mut Steps,
    server: &PgContainer,
    tools: &Tools,
    databases: &[String],
    dir: &Path,
    counts: Counts,
) -> Res<DumpSet> {
    let compression = tools.compression().to_string();
    let globals_path = dir.join("globals.sql");
    let globals = steps.run(rt, "Dumping the roles (pg_dumpall --globals-only)", |_| {
        let done = tools.run(
            "pg_dumpall",
            &["--globals-only".into()],
            Input::Null,
            Output::Hashed(&globals_path),
        )?;
        if !done.ok() {
            return Err(StepError::new(format!(
                "`pg_dumpall --globals-only` failed (exit code {}):\n{}",
                done.code,
                done.stderr_tail(20)
            )));
        }
        Ok(DumpedFile {
            file: "globals.sql".into(),
            bytes: done.bytes,
            sha256: done.sha256.unwrap_or_default(),
        })
    })?;
    let mut dumped = Vec::new();
    for (i, database) in databases.iter().enumerate() {
        let file = dump_file_name(i, database);
        let path = dir.join(&file);
        let db = steps.run(
            rt,
            &format!("Dumping '{database}' (pg_dump, {compression})"),
            |_| {
                // Before the snapshot: a write after it changes these.
                let activity = activity_of(server, database)?;
                let snapshot = server.export_snapshot(database)?;
                let mut args = vec!["-Fc".to_string(), "--create".to_string()];
                if compression == "zstd" {
                    args.push("--compress=zstd".into());
                }
                args.extend([
                    format!("--snapshot={}", snapshot.id),
                    "-d".to_string(),
                    docker::conninfo(database),
                ]);
                let done = tools.run("pg_dump", &args, Input::Null, Output::Hashed(&path))?;
                if !done.ok() {
                    return Err(StepError::new(format!(
                        "`pg_dump` of '{database}' failed (exit code {}):\n{}",
                        done.code,
                        done.stderr_tail(20)
                    )));
                }
                let fp = fingerprint_of(server, database, Some(&snapshot.id), counts)?;
                snapshot.finish()?;
                let listed = tools.run(
                    "pg_restore",
                    &["--list".into()],
                    Input::File(&path),
                    Output::Capture,
                )?;
                // An empty database lists no entries: only the exit code says it is readable.
                let entries = listed
                    .stdout_text()
                    .lines()
                    .filter(|l| !l.trim().is_empty() && !l.starts_with(';'))
                    .count();
                if !listed.ok() {
                    return Err(StepError::new(format!(
                        "The dump of '{database}' cannot be read back (`pg_restore --list`):\n{}",
                        listed.stderr_tail(10)
                    )));
                }
                Ok(DumpedDb {
                    name: database.clone(),
                    dump: DumpedFile {
                        file: file.clone(),
                        bytes: done.bytes,
                        sha256: done.sha256.unwrap_or_default(),
                    },
                    entries,
                    fingerprint: fp,
                    activity: Some(activity),
                })
            },
        )?;
        rt.print(&format!(
            "    {}: {}\n",
            db.dump.file,
            super::human_bytes(db.dump.bytes as i64)
        ));
        dumped.push(db);
    }
    Ok(DumpSet {
        compression,
        exact_counts: counts == Counts::Exact,
        globals,
        databases: dumped,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    fn set() -> DumpSet {
        DumpSet {
            compression: "zstd".into(),
            exact_counts: true,
            globals: DumpedFile {
                file: "globals.sql".into(),
                bytes: 3,
                sha256: "x".into(),
            },
            databases: vec![DumpedDb {
                name: "meerschaum".into(),
                dump: DumpedFile {
                    file: "01-meerschaum.dump".into(),
                    bytes: 10,
                    sha256: "y".into(),
                },
                entries: 42,
                fingerprint: json!({
                    "extensions": [{"name": "timescaledb", "version": "2.29.2", "schema": "public"}],
                    "rowcounts": {"public.a": 3, "public.b": 4}
                }),
                activity: None,
            }],
        }
    }

    #[test]
    fn dump_sets_round_trip_through_the_manifest() {
        let original = set();
        let value = original.to_json();
        assert_eq!(value["databases"][0]["timescaledb"], json!("2.29.2"));
        assert_eq!(value["databases"][0]["rows"], json!(7));
        let back = DumpSet::from_json(&value).unwrap();
        assert_eq!(back.compression, "zstd");
        assert!(back.exact_counts);
        assert_eq!(back.globals, original.globals);
        assert_eq!(back.databases[0].dump, original.databases[0].dump);
        assert_eq!(back.databases[0].entries, 42);
        assert_eq!(back.total_bytes(), 13);
        assert!(DumpSet::from_json(&json!({})).is_err());
    }

    #[test]
    fn damaged_files_are_named() {
        let dir = tempfile::tempdir().unwrap();
        std::fs::write(dir.path().join("globals.sql"), "abc").unwrap();
        std::fs::write(dir.path().join("01-meerschaum.dump"), "0123456789").unwrap();
        let mut good = set();
        good.globals.sha256 = docker::sha256_file(&dir.path().join("globals.sql"))
            .unwrap()
            .0;
        good.databases[0].dump.sha256 = docker::sha256_file(&dir.path().join("01-meerschaum.dump"))
            .unwrap()
            .0;
        assert!(good.verify_files(dir.path()).is_ok());
        std::fs::write(dir.path().join("01-meerschaum.dump"), "0123456780").unwrap();
        let error = good.verify_files(dir.path()).unwrap_err();
        assert!(error.contains("01-meerschaum.dump"), "{error}");
        assert!(!error.contains("globals.sql:"), "{error}");
        std::fs::remove_file(dir.path().join("globals.sql")).unwrap();
        assert!(
            good.verify_files(dir.path())
                .unwrap_err()
                .contains("globals.sql")
        );
    }

    #[test]
    fn snapshot_queries_import_the_snapshot_first() {
        let sql = in_snapshot(Some("00000003-0000001A-1"), "SELECT 1;");
        assert!(sql.starts_with(
            "BEGIN ISOLATION LEVEL REPEATABLE READ READ ONLY;\nSET TRANSACTION SNAPSHOT '00000003-0000001A-1';\nSELECT 1;"
        ));
        assert_eq!(in_snapshot(None, "SELECT 1;"), "SELECT 1;");
    }
}
