//! Text the upgrade generates for, or reads from, the PostgreSQL client tools: quoting, version
//! order, the restore list filter, the globals rewrite, and the database-level statements of a
//! `pg_restore --create` script.

use std::cmp::Ordering;
use std::collections::BTreeSet;

/// `quote_ident`: always quoted, so any name is safe.
pub fn quote_ident(name: &str) -> String {
    format!("\"{}\"", name.replace('"', "\"\""))
}

/// `quote_literal` for `standard_conforming_strings = on` (every supported server).
pub fn quote_literal(text: &str) -> String {
    format!("'{}'", text.replace('\'', "''"))
}

/// Compare two extension versions (`2.29.2` < `2.30.0`): numeric parts numerically, anything
/// else as text.
pub fn version_cmp(a: &str, b: &str) -> Ordering {
    let split =
        |v: &str| -> Vec<String> { v.split(['.', '-']).map(str::to_string).collect::<Vec<_>>() };
    let (pa, pb) = (split(a), split(b));
    for (x, y) in pa.iter().zip(pb.iter()) {
        let ord = match (x.parse::<u64>(), y.parse::<u64>()) {
            (Ok(nx), Ok(ny)) => nx.cmp(&ny),
            _ => x.cmp(y),
        };
        if ord != Ordering::Equal {
            return ord;
        }
    }
    pa.len().cmp(&pb.len())
}

/// Whether the tag part of a restore-list entry names `name`: the bare or the quoted name,
/// optionally followed by the owner.
fn names_extension(tag: &str, name: &str) -> bool {
    let tag = tag.trim_end();
    let quoted = quote_ident(name);
    [name, quoted.as_str()]
        .iter()
        .any(|n| tag == *n || tag.starts_with(&format!("{n} ")))
}

/// The `<desc> <namespace> <tag> <owner>` part of a `pg_restore -l` entry line.
fn entry_desc(line: &str) -> Option<&str> {
    line.split_once("; ")
        .filter(|(id, _)| !id.is_empty() && id.chars().all(|c| c.is_ascii_digit()))
        .and_then(|(_, rest)| {
            // `<tableoid> <oid> <desc> <namespace> <tag> <owner>`.
            let mut parts = rest.splitn(3, ' ');
            let (_, _) = (parts.next()?, parts.next()?);
            parts.next()
        })
}

/// Comment out the `EXTENSION` entries (and their `COMMENT ON EXTENSION` entries) of `skip` in a
/// `pg_restore -l` list: those extensions already exist in the new database; and the `SCHEMA`
/// entries of `skip_schemas` (created before an extension that lives in them). Returns the list
/// for `pg_restore -L` and the names of the entries that were commented out.
pub fn filter_restore_list(
    list: &str,
    skip: &BTreeSet<String>,
    skip_schemas: &BTreeSet<String>,
) -> (String, Vec<String>) {
    let mut out = String::with_capacity(list.len());
    let mut skipped = Vec::new();
    for line in list.lines() {
        let entry = entry_desc(line);
        let skip_line = entry.is_some_and(|desc| {
            if let Some(tag) = desc.strip_prefix("EXTENSION - ") {
                skip.iter().any(|n| names_extension(tag, n))
            } else if let Some(tag) = desc.strip_prefix("COMMENT - EXTENSION ") {
                skip.iter().any(|n| names_extension(tag, n))
            } else if let Some(tag) = desc.strip_prefix("SCHEMA - ") {
                skip_schemas.iter().any(|n| names_extension(tag, n))
            } else {
                false
            }
        });
        if skip_line {
            skipped.push(entry.unwrap_or_default().trim_end().to_string());
            out.push(';');
        }
        out.push_str(line);
        out.push('\n');
    }
    (out, skipped)
}

/// The owner of schema `name` in a `pg_restore -l` list (its `SCHEMA` entry).
pub fn schema_owner(list: &str, name: &str) -> Option<String> {
    list.lines().find_map(|line| {
        let tag = entry_desc(line)?.strip_prefix("SCHEMA - ")?;
        let owner = tag.strip_prefix(name)?.strip_prefix(' ')?.trim();
        (!owner.is_empty() && !owner.contains(' ')).then(|| owner.to_string())
    })
}

/// The database-level entries of a `pg_restore --create -l` list: the database itself, its
/// properties (`ALTER DATABASE ... SET`), and its comment, security label and privileges.
pub fn database_entries(list: &str) -> String {
    let mut out = String::new();
    for line in list.lines() {
        let Some((id, rest)) = line.split_once("; ") else {
            continue;
        };
        if id.is_empty() || !id.chars().all(|c| c.is_ascii_digit()) {
            continue;
        }
        let mut parts = rest.splitn(3, ' ');
        let (Some(_), Some(_), Some(desc)) = (parts.next(), parts.next(), parts.next()) else {
            continue;
        };
        if [
            "DATABASE ",
            "COMMENT - DATABASE ",
            "ACL - DATABASE ",
            "SECURITY LABEL - DATABASE ",
        ]
        .iter()
        .any(|p| desc.starts_with(p))
        {
            out.push_str(line);
            out.push('\n');
        }
    }
    out
}

/// `pg_dumpall --globals-only` creates every role with `CREATE ROLE name;`, which fails for the
/// roles the new server's setup already created (its superuser): create each role only when it
/// is missing, so that the script can run with `ON_ERROR_STOP` and every remaining error is real.
/// The `ALTER ROLE` that follows each one sets its attributes and password either way.
pub fn rewrite_globals(sql: &str) -> Result<String, String> {
    const TAG: &str = "$mrsm_upgrade$";
    let mut out = String::with_capacity(sql.len() + 256);
    for line in sql.lines() {
        let role = line
            .strip_prefix("CREATE ROLE ")
            .and_then(|r| r.strip_suffix(';'));
        match role {
            Some(ident) => {
                if ident.contains(TAG) {
                    return Err(format!("Cannot restore the role {ident}."));
                }
                let name = match ident.strip_prefix('"').and_then(|i| i.strip_suffix('"')) {
                    Some(inner) => inner.replace("\"\"", "\""),
                    None => ident.to_string(),
                };
                out.push_str(&format!(
                    "DO {TAG}BEGIN IF NOT EXISTS (SELECT FROM pg_catalog.pg_roles WHERE rolname = \
                     {}) THEN CREATE ROLE {ident}; END IF; END{TAG};",
                    quote_literal(&name)
                ));
            }
            None => out.push_str(line),
        }
        out.push('\n');
    }
    Ok(out)
}

/// The database-level statements of a `pg_restore --create` script for one database.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct DatabaseStatements {
    /// `CREATE DATABASE ...` (encoding, locale and template of the original).
    pub create: Vec<String>,
    /// The other statements of the `DATABASE` entry (`ALTER DATABASE ... OWNER TO ...`).
    pub alter: Vec<String>,
    /// The `COMMENT`, `DATABASE PROPERTIES` (`ALTER DATABASE ... SET ...`, `ALTER ROLE ... IN
    /// DATABASE ... SET ...`) and `ACL` statements, applied after the restore.
    pub properties: String,
}

/// Split the script `pg_restore --create -f - -L <database entries>` prints into the statements
/// that create the database and those that restore its properties. A section starts at its
/// `-- Name: ...; Type: <TYPE>; ...` header and ends at the next header or at a meta-command
/// (`\connect`, `\unrestrict`), after which `pg_restore` only resets session settings.
pub fn split_database_script(script: &str) -> DatabaseStatements {
    let mut out = DatabaseStatements::default();
    let mut current: Option<String> = None;
    let mut in_database_statement = false;
    for line in script.lines() {
        if let Some(rest) = line.strip_prefix("-- Name: ")
            && let Some((_, after)) = rest.split_once("; Type: ")
        {
            current = Some(after.split("; ").next().unwrap_or_default().to_string());
            in_database_statement = false;
            continue;
        }
        if line.starts_with('\\') {
            current = None;
            continue;
        }
        let Some(kind) = current.as_deref() else {
            continue;
        };
        if kind == "DATABASE" {
            // One statement per line in this entry; skip the comment lines and blanks.
            let trimmed = line.trim();
            if trimmed.is_empty() || trimmed == "--" {
                in_database_statement = false;
                continue;
            }
            if trimmed.starts_with("CREATE DATABASE ") {
                out.create.push(line.to_string());
                in_database_statement = !trimmed.ends_with(';');
            } else if in_database_statement {
                if let Some(last) = out.create.last_mut() {
                    last.push('\n');
                    last.push_str(line);
                }
                in_database_statement = !trimmed.ends_with(';');
            } else {
                out.alter.push(line.to_string());
            }
        } else if line != "--" {
            // `--` lines only frame the section headers.
            out.properties.push_str(line);
            out.properties.push('\n');
        }
    }
    let trimmed = out.properties.trim();
    out.properties = if trimmed.is_empty() {
        String::new()
    } else {
        format!("{trimmed}\n")
    };
    out
}

/// The settings in a `postgresql.auto.conf` (`ALTER SYSTEM` writes `name = 'value'` lines, with
/// quotes and backslashes doubled), in order; a later line wins.
pub fn parse_auto_conf(text: &str) -> Vec<(String, String)> {
    let mut out: Vec<(String, String)> = Vec::new();
    for line in text.lines() {
        let line = line.trim();
        if line.is_empty() || line.starts_with('#') {
            continue;
        }
        let Some((name, value)) = line.split_once('=') else {
            continue;
        };
        let name = name.trim().to_string();
        let value = value.trim();
        let value = match value.strip_prefix('\'').and_then(|v| v.strip_suffix('\'')) {
            Some(inner) => {
                let mut unescaped = String::with_capacity(inner.len());
                let mut chars = inner.chars().peekable();
                while let Some(c) = chars.next() {
                    if (c == '\'' || c == '\\') && chars.peek() == Some(&c) {
                        chars.next();
                    }
                    unescaped.push(c);
                }
                unescaped
            }
            None => value.to_string(),
        };
        out.retain(|(n, _)| *n != name);
        out.push((name, value));
    }
    out
}

/// Why a server setting of the old server is not carried over to the new one (`None`: it is).
/// These act on files or other servers that the old server owns: the new server would archive
/// into the old server's WAL archive, or replicate from its primary.
pub fn not_carried(name: &str) -> Option<&'static str> {
    let name = name.to_ascii_lowercase();
    if name.starts_with("archive_") || name == "restore_command" || name.starts_with("recovery_") {
        return Some("WAL archiving and recovery");
    }
    if name.starts_with("primary_")
        || name == "promote_trigger_file"
        || name == "synchronous_standby_names"
    {
        return Some("replication");
    }
    if [
        "data_directory",
        "config_file",
        "hba_file",
        "ident_file",
        "external_pid_file",
        "unix_socket_directories",
        "port",
    ]
    .contains(&name.as_str())
    {
        return Some("the server's files, socket and port");
    }
    None
}

/// `CREATE DATABASE` with a database's encoding and locale (to check that a server can create
/// it), for a server of PostgreSQL `major`.
pub fn create_database_sql(
    name: &str,
    locale: &super::fingerprint::DbLocale,
    major: u32,
) -> String {
    let mut sql = format!(
        "CREATE DATABASE {} TEMPLATE template0 ENCODING {} LC_COLLATE {} LC_CTYPE {}",
        quote_ident(name),
        quote_literal(&locale.encoding),
        quote_literal(&locale.collate),
        quote_literal(&locale.ctype)
    );
    if major >= 15 {
        match locale.provider.as_str() {
            "i" => {
                sql.push_str(" LOCALE_PROVIDER icu");
                if let Some(icu) = &locale.locale {
                    sql.push_str(&format!(" ICU_LOCALE {}", quote_literal(icu)));
                }
            }
            "b" => {
                sql.push_str(" LOCALE_PROVIDER builtin");
                if let Some(builtin) = &locale.locale {
                    sql.push_str(&format!(" BUILTIN_LOCALE {}", quote_literal(builtin)));
                }
            }
            _ => sql.push_str(" LOCALE_PROVIDER libc"),
        }
    }
    if major >= 16
        && let Some(rules) = &locale.icu_rules
    {
        sql.push_str(&format!(" ICU_RULES {}", quote_literal(rules)));
    }
    sql.push(';');
    sql
}

/// The settings PostgreSQL keeps as lists of separately quoted elements (`GUC_LIST_QUOTE`).
const LIST_SETTINGS: &[&str] = &[
    "search_path",
    "temp_tablespaces",
    "shared_preload_libraries",
    "session_preload_libraries",
    "local_preload_libraries",
];

/// The elements of a list setting's value as `postgresql.auto.conf` holds it
/// (`timescaledb, pg_stat_statements`, `"$user", public`).
pub fn split_setting_list(value: &str) -> Vec<String> {
    let mut items = Vec::new();
    let mut current = String::new();
    let mut quoted = false;
    let mut chars = value.chars().peekable();
    while let Some(c) = chars.next() {
        match c {
            '"' if quoted && chars.peek() == Some(&'"') => {
                current.push('"');
                chars.next();
            }
            '"' => quoted = !quoted,
            ',' if !quoted => items.push(std::mem::take(&mut current)),
            c if !quoted && c.is_whitespace() => {}
            c => current.push(c),
        }
    }
    items.push(current);
    items.into_iter().filter(|i| !i.is_empty()).collect()
}

/// The new server's preloaded libraries followed by the ones only the old server preloaded.
pub fn merge_preload(new: &str, old: &[String]) -> Vec<String> {
    let mut merged = split_setting_list(new);
    for lib in old {
        if !merged.contains(lib) {
            merged.push(lib.clone());
        }
    }
    merged
}

/// A setting's name for `ALTER SYSTEM` (bare when it is a plain dotted name, as every setting is).
fn setting_name(name: &str) -> String {
    let plain = name.split('.').all(|part| {
        let mut chars = part.chars();
        chars
            .next()
            .is_some_and(|c| c.is_ascii_lowercase() || c == '_')
            && chars.all(|c| c.is_ascii_lowercase() || c.is_ascii_digit() || c == '_')
    });
    if plain {
        name.to_string()
    } else {
        quote_ident(name)
    }
}

/// `ALTER SYSTEM SET name = 'a', 'b';` for a list setting (one literal per element: a single
/// literal would be ONE element, e.g. one library named `timescaledb,pg_stat_statements`).
pub fn alter_system_list(name: &str, items: &[String]) -> String {
    let rhs = if items.is_empty() {
        "''".to_string()
    } else {
        items
            .iter()
            .map(|i| quote_literal(i))
            .collect::<Vec<_>>()
            .join(", ")
    };
    format!("ALTER SYSTEM SET {} = {rhs};", setting_name(name))
}

/// `ALTER SYSTEM SET name = value;` for a value as `postgresql.auto.conf` holds it.
pub fn alter_system(name: &str, value: &str) -> String {
    if LIST_SETTINGS.contains(&name.to_ascii_lowercase().as_str()) {
        alter_system_list(name, &split_setting_list(value))
    } else {
        format!(
            "ALTER SYSTEM SET {} = {};",
            setting_name(name),
            quote_literal(value)
        )
    }
}

/// Split a `shared_preload_libraries` value into the libraries present in `files` (the new
/// server's library directory: `name.so`, `name.dylib`, `name.dll`, or a `$libdir/` path) and the
/// missing ones.
pub fn available_libraries(value: &str, files: &BTreeSet<String>) -> (Vec<String>, Vec<String>) {
    let mut kept = Vec::new();
    let mut missing = Vec::new();
    for lib in split_setting_list(value) {
        let stem = lib.strip_prefix("$libdir/").unwrap_or(&lib);
        let found = ["", ".so", ".dylib", ".dll"]
            .iter()
            .any(|ext| files.contains(&format!("{stem}{ext}")));
        if found {
            kept.push(lib);
        } else {
            missing.push(lib);
        }
    }
    (kept, missing)
}

/// A name for a file holding database `name`'s dump: `NN-<safe name>.dump`.
pub fn dump_file_name(index: usize, name: &str) -> String {
    let safe: String = name
        .chars()
        .map(|c| {
            if c.is_ascii_alphanumeric() || c == '_' || c == '-' {
                c
            } else {
                '_'
            }
        })
        .collect();
    format!("{:02}-{safe}.dump", index + 1)
}

#[cfg(test)]
mod tests {
    use super::*;

    /// A `pg_restore -l` listing: the entries of objects without an owner (extensions and their
    /// comments) end with a space.
    const LIST: &str = concat!(
        ";\n",
        "; Archive created at 2026-10-18 15:52:32 UTC\n",
        ";     dbname: meerschaum\n",
        ";\n",
        "; Selected TOC Entries:\n",
        ";\n",
        "15; 2615 25266 SCHEMA - _mrsm_internal mrsm\n",
        "2; 3079 23103 EXTENSION - timescaledb \n",
        "5193; 0 0 COMMENT - EXTENSION timescaledb \n",
        "3; 3079 23830 EXTENSION - postgis \n",
        "5194; 0 0 COMMENT - EXTENSION postgis \n",
        "4; 3079 23999 EXTENSION - uuid-ossp \n",
        "5195; 0 0 COMMENT - EXTENSION \"uuid-ossp\" \n",
        "6; 3079 24000 EXTENSION - timescaledb_toolkit \n",
        "330; 1259 25267 TABLE _mrsm_internal engine_state mrsm\n",
        "289; 1259 24924 TABLE public timescaledb mrsm\n",
        "5143; 0 24924 TABLE DATA public metrics mrsm\n",
    );

    #[test]
    fn the_restore_list_skips_only_existing_extensions_and_their_comments() {
        let skip: BTreeSet<String> = ["timescaledb", "uuid-ossp"]
            .iter()
            .map(|s| s.to_string())
            .collect();
        let (list, skipped) = filter_restore_list(LIST, &skip, &BTreeSet::new());
        assert_eq!(
            skipped,
            vec![
                "EXTENSION - timescaledb",
                "COMMENT - EXTENSION timescaledb",
                "EXTENSION - uuid-ossp",
                "COMMENT - EXTENSION \"uuid-ossp\"",
            ]
        );
        assert!(list.contains(";2; 3079 23103 EXTENSION - timescaledb \n"));
        assert!(list.contains(";5195; 0 0 COMMENT - EXTENSION \"uuid-ossp\" \n"));
        // Not a prefix match: the toolkit, PostGIS and a table named `timescaledb` stay.
        assert!(list.contains("\n6; 3079 24000 EXTENSION - timescaledb_toolkit \n"));
        assert!(list.contains("\n3; 3079 23830 EXTENSION - postgis \n"));
        assert!(list.contains("\n289; 1259 24924 TABLE public timescaledb mrsm\n"));
        assert_eq!(list.lines().count(), LIST.lines().count());
    }

    #[test]
    fn a_schema_created_first_is_skipped_and_its_owner_found() {
        let schemas: BTreeSet<String> = ["_mrsm_internal".to_string()].into_iter().collect();
        let (list, skipped) = filter_restore_list(LIST, &BTreeSet::new(), &schemas);
        assert_eq!(skipped, vec!["SCHEMA - _mrsm_internal mrsm"]);
        assert!(list.contains(";15; 2615 25266 SCHEMA - _mrsm_internal mrsm\n"));
        assert!(list.contains("\n330; 1259 25267 TABLE _mrsm_internal engine_state mrsm\n"));
        assert_eq!(
            schema_owner(LIST, "_mrsm_internal").as_deref(),
            Some("mrsm")
        );
        assert_eq!(schema_owner(LIST, "_mrsm"), None);
        assert_eq!(schema_owner(LIST, "public"), None);
    }

    #[test]
    fn settings_of_files_and_other_servers_are_not_carried() {
        for name in [
            "archive_mode",
            "archive_command",
            "restore_command",
            "recovery_target_time",
            "primary_conninfo",
            "primary_slot_name",
            "hba_file",
            "port",
        ] {
            assert!(not_carried(name).is_some(), "{name}");
        }
        for name in [
            "shared_preload_libraries",
            "work_mem",
            "timescaledb.telemetry_level",
        ] {
            assert!(not_carried(name).is_none(), "{name}");
        }
    }

    #[test]
    fn create_database_names_the_provider_explicitly() {
        use super::super::fingerprint::DbLocale;
        let libc = DbLocale {
            encoding: "UTF8".into(),
            collate: "en_US.utf8".into(),
            ctype: "en_US.utf8".into(),
            provider: "c".into(),
            locale: None,
            icu_rules: None,
        };
        assert_eq!(
            create_database_sql("db", &libc, 18),
            "CREATE DATABASE \"db\" TEMPLATE template0 ENCODING 'UTF8' LC_COLLATE 'en_US.utf8' \
             LC_CTYPE 'en_US.utf8' LOCALE_PROVIDER libc;"
        );
        assert!(!create_database_sql("db", &libc, 14).contains("LOCALE_PROVIDER"));
        let icu = DbLocale {
            provider: "i".into(),
            locale: Some("en-US".into()),
            icu_rules: Some("&a < b".into()),
            ..libc
        };
        assert!(
            create_database_sql("db", &icu, 17)
                .ends_with("LOCALE_PROVIDER icu ICU_LOCALE 'en-US' ICU_RULES '&a < b';")
        );
    }

    #[test]
    fn database_entries_are_the_database_level_ones() {
        let list = "\
;
; Selected TOC Entries:
;
5190; 1262 16384 DATABASE - meerschaum mrsm
5191; 0 0 COMMENT - DATABASE meerschaum mrsm
5192; 0 0 DATABASE PROPERTIES - meerschaum mrsm
5193; 0 0 ACL - DATABASE meerschaum mrsm
15; 2615 25266 SCHEMA - _mrsm_internal mrsm
5194; 0 0 COMMENT - EXTENSION timescaledb
289; 1259 24924 TABLE public DATABASE mrsm
";
        assert_eq!(
            database_entries(list),
            "5190; 1262 16384 DATABASE - meerschaum mrsm\n\
             5191; 0 0 COMMENT - DATABASE meerschaum mrsm\n\
             5192; 0 0 DATABASE PROPERTIES - meerschaum mrsm\n\
             5193; 0 0 ACL - DATABASE meerschaum mrsm\n"
        );
    }

    #[test]
    fn globals_create_only_missing_roles() {
        let sql = "\\restrict abc\nCREATE ROLE mrsm;\nALTER ROLE mrsm WITH SUPERUSER LOGIN PASSWORD 'SCRAM';\nCREATE ROLE \"Odd \"\"Name\";\n\\unrestrict abc\n";
        let out = rewrite_globals(sql).unwrap();
        assert!(out.contains(
            "DO $mrsm_upgrade$BEGIN IF NOT EXISTS (SELECT FROM pg_catalog.pg_roles WHERE rolname = 'mrsm') THEN CREATE ROLE mrsm; END IF; END$mrsm_upgrade$;"
        ));
        assert!(out.contains("rolname = 'Odd \"Name') THEN CREATE ROLE \"Odd \"\"Name\";"));
        assert!(out.contains("ALTER ROLE mrsm WITH SUPERUSER LOGIN PASSWORD 'SCRAM';\n"));
        assert!(out.starts_with("\\restrict abc\n"));
        assert!(rewrite_globals("CREATE ROLE \"a$mrsm_upgrade$b\";\n").is_err());
    }

    #[test]
    fn database_scripts_split_into_create_and_properties() {
        let script = "\
--
-- PostgreSQL database dump
--

\\restrict KEY

SET statement_timeout = 0;
SELECT pg_catalog.set_config('search_path', '', false);

--
-- Name: meerschaum; Type: DATABASE; Schema: -; Owner: mrsm
--

CREATE DATABASE meerschaum WITH TEMPLATE = template0 ENCODING = 'UTF8' LOCALE_PROVIDER = libc LOCALE = 'C.UTF-8';


ALTER DATABASE meerschaum OWNER TO mrsm;

\\unrestrict KEY
\\connect meerschaum
\\restrict KEY

SET statement_timeout = 0;

--
-- Name: DATABASE meerschaum; Type: COMMENT; Schema: -; Owner: mrsm
--

COMMENT ON DATABASE meerschaum IS 'two
lines';


--
-- Name: meerschaum; Type: DATABASE PROPERTIES; Schema: -; Owner: mrsm
--

ALTER DATABASE meerschaum SET \"TimeZone\" TO 'UTC';


\\unrestrict KEY
\\connect meerschaum
\\restrict KEY

SET statement_timeout = 0;

--
-- PostgreSQL database dump complete
--
";
        let parts = split_database_script(script);
        assert_eq!(
            parts.create,
            vec![
                "CREATE DATABASE meerschaum WITH TEMPLATE = template0 ENCODING = 'UTF8' LOCALE_PROVIDER = libc LOCALE = 'C.UTF-8';"
            ]
        );
        assert_eq!(
            parts.alter,
            vec!["ALTER DATABASE meerschaum OWNER TO mrsm;"]
        );
        assert!(
            parts
                .properties
                .starts_with("COMMENT ON DATABASE meerschaum IS 'two\nlines';\n")
        );
        assert!(
            parts
                .properties
                .ends_with("\nALTER DATABASE meerschaum SET \"TimeZone\" TO 'UTC';\n")
        );
        assert!(!parts.properties.contains("statement_timeout"));
        assert!(!parts.properties.contains("\\connect"));
        assert!(!parts.properties.contains("dump complete"));
        // A database without properties.
        let bare = split_database_script(
            "--\n-- Name: second; Type: DATABASE; Schema: -; Owner: mrsm\n--\n\nCREATE DATABASE second WITH TEMPLATE = template0;\n\n\nALTER DATABASE second OWNER TO mrsm;\n\n\\connect second\nSET x = 0;\n",
        );
        assert_eq!(bare.create.len(), 1);
        assert_eq!(bare.alter, vec!["ALTER DATABASE second OWNER TO mrsm;"]);
        assert_eq!(bare.properties, "");
    }

    #[test]
    fn versions_compare_numerically() {
        assert_eq!(version_cmp("2.29.2", "2.30.0"), Ordering::Less);
        assert_eq!(version_cmp("2.9.0", "2.10.0"), Ordering::Less);
        assert_eq!(version_cmp("2.30.2", "2.30.2"), Ordering::Equal);
        assert_eq!(version_cmp("3.6.4", "3.6"), Ordering::Greater);
        assert_eq!(version_cmp("1.26.0", "1.9.9"), Ordering::Greater);
    }

    #[test]
    fn preload_libraries_merge_without_duplicates() {
        assert_eq!(
            merge_preload(
                "timescaledb,pg_textsearch",
                &["timescaledb".into(), "pg_stat_statements".into()]
            ),
            vec!["timescaledb", "pg_textsearch", "pg_stat_statements"]
        );
        assert_eq!(
            merge_preload("", &["timescaledb".into()]),
            vec!["timescaledb"]
        );
    }

    #[test]
    fn auto_conf_files_parse_like_postgres_writes_them() {
        let text = "# Do not edit this file manually!\n\
                    # It will be overwritten by the ALTER SYSTEM command.\n\
                    timescaledb.telemetry_level = 'off'\n\
                    shared_preload_libraries = 'timescaledb, pg_stat_statements'\n\
                    search_path = '\"$user\", public'\n\
                    log_line_prefix = 'it''s \\\\d '\n\
                    work_mem = '64MB'\n\
                    work_mem = '128MB'\n";
        assert_eq!(
            parse_auto_conf(text),
            vec![
                ("timescaledb.telemetry_level".into(), "off".into()),
                (
                    "shared_preload_libraries".into(),
                    "timescaledb, pg_stat_statements".into()
                ),
                ("search_path".into(), "\"$user\", public".into()),
                ("log_line_prefix".into(), "it's \\d ".into()),
                ("work_mem".into(), "128MB".into()),
            ]
        );
        assert!(parse_auto_conf("").is_empty());
    }

    #[test]
    fn list_settings_get_one_literal_per_element() {
        assert_eq!(
            split_setting_list("timescaledb, pg_stat_statements"),
            vec!["timescaledb", "pg_stat_statements"]
        );
        assert_eq!(
            split_setting_list("\"$user\", public, \"a,\"\"b\""),
            vec!["$user", "public", "a,\"b"]
        );
        assert!(split_setting_list("").is_empty());
        assert_eq!(
            alter_system("shared_preload_libraries", "timescaledb,pg_stat_statements"),
            "ALTER SYSTEM SET shared_preload_libraries = 'timescaledb', 'pg_stat_statements';"
        );
        assert_eq!(
            alter_system("search_path", "\"$user\", public"),
            "ALTER SYSTEM SET search_path = '$user', 'public';"
        );
        assert_eq!(
            alter_system("timescaledb.telemetry_level", "off"),
            "ALTER SYSTEM SET timescaledb.telemetry_level = 'off';"
        );
        assert_eq!(
            alter_system("work_mem", "64MB"),
            "ALTER SYSTEM SET work_mem = '64MB';"
        );
        assert_eq!(
            alter_system_list("shared_preload_libraries", &[]),
            "ALTER SYSTEM SET shared_preload_libraries = '';"
        );
    }

    #[test]
    fn only_the_libraries_the_new_image_has_are_preloaded() {
        let files: BTreeSet<String> = ["timescaledb.so", "pg_stat_statements.so", "plpgsql.so"]
            .iter()
            .map(|s| s.to_string())
            .collect();
        let (kept, missing) = available_libraries(
            "timescaledb, pg_stat_statements,$libdir/plpgsql, pg_cron",
            &files,
        );
        assert_eq!(
            kept,
            vec!["timescaledb", "pg_stat_statements", "$libdir/plpgsql"]
        );
        assert_eq!(missing, vec!["pg_cron"]);
    }

    #[test]
    fn quoting_and_file_names() {
        assert_eq!(quote_ident("a\"b"), "\"a\"\"b\"");
        assert_eq!(quote_literal("it's"), "'it''s'");
        assert_eq!(dump_file_name(0, "meerschaum"), "01-meerschaum.dump");
        assert_eq!(dump_file_name(11, "my db/x"), "12-my_db_x.dump");
    }
}
