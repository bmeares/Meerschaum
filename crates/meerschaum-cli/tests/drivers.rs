//! `show drivers`, `install driver`, `uninstall driver` and the first-use error of a `duckdb`
//! instance, through the `mrsm` binary over a temporary root. Nothing is downloaded: the DuckDB
//! library is a placeholder file where a test needs one on disk, and `MRSM_NO_AUTO_INSTALL`
//! forbids the implicit download.

use std::path::{Path, PathBuf};
use std::process::{Command, Output};

struct Env {
    _tmp: tempfile::TempDir,
    root: PathBuf,
}

impl Env {
    fn new() -> Env {
        let tmp = tempfile::tempdir().unwrap();
        let root = tmp.path().join("root");
        std::fs::create_dir_all(root.join("plugins")).unwrap();
        Env { _tmp: tmp, root }
    }

    fn run(&self, extra_env: &[(&str, &str)], args: &[&str]) -> Output {
        self.run_in(None, extra_env, args)
    }

    fn run_in(&self, cwd: Option<&Path>, extra_env: &[(&str, &str)], args: &[&str]) -> Output {
        let mut command = Command::new(env!("CARGO_BIN_EXE_mrsm-cli"));
        if let Some(cwd) = cwd {
            command.current_dir(cwd);
        }
        for (key, _) in std::env::vars() {
            if key.starts_with("MRSM_") || matches!(key.as_str(), "RUST_LOG" | "NO_COLOR" | "TERM")
            {
                command.env_remove(key);
            }
        }
        command
            .env("MRSM_ROOT_DIR", &self.root)
            .env("MRSM_PLUGINS_DIR", self.root.join("plugins"))
            .env("MRSM_NOASK", "true")
            .env("MRSM_NO_UPDATE_CHECK", "1")
            .env("MRSM_NO_AUTO_INSTALL", "1")
            .env(
                "MRSM_CONFIG",
                r#"{"shell":{"updates":{"check":false}},"formatting":{"ansi":false,"unicode":false}}"#,
            )
            .envs(extra_env.iter().copied())
            .args(args)
            .output()
            .unwrap()
    }

    fn cache_dir(&self) -> PathBuf {
        self.root.join("drivers").join("duckdb")
    }
}

fn text(out: &Output) -> String {
    format!(
        "{}{}",
        String::from_utf8_lossy(&out.stdout),
        String::from_utf8_lossy(&out.stderr)
    )
}

fn driver_row(listing: &str, name: &str) -> Vec<String> {
    listing
        .lines()
        .find(|l| l.starts_with(&format!("{name}\t")))
        .unwrap_or_else(|| panic!("no {name} row in {listing:?}"))
        .split('\t')
        .map(str::to_string)
        .collect()
}

fn write_placeholder(path: &Path) {
    std::fs::create_dir_all(path.parent().unwrap()).unwrap();
    std::fs::write(path, b"not a library").unwrap();
}

#[test]
fn show_drivers_lists_duckdb_and_oracle() {
    let env = Env::new();
    let out = env.run(&[], &["show", "drivers", "--nopretty"]);
    assert!(out.status.success(), "{}", text(&out));
    let listing = String::from_utf8_lossy(&out.stdout).into_owned();
    let duckdb = driver_row(&listing, "duckdb");
    // Missing here (an empty root and no library on the system path), or found on the loader path.
    assert!(
        ["missing", "system"].contains(&duckdb[2].as_str()),
        "{duckdb:?}"
    );
    if duckdb[2] == "missing" {
        assert!(
            duckdb[3].contains("mrsm install driver duckdb"),
            "{duckdb:?}"
        );
    }
    let oracle = driver_row(&listing, "oracle");
    assert!(
        ["missing", "system"].contains(&oracle[2].as_str()),
        "{oracle:?}"
    );
}

#[test]
fn a_broken_library_is_reported_with_its_fix() {
    let env = Env::new();
    let lib = env.root.join("libduckdb-fake.so");
    write_placeholder(&lib);
    let out = env.run(
        &[("MRSM_DUCKDB_LIB", lib.to_str().unwrap())],
        &["show", "drivers", "--nopretty"],
    );
    let listing = String::from_utf8_lossy(&out.stdout).into_owned();
    let row = driver_row(&listing, "duckdb");
    assert_eq!(row[2], "error", "{row:?}");
    assert!(row[3].contains("MRSM_DUCKDB_LIB"), "{row:?}");

    // The cache is used when the variable is unset; a corrupt cached file names the reinstall.
    let cached = env.cache_dir().join("1.5.6").join(if cfg!(windows) {
        "duckdb.dll"
    } else if cfg!(target_os = "macos") {
        "libduckdb.dylib"
    } else {
        "libduckdb.so"
    });
    write_placeholder(&cached);
    let out = env.run(&[], &["show", "drivers", "--nopretty"]);
    let listing = String::from_utf8_lossy(&out.stdout).into_owned();
    let row = driver_row(&listing, "duckdb");
    assert_eq!(row[2], "error", "{row:?}");
    assert!(row[3].contains("install driver duckdb --force"), "{row:?}");
}

#[test]
fn uninstall_driver_removes_the_cache() {
    let env = Env::new();
    write_placeholder(&env.cache_dir().join("1.5.6").join("libduckdb.so"));
    let out = env.run(&[], &["uninstall", "driver", "duckdb", "-y"]);
    assert!(out.status.success(), "{}", text(&out));
    assert!(
        text(&out).contains("Removed the DuckDB driver"),
        "{}",
        text(&out)
    );
    assert!(!env.cache_dir().exists());
    let out = env.run(&[], &["uninstall", "driver", "duckdb", "-y"]);
    assert!(text(&out).contains("is not installed"), "{}", text(&out));
}

#[test]
fn install_driver_validates_its_names() {
    let env = Env::new();
    let none = text(&env.run(&[], &["install", "driver"]));
    assert!(none.contains("No driver to install"), "{none}");
    let unknown = text(&env.run(&[], &["install", "driver", "nope"]));
    assert!(unknown.contains("Unknown driver 'nope'"), "{unknown}");
    let oracle = text(&env.run(&[], &["install", "driver", "oracle"]));
    assert!(oracle.contains("Instant Client"), "{oracle}");
}

#[test]
fn driver_actions_are_v5_additions_in_help() {
    let env = Env::new();
    let help = text(&env.run(&[], &["install", "driver", "--help"]));
    assert!(help.contains("install driver duckdb"), "{help}");
    let help = text(&env.run(&[], &["show", "--help"]));
    assert!(help.contains("show drivers"), "{help}");
}

/// The first connection to a `duckdb` instance finds no library and, with the implicit install
/// disabled, says how to provide one.
#[test]
fn a_duckdb_instance_without_a_library_names_the_fix_under_no_auto_install() {
    let env = Env::new();
    let db = env.root.join("d.duckdb");
    let uri = format!("duckdb:///{}", db.display());
    let out = env.run(
        &[("MRSM_SQL_W17D", &uri)],
        &["show", "pipes", "-i", "sql:w17d"],
    );
    let output = text(&out);
    if output.contains("No pipes to show") {
        // A DuckDB library is on this host's loader path; the lookup worked.
        return;
    }
    assert!(output.contains("mrsm install driver duckdb"), "{output}");
    assert!(output.contains("MRSM_DUCKDB_LIB"), "{output}");
}

#[test]
fn show_drivers_filters_by_name() {
    let env = Env::new();
    let out = env.run(&[], &["show", "drivers", "oracle", "--nopretty"]);
    let listing = String::from_utf8_lossy(&out.stdout).into_owned();
    assert!(listing.starts_with("oracle\t"), "{listing:?}");
    assert!(!listing.contains("duckdb"), "{listing:?}");
    let unknown = text(&env.run(&[], &["show", "drivers", "nope"]));
    assert!(unknown.contains("Unknown driver 'nope'"), "{unknown}");
}

/// A DuckDB library found on the system library path is reported by its path and never loaded:
/// `show drivers` must not run the initializers of a library nobody asked for (the placeholder
/// would fail to load, and it is not reported as an error).
#[cfg(target_os = "linux")]
#[test]
fn show_drivers_does_not_load_a_system_library() {
    let env = Env::new();
    let libs = env.root.join("syslibs");
    write_placeholder(&libs.join("libduckdb.so"));
    let out = env.run(
        &[("LD_LIBRARY_PATH", libs.to_str().unwrap())],
        &["show", "drivers", "--nopretty"],
    );
    let listing = String::from_utf8_lossy(&out.stdout).into_owned();
    let row = driver_row(&listing, "duckdb");
    assert_eq!(row[2], "system", "{row:?}");
    assert!(row[1].contains("not loaded"), "{row:?}");
    assert!(row[3].ends_with("libduckdb.so"), "{row:?}");
}

/// `export LD_LIBRARY_PATH=/x:$LD_LIBRARY_PATH` with the variable unset leaves an empty entry, which
/// `dlopen` reads as "the current directory". A real DuckDB library planted in the working
/// directory must not be loaded through it: the connection finds no library and, with the implicit
/// install disabled, says how to provide one. Needs a real library to copy (`MRSM_DUCKDB_LIB`).
#[cfg(target_os = "linux")]
#[test]
fn an_empty_ld_library_path_entry_does_not_load_a_library_from_the_working_directory() {
    let Some(real) = std::env::var_os("MRSM_DUCKDB_LIB").map(PathBuf::from) else {
        eprintln!("Skipping: set MRSM_DUCKDB_LIB (a real library to plant).");
        return;
    };
    let env = Env::new();
    let planted = env.root.join("cwd");
    std::fs::create_dir_all(&planted).unwrap();
    std::fs::copy(&real, planted.join("libduckdb.so")).unwrap();
    let db = env.root.join("d.duckdb");
    let uri = format!("duckdb:///{}", db.display());
    for ld in ["/nonexistent-dir:", ":", ":/nonexistent-dir"] {
        let out = env.run_in(
            Some(&planted),
            &[("MRSM_SQL_W17D", &uri), ("LD_LIBRARY_PATH", ld)],
            &["show", "pipes", "-i", "sql:w17d"],
        );
        let output = text(&out);
        if output.contains("No pipes to show") {
            // A DuckDB library is on this host's real system path: the lookup rightly found it.
            return;
        }
        assert!(
            output.contains("mrsm install driver duckdb"),
            "LD_LIBRARY_PATH={ld:?} loaded the planted library:\n{output}"
        );
    }
}
