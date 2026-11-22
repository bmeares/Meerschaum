//! `sql`: queries routed to read or exec, and the interactive clients (Rainfrog, the Python
//! clients, the plain client) with the password kept out of the command line.

use super::*;

/// `sql <label> "CREATE VIEW ... AS SELECT ..."`: the text contains `select`, so v4 routes it to `read`;
/// the statement runs, `read()` finds no result set and the action prints `None` and succeeds.
#[test]
fn sql_runs_a_create_view_routed_to_read_and_prints_none() {
    let home = Home::new();
    let created = home.sql("CREATE VIEW v AS SELECT 1 AS a");
    assert_eq!(created.code, 0, "{}", created.text);
    assert!(created.text.contains("None"), "{}", created.text);
    assert!(
        created.text.contains("does not return rows"),
        "{}",
        created.text
    );
    let rows = home.sql("SELECT a FROM v");
    assert_eq!(rows.code, 0, "{}", rows.text);
    assert!(rows.text.contains("|   1 |"), "{}", rows.text);
}

#[test]
fn sql_reads_execs_and_prints_none_for_a_bad_query() {
    let home = Home::new();
    let created = home.sql("CREATE TABLE t (a int, b text)");
    assert_eq!(created.code, 0, "{}", created.text);
    assert!(created.text.contains("Successfully executed query"));

    // An empty table: tabulate draws the separator without alignment colons.
    let empty = home.sql("SELECT * FROM t");
    assert_eq!(empty.code, 0, "{}", empty.text);
    assert!(empty.text.contains("|-----|-----|"), "{}", empty.text);
    assert!(
        empty.text.contains("[0 rows x 2 columns]"),
        "{}",
        empty.text
    );

    assert_eq!(home.sql("INSERT INTO t VALUES (1, 'x')").code, 0);
    let rows = home.sql("SELECT * FROM t");
    assert!(rows.text.contains("|   a | b   |"), "{}", rows.text);
    assert!(rows.text.contains("|   1 | x   |"), "{}", rows.text);
    assert!(rows.text.contains("[1 row x 2 columns]"), "{}", rows.text);

    let json = home.run(&["sql", "local", "SELECT a FROM t", "--nopretty"]);
    assert!(
        json.text.contains(r#"{"columns":["a"],"data":[[1]]}"#),
        "{}",
        json.text
    );

    // v4 logs the error and prints `None`; the action itself succeeds.
    let bad = home.sql("SELEC nothing");
    assert_eq!(bad.code, 0, "{}", bad.text);
    assert!(bad.text.contains("None"), "{}", bad.text);
    assert!(bad.text.contains("syntax error"), "{}", bad.text);

    // `sql sql:local <query>` accepts full keys too, and an unknown label fails loudly.
    let keyed = home.run(&["sql", "sql:local", "SELECT 2 AS two"]);
    assert!(keyed.text.contains("two"), "{}", keyed.text);
    let unknown = home.run(&["sql", "nonexistent", "SELECT 1"]);
    no_worker(&unknown);
}

#[test]
fn sql_passes_the_password_in_the_environment_not_on_the_command_line() {
    let bin = tempfile::tempdir().unwrap();
    for (program, variable) in [("pgcli", "PGPASSWORD"), ("mycli", "MYSQL_PWD")] {
        let path = bin.path().join(program);
        std::fs::write(
            &path,
            format!("#!/bin/sh\necho \"ARGV: $@\"\necho \"{variable}=${variable}\"\n"),
        )
        .unwrap();
        use std::os::unix::fs::PermissionsExt;
        std::fs::set_permissions(&path, std::fs::Permissions::from_mode(0o755)).unwrap();
    }
    let path = format!(
        "{}:{}",
        bin.path().display(),
        std::env::var("PATH").unwrap_or_default()
    );
    let home = Home::new()
        .with_env("PATH", path)
        .with_env(
            "MRSM_PATCH",
            r#"{"system":{"connectors":{"sql":{"cli":{"postgresql":"pgcli","mysql":"mycli"}}}}}"#,
        )
        .with_env("MRSM_SQL_PG", "postgresql://alice:s%40cret@127.0.0.1:1/db")
        .with_env("MRSM_SQL_MY", "mysql://bob:p%2Bw@127.0.0.1:1/db");
    // The databases are down: the client is started anyway and reports its own errors.
    let pg = home.run(&["sql", "pg"]);
    assert!(
        pg.text.contains("ARGV: postgresql://alice@127.0.0.1:1/db"),
        "{}",
        pg.text
    );
    assert!(pg.text.contains("PGPASSWORD=s@cret"), "{}", pg.text);
    assert!(
        !pg.text.contains("ARGV: postgresql://alice:"),
        "{}",
        pg.text
    );
    let my = home.run(&["sql", "my"]);
    assert!(
        my.text.contains("ARGV: mysql://bob@127.0.0.1:1/db"),
        "{}",
        my.text
    );
    assert!(my.text.contains("MYSQL_PWD=p+w"), "{}", my.text);
}

#[test]
fn sql_prefers_rainfrog_and_nopretty_forces_plain_client() {
    use std::os::unix::fs::PermissionsExt;
    let bin = tempfile::tempdir().unwrap();
    for (name, body) in [
        (
            "rainfrog",
            "echo RAINFROG_ARGS: \"$@\"; case \"$DATABASE_URL\" in *s%40cret*) echo PRIVATE_URL_RECEIVED;; esac",
        ),
        ("psql", "echo PLAIN_ARGS: \"$@\""),
        ("pgcli", "echo PYTHON_CLIENT"),
    ] {
        let program = bin.path().join(name);
        std::fs::write(&program, format!("#!/bin/sh\n{body}\n")).unwrap();
        std::fs::set_permissions(&program, std::fs::Permissions::from_mode(0o755)).unwrap();
    }
    let home = Home::new()
        .with_env("PATH", bin.path().display().to_string())
        .with_env(
            "MRSM_SQL_PG",
            "postgresql://alice:s%40cret@127.0.0.1:1/db?sslmode=require",
        );
    let pretty = home.run(&["sql", "pg"]);
    assert_eq!(pretty.code, 0, "{}", pretty.text);
    assert!(
        pretty.text.contains("RAINFROG_ARGS: --driver postgres"),
        "{}",
        pretty.text
    );
    assert!(
        pretty.text.contains("PRIVATE_URL_RECEIVED"),
        "{}",
        pretty.text
    );
    assert!(!pretty.text.contains("PYTHON_CLIENT"), "{}", pretty.text);
    assert!(!pretty.text.contains("s%40cret"), "{}", pretty.text);
    let plain = home.run(&["sql", "pg", "--nopretty"]);
    assert_eq!(plain.code, 0, "{}", plain.text);
    assert!(
        plain.text.contains("PLAIN_ARGS: postgresql://alice@"),
        "{}",
        plain.text
    );
    assert!(!plain.text.contains("RAINFROG_ARGS"), "{}", plain.text);
    assert!(!plain.text.contains("s%40cret"), "{}", plain.text);
}

/// A service's reduced `PATH` (`/usr/local/bin:/usr/bin:/bin`) hid every client the user
/// had installed; `sql` also searches `~/.cargo/bin`, `~/.local/bin` and Meerschaum's virtual
/// environments, where `mrsm install packages pgcli` puts its script beside the packages.
#[test]
fn sql_finds_clients_outside_a_reduced_path() {
    use std::os::unix::fs::PermissionsExt;
    let write = |path: &std::path::Path, body: &str| {
        std::fs::create_dir_all(path.parent().unwrap()).unwrap();
        std::fs::write(path, format!("#!/bin/sh\n{body}\n")).unwrap();
        std::fs::set_permissions(path, std::fs::Permissions::from_mode(0o755)).unwrap();
    };
    let empty = tempfile::tempdir().unwrap();
    let reduced = empty.path().display().to_string();

    // `pip install --user pgcli`: `~/.local/bin`.
    let home = Home::new()
        .with_env("PATH", reduced.clone())
        .with_env("MRSM_SQL_PG", "postgresql://alice@127.0.0.1:1/db");
    write(
        &home.path().join(".local/bin/pgcli"),
        "echo LOCAL_PGCLI: \"$@\"",
    );
    let local = home.run(&["sql", "pg"]);
    assert_eq!(local.code, 0, "{}", local.text);
    assert!(
        local
            .text
            .contains("LOCAL_PGCLI: postgresql://alice@127.0.0.1:1/db"),
        "{}",
        local.text
    );
    // `cargo install rainfrog`: `~/.cargo/bin` comes before the Python client.
    write(
        &home.path().join(".cargo/bin/rainfrog"),
        "echo CARGO_RAINFROG: \"$@\"",
    );
    let cargo = home.run(&["sql", "pg"]);
    assert!(
        cargo.text.contains("CARGO_RAINFROG: --driver postgres"),
        "{}",
        cargo.text
    );

    // `mrsm install packages pgcli`: the script beside the venv's packages runs with the venv's
    // interpreter and those packages on its path.
    let venv_home = Home::new()
        .with_env("PATH", reduced.clone())
        .with_env("MRSM_SQL_PG", "postgresql://alice@127.0.0.1:1/db");
    let venv = venv_home.path().join("venvs").join("mrsm");
    let site = venv.join("lib").join("python3.99").join("site-packages");
    write(
        &venv.join("bin/python"),
        "echo VENV_PYTHON: \"$@\" PYTHONPATH=$PYTHONPATH",
    );
    write(&site.join("bin/pgcli"), "exit 9");
    let installed = venv_home.run(&["sql", "pg"]);
    assert_eq!(installed.code, 0, "{}", installed.text);
    let expected = format!(
        "VENV_PYTHON: {} postgresql://alice@127.0.0.1:1/db PYTHONPATH={}",
        site.join("bin/pgcli").display(),
        site.display()
    );
    assert!(installed.text.contains(&expected), "{}", installed.text);

    // Nothing anywhere, and nothing can be asked: the message names what was searched and how
    // to install a client.
    let none = Home::new()
        .with_env("PATH", reduced.clone())
        .with_env("MRSM_SQL_PG", "postgresql://alice@127.0.0.1:1/db");
    let missing = none.run(&["sql", "pg", "--noask"]);
    assert_eq!(missing.code, 1, "{}", missing.text);
    assert!(
        missing.text.contains(&format!("on PATH ({reduced})")),
        "{}",
        missing.text
    );
    assert!(
        missing.text.contains("mrsm install packages pgcli"),
        "{}",
        missing.text
    );
}

/// In a terminal with no client anywhere, `sql` offers to install the flavor's Python
/// client into the `mrsm` virtual environment instead of only failing; declining fails with the
/// install command.
#[test]
fn sql_offers_to_install_a_client_in_a_terminal() {
    use portable_pty::{CommandBuilder, PtySize, native_pty_system};
    let home = Home::new();
    let empty = tempfile::tempdir().unwrap();
    let pair = native_pty_system()
        .openpty(PtySize {
            rows: 24,
            cols: 200,
            pixel_width: 0,
            pixel_height: 0,
        })
        .unwrap();
    let mut cmd = CommandBuilder::new(env!("CARGO_BIN_EXE_mrsm-cli"));
    cmd.env_clear();
    cmd.env("PATH", empty.path());
    cmd.env("HOME", home.path());
    cmd.env("MRSM_ROOT_DIR", home.path());
    cmd.env("MRSM_PLUGINS_DIR", home.plugins.path());
    cmd.env("MRSM_PYTHON", home.path().join("no-python"));
    cmd.env("MRSM_SQL_PG", "postgresql://alice@127.0.0.1:1/db");
    cmd.args(["sql", "pg"]);
    let mut child = pair.slave.spawn_command(cmd).unwrap();
    drop(pair.slave);
    let mut reader = pair.master.try_clone_reader().unwrap();
    let mut writer = pair.master.take_writer().unwrap();
    let output = std::sync::Arc::new(std::sync::Mutex::new(Vec::<u8>::new()));
    let sink = output.clone();
    std::thread::spawn(move || {
        let mut buffer = [0u8; 4096];
        while let Ok(n) = std::io::Read::read(&mut reader, &mut buffer) {
            if n == 0 {
                break;
            }
            sink.lock().unwrap().extend_from_slice(&buffer[..n]);
        }
    });
    let text = |output: &std::sync::Arc<std::sync::Mutex<Vec<u8>>>| {
        strip_ansi(&String::from_utf8_lossy(&output.lock().unwrap()))
    };
    let deadline = Instant::now() + Duration::from_secs(30);
    while !text(&output).contains("Install pgcli into the Meerschaum virtual environment now?") {
        assert!(
            Instant::now() < deadline,
            "no install offer:\n{}",
            text(&output)
        );
        std::thread::sleep(Duration::from_millis(50));
    }
    std::io::Write::write_all(&mut writer, b"n\r").unwrap();
    loop {
        if child.try_wait().unwrap().is_some() {
            break;
        }
        assert!(
            Instant::now() < deadline,
            "sql did not end:\n{}",
            text(&output)
        );
        std::thread::sleep(Duration::from_millis(50));
    }
    std::thread::sleep(Duration::from_millis(200));
    let shown = text(&output);
    assert!(shown.contains("Cannot find a SQL client"), "{shown}");
    assert!(shown.contains("mrsm install packages pgcli"), "{shown}");
}

/// A Python client script whose interpreter cannot import its package (a `pgcli` left in
/// `~/.local/bin` by an older Python: `ModuleNotFoundError: No module named 'pgcli'` in a web
/// terminal) is skipped with a warning, and the next client is used.
#[test]
fn sql_skips_a_python_client_that_cannot_import_its_package() {
    use std::os::unix::fs::PermissionsExt;
    let python = ["/usr/bin/python3", "/usr/local/bin/python3", "/bin/python3"]
        .into_iter()
        .find(|p| std::path::Path::new(p).is_file())
        .expect("a python3 interpreter");
    let stale = tempfile::tempdir().unwrap();
    let plain = tempfile::tempdir().unwrap();
    let script = stale.path().join("pgcli");
    std::fs::write(
        &script,
        format!(
            "#!{python}\n# -*- coding: utf-8 -*-\nimport re\nimport sys\n\
             from _mrsm_missing_pgcli.main import cli\nsys.exit(cli())\n"
        ),
    )
    .unwrap();
    std::fs::set_permissions(&script, std::fs::Permissions::from_mode(0o755)).unwrap();
    let psql = plain.path().join("psql");
    std::fs::write(&psql, "#!/bin/sh\nexit 7\n").unwrap();
    std::fs::set_permissions(&psql, std::fs::Permissions::from_mode(0o755)).unwrap();
    let path = format!("{}:{}", stale.path().display(), plain.path().display());
    let home = Home::new()
        .with_env("PATH", path)
        .with_env("MRSM_SQL_PG", "postgresql://alice@127.0.0.1:1/db");
    let result = home.run(&["sql", "pg"]);
    assert_eq!(result.code, 1, "{}", result.text);
    assert!(
        result.text.contains("SQL client exited with status 7"),
        "the plain client after the broken one runs: {}",
        result.text
    );
    assert!(
        result.text.contains("Skipping pgcli at")
            && result.text.contains("cannot import `_mrsm_missing_pgcli`"),
        "{}",
        result.text
    );
    assert!(
        !result.text.contains("ModuleNotFoundError"),
        "{}",
        result.text
    );
}

#[test]
fn sql_propagates_interactive_client_failure() {
    use std::os::unix::fs::PermissionsExt;
    let bin = tempfile::tempdir().unwrap();
    let program = bin.path().join("rainfrog");
    std::fs::write(&program, "#!/bin/sh\nexit 7\n").unwrap();
    std::fs::set_permissions(&program, std::fs::Permissions::from_mode(0o755)).unwrap();
    let home = Home::new()
        .with_env("PATH", bin.path().display().to_string())
        .with_env("MRSM_SQL_PG", "postgresql://alice@127.0.0.1:1/db");
    let result = home.run(&["sql", "pg"]);
    assert_eq!(result.code, 1, "{}", result.text);
    assert!(
        result.text.contains("SQL client exited with status 7"),
        "{}",
        result.text
    );
}
