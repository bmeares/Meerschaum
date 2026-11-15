//! 5.1: incidental processes Meerschaum starts that are not Meerschaum (the editor, interpreter
//! probes) inherit no `MRSM_CONFIG` / `MRSM_PATCH` / `_FILE` form / `MRSM_<TYPE>_<LABEL>`
//! variable (`meerschaum_core::child_env`), while the rest of the environment (`MRSM_ROOT_DIR`,
//! `PATH`) is kept. Commands the user asks for (`sh`, `os`, `!command`, an unknown word) keep the
//! whole environment and can run `mrsm` again with the parent's configuration.
#![cfg(unix)]

use std::process::Command;

const SECRET: &str = "child-s3cret-xyz";

fn mrsm(root: &std::path::Path, args: &[&str], extra: &[(&str, &str)]) -> (i32, String, String) {
    let plugins = root.join("plugins");
    std::fs::create_dir_all(&plugins).unwrap();
    let mut command = Command::new(env!("CARGO_BIN_EXE_mrsm-cli"));
    command
        .args(args)
        .env_remove("MRSM_SQL_MAIN")
        .env_remove("MRSM_CONFIG")
        .env_remove("MRSM_CONFIG_FILE")
        .env("MRSM_ROOT_DIR", root)
        .env("MRSM_PLUGINS_DIR", &plugins)
        .env("MRSM_NOASK", "true")
        .env(
            "MRSM_SQL_TPRCHILD",
            format!("sqlite:///{}/child.db?password={SECRET}", root.display()),
        )
        .env(
            "MRSM_PATCH",
            format!(r#"{{"tpr_child": {{"password": "{SECRET}"}}}}"#),
        )
        .env(
            "MRSM_CONFIG",
            r#"{"tpr_config": {"marker": "from-mrsm-config"}}"#,
        )
        .env("MRSM_PATCH_FILE", "")
        .stdin(std::process::Stdio::null());
    for (k, v) in extra {
        command.env(k, v);
    }
    let output = command.output().unwrap();
    (
        output.status.code().unwrap_or(-1),
        String::from_utf8_lossy(&output.stdout).into_owned(),
        String::from_utf8_lossy(&output.stderr).into_owned(),
    )
}

fn assert_scrubbed(env_text: &str, root: &std::path::Path) {
    assert!(!env_text.contains(SECRET), "{env_text}");
    assert!(!env_text.contains("MRSM_SQL_TPRCHILD="), "{env_text}");
    assert!(!env_text.contains("MRSM_PATCH="), "{env_text}");
    assert!(
        env_text.contains(&format!("MRSM_ROOT_DIR={}", root.display())),
        "{env_text}"
    );
    assert!(env_text.contains("PATH="), "{env_text}");
}

fn assert_kept(env_text: &str) {
    assert!(env_text.contains("MRSM_SQL_TPRCHILD="), "{env_text}");
    assert!(env_text.contains("MRSM_PATCH="), "{env_text}");
    assert!(env_text.contains("MRSM_CONFIG="), "{env_text}");
    assert!(env_text.contains(SECRET), "{env_text}");
}

/// A command the user asked for keeps `MRSM_CONFIG`, `MRSM_PATCH` and the connector variable.
#[test]
fn user_commands_keep_the_whole_environment() {
    let root = tempfile::tempdir().unwrap();
    for args in [
        &["sh", "env"][..],
        &["os", "env"],
        &["!env"],
        // A word that is not an action falls through to the system shell.
        &["printenv"],
    ] {
        let (code, stdout, stderr) = mrsm(root.path(), args, &[]);
        assert_eq!(code, 0, "{args:?}: {stdout}{stderr}");
        assert_kept(&stdout);
    }
}

/// A shell command that runs `mrsm` again reads the parent's patches and connector variable.
#[test]
fn a_shell_command_can_run_mrsm_with_the_parents_configuration() {
    let root = tempfile::tempdir().unwrap();
    let bin = env!("CARGO_BIN_EXE_mrsm-cli");
    let script = root.path().join("again.sh");
    std::fs::write(
        &script,
        format!(
            "#!/bin/sh\nset -e\n{bin} show config tpr_child --nopretty\n\
             {bin} show config tpr_config --nopretty\n\
             {bin} show connectors sql:tprchild --nopretty\n"
        ),
    )
    .unwrap();
    use std::os::unix::fs::PermissionsExt;
    std::fs::set_permissions(&script, std::fs::Permissions::from_mode(0o755)).unwrap();
    let script = script.to_string_lossy().into_owned();
    let (code, stdout, stderr) = mrsm(root.path(), &["sh", &script], &[]);
    assert_eq!(code, 0, "{stdout}{stderr}");
    assert!(stdout.contains(SECRET), "{stdout}{stderr}");
    assert!(stdout.contains("from-mrsm-config"), "{stdout}{stderr}");
    assert!(stdout.contains("child.db"), "{stdout}{stderr}");
}

#[test]
fn the_editor_inherits_no_secret() {
    let root = tempfile::tempdir().unwrap();
    let out = root.path().join("editor.env");
    let editor = root.path().join("editor.sh");
    std::fs::write(&editor, format!("#!/bin/sh\nenv > {}\n", out.display())).unwrap();
    use std::os::unix::fs::PermissionsExt;
    std::fs::set_permissions(&editor, std::fs::Permissions::from_mode(0o755)).unwrap();
    let editor = editor.to_string_lossy().into_owned();
    let (_, stdout, stderr) = mrsm(
        root.path(),
        &["edit", "config", "meerschaum"],
        &[("EDITOR", &editor), ("VISUAL", &editor)],
    );
    let text = std::fs::read_to_string(&out)
        .unwrap_or_else(|e| panic!("the editor did not run ({e}):\n{stdout}{stderr}"));
    assert_scrubbed(&text, root.path());
}

/// An interpreter probe (here `doctor`'s probe of `MRSM_PYTHON`) is incidental: no secrets.
#[test]
fn an_interpreter_probe_inherits_no_secret() {
    let root = tempfile::tempdir().unwrap();
    let out = root.path().join("python.env");
    let python = root.path().join("python.sh");
    std::fs::write(
        &python,
        format!("#!/bin/sh\nenv > {}\nexit 1\n", out.display()),
    )
    .unwrap();
    use std::os::unix::fs::PermissionsExt;
    std::fs::set_permissions(&python, std::fs::Permissions::from_mode(0o755)).unwrap();
    let python = python.to_string_lossy().into_owned();
    let (_, stdout, stderr) = mrsm(
        root.path(),
        &["install", "packages", "tpr-child-env-probe"],
        &[("MRSM_PYTHON", &python)],
    );
    let text = std::fs::read_to_string(&out)
        .unwrap_or_else(|e| panic!("the probe did not run ({e}):\n{stdout}{stderr}"));
    assert_scrubbed(&text, root.path());
}
