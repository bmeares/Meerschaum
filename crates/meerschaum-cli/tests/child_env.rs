//! 5.1: processes Meerschaum starts that are not Meerschaum (`sh` / `!command`, `os`, the editor)
//! inherit no `MRSM_CONFIG` / `MRSM_PATCH` / `_FILE` form / `MRSM_<TYPE>_<LABEL>` variable
//! (`meerschaum_core::child_env`), while the rest of the environment (`MRSM_ROOT_DIR`, `PATH`)
//! is kept, and Meerschaum's own process keeps them.
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

#[test]
fn a_shell_command_inherits_no_secret() {
    let root = tempfile::tempdir().unwrap();
    let (code, stdout, stderr) = mrsm(root.path(), &["sh", "env"], &[]);
    assert_eq!(code, 0, "{stdout}{stderr}");
    assert_scrubbed(&stdout, root.path());
    // Meerschaum itself still reads them (`MRSM_PATCH` here).
    let (_, stdout, _) = mrsm(
        root.path(),
        &["show", "config", "tpr_child", "--nopretty"],
        &[],
    );
    assert!(stdout.contains("password"), "{stdout}");
}

#[test]
fn the_os_action_inherits_no_secret() {
    let root = tempfile::tempdir().unwrap();
    let (code, stdout, stderr) = mrsm(root.path(), &["os", "env"], &[]);
    assert_eq!(code, 0, "{stdout}{stderr}");
    assert_scrubbed(&stdout, root.path());
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
