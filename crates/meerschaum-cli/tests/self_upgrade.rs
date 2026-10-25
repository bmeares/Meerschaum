//! Upgrade ownership is exercised through an isolated, copied executable and offline installer.
#![cfg(unix)]
mod plugin_lifecycle_support;
use plugin_lifecycle_support::{Home, run_binary};
use std::{
    io::Write,
    os::unix::fs::PermissionsExt,
    process::{Command, Stdio},
};

#[test]
fn cargo_upgrade_uses_the_receipted_prefix_without_a_python_host() {
    for shell in [false, true] {
        let home = Home::new();
        let prefix = home.root.join("custom-cargo");
        let bin = prefix.join("bin");
        std::fs::create_dir_all(&bin).unwrap();
        let executable = bin.join("mrsm");
        std::fs::copy(env!("CARGO_BIN_EXE_mrsm-cli"), &executable).unwrap();
        std::fs::write(prefix.join(".crates.toml"), "[v1]\n'meerschaum 5.0.0-rc.11 (registry+https://github.com/rust-lang/crates.io-index)' = ['mrsm']\n").unwrap();
        let installer = bin.join("cargo");
        std::fs::write(
            &installer,
            r#"#!/bin/sh
set -eu
printf '%s\n' "$@" > "$MRSM_TEST_INSTALL_ARGS"
/bin/mv "$MRSM_TEST_INSTALL_PREFIX/bin/mrsm" "$MRSM_TEST_INSTALL_PREFIX/bin/old-mrsm"
printf '#!/bin/sh\nprintf "Meerschaum v5.0.1\\n"\n' > "$MRSM_TEST_INSTALL_PREFIX/bin/mrsm"
/bin/chmod +x "$MRSM_TEST_INSTALL_PREFIX/bin/mrsm"
"#,
        )
        .unwrap();
        std::fs::set_permissions(&installer, std::fs::Permissions::from_mode(0o755)).unwrap();
        let receipt = home.root.join("installer-args");
        let mut command = Command::new(&executable);
        let mut env = home.env();
        env.retain(|(key, _)| key != "PATH");
        if shell {
            command.arg("--shell");
        } else {
            command.args(["upgrade", "mrsm", "-y", "-A", "--version", "5.0.1"]);
        }
        command
            .env_clear()
            .envs(env)
            .env("PATH", &bin)
            .env("MRSM_TEST_INSTALL_PREFIX", &prefix)
            .env("MRSM_TEST_INSTALL_ARGS", &receipt)
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped());
        let mut child = command.spawn().unwrap();
        if shell {
            child
                .stdin
                .as_mut()
                .unwrap()
                .write_all(b"upgrade mrsm -y -A --version 5.0.1\nEOF\n")
                .unwrap();
        }
        drop(child.stdin.take());
        let output = child.wait_with_output().unwrap();
        let text = format!(
            "{}{}",
            String::from_utf8_lossy(&output.stdout),
            String::from_utf8_lossy(&output.stderr)
        );
        assert!(output.status.success(), "{text}");
        assert!(text.contains("Installed Meerschaum 5.0.1"), "{text}");
        let args = std::fs::read_to_string(receipt).unwrap();
        assert_eq!(
            args.lines().collect::<Vec<_>>(),
            [
                "install",
                "--locked",
                "--force",
                "--root",
                prefix.to_str().unwrap(),
                "--version",
                "5.0.1",
                "meerschaum"
            ]
        );
        assert!(!text.contains("Python host"), "{text}");
        if shell {
            assert!(
                String::from_utf8_lossy(&output.stdout)
                    .trim_end()
                    .ends_with("Meerschaum v5.0.1"),
                "{text}"
            );
        }
    }
}

#[test]
fn container_upgrade_never_runs_an_installer() {
    let home = Home::new();
    let mut env = home.env();
    env.push(("MRSM_RUNTIME".into(), "docker".into()));
    env.push(("MRSM_PYTHON".into(), "/nonexistent/python".into()));
    let run = run_binary(&["upgrade", "mrsm", "-y"], &env, None);
    assert!(!run.success);
    assert!(run.text().contains("Docker image"), "{}", run.text());
    assert!(!run.text().contains("Unable to select a published release"));
}
