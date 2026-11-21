//! 5.1: the API server's sandbox probe. `mrsm __mrsm_team_sandbox_probe__ <mode> <canary> [max
//! ABI]` applies a team process's sandbox to the process started for it and prints the report:
//! the server decides from it whether Unix sockets are denied to team code (Landlock from Linux
//! 7.1, else the seccomp filter), and so whether web terminals may keep tmux.

use meerschaum_core::team_scope::sandbox::{
    self, Mode, Report, SOCKETS_BY_LANDLOCK, SOCKETS_BY_SECCOMP, SocketProbe,
};

fn probe(mode: &str, max_abi: Option<u32>) -> Report {
    let dir = tempfile::tempdir().unwrap();
    let canary = dir.path().join("canary");
    std::fs::write(&canary, b"x").unwrap();
    let mut command = std::process::Command::new(env!("CARGO_BIN_EXE_mrsm-cli"));
    command
        .arg(sandbox::PROBE_ARG)
        .arg(mode)
        .arg(&canary)
        .env_clear()
        .env("PATH", std::env::var_os("PATH").unwrap_or_default());
    if let Some(abi) = max_abi {
        command.arg(abi.to_string());
    }
    let out = command.output().unwrap();
    assert!(out.status.success(), "{out:?}");
    let stdout = String::from_utf8_lossy(&out.stdout);
    serde_json::from_str(stdout.lines().last().unwrap_or_default()).unwrap()
}

#[test]
fn the_probe_reports_how_unix_sockets_are_denied() {
    let support = sandbox::kernel_support();
    if !support.available() {
        let report = probe("auto", None);
        assert!(
            !report.enforced && !report.denies_unix_sockets(),
            "{report:?}"
        );
        eprintln!("No Landlock here: the probe reports nothing enforced.");
        return;
    }
    // "Filesystem only": the seccomp filter, proven by a refused socket.
    let report = probe("auto", Some(8));
    assert!(report.enforced, "{report:?}");
    assert_eq!(report.unix_sockets, SOCKETS_BY_SECCOMP, "{report:?}");
    assert_eq!(report.socket_probe, SocketProbe::Denied, "{report:?}");
    assert!(report.denies_unix_sockets() && report.verdict().is_ok());
    assert!(
        report
            .describe()
            .contains("Unix sockets are denied to team code by a seccomp filter"),
        "{}",
        report.describe()
    );
    // The kernel's own level.
    let report = probe("auto", None);
    assert!(report.denies_unix_sockets(), "{report:?}");
    let expected = if support.abi() >= sandbox::SOCKETS_ABI {
        SOCKETS_BY_LANDLOCK
    } else {
        SOCKETS_BY_SECCOMP
    };
    assert_eq!(report.unix_sockets, expected, "{report:?}");
    // `off`: nothing is applied and nothing is denied.
    assert!(
        !sandbox::probe_team_process(Mode::Off, None)
            .unwrap()
            .denies_unix_sockets()
    );
}
