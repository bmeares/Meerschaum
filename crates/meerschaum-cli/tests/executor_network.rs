//! The deployment resolver must exit before config or plugin discovery in the real CLI.

use std::process::{Command, Stdio};
use std::time::{Duration, Instant};

#[test]
fn native_relay_dns_child_does_not_open_configuration_or_plugins() {
    let parent = std::env::var_os("MRSM_ROOT_DIR")
        .map(std::path::PathBuf::from)
        .unwrap_or_else(|| {
            std::path::PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../../test_root")
        });
    std::fs::create_dir_all(&parent).unwrap();
    let root = tempfile::Builder::new()
        .prefix("relay-dns-")
        .tempdir_in(parent)
        .unwrap();
    let plugins = root.path().join("plugins");
    std::fs::create_dir(&plugins).unwrap();
    std::fs::write(
        plugins.join("probe.py"),
        "raise RuntimeError('relay DNS must never discover plugins')\n",
    )
    .unwrap();
    let config = root.path().join("config");
    std::fs::create_dir(&config).unwrap();
    // An early helper neither parses malformed owner configuration nor creates default config.
    std::fs::write(config.join("meerschaum.yaml"), "invalid: [unclosed\n").unwrap();
    let mut process = Command::new(env!("CARGO_BIN_EXE_mrsm-cli"))
        .args(["--_mrsm-resolve-executor-relays", "[\"localhost\"]"])
        .env_clear()
        .env("HOME", root.path())
        .env("MRSM_ROOT_DIR", root.path())
        .env("MRSM_PLUGINS_DIR", &plugins)
        .env("MRSM_NO_UPDATE_CHECK", "1")
        .stdin(Stdio::null())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .unwrap();
    let deadline = Instant::now() + Duration::from_secs(10);
    loop {
        if process.try_wait().unwrap().is_some() {
            break;
        }
        if Instant::now() >= deadline {
            let _ = process.kill();
            let _ = process.wait();
            panic!("The owned native relay resolver exceeded its deadline.");
        }
        std::thread::sleep(Duration::from_millis(10));
    }
    let result = process.wait_with_output().unwrap();
    assert!(
        result.status.success(),
        "{}",
        String::from_utf8_lossy(&result.stderr)
    );
    let addresses: std::collections::BTreeMap<String, Vec<std::net::IpAddr>> =
        serde_json::from_slice(&result.stdout).unwrap();
    assert_eq!(addresses.len(), 1);
    assert!(!addresses["localhost"].is_empty());
    assert!(
        addresses["localhost"]
            .iter()
            .all(std::net::IpAddr::is_loopback)
    );
    assert_eq!(std::fs::read_dir(root.path()).unwrap().count(), 2);
    assert_eq!(std::fs::read_dir(&config).unwrap().count(), 1);
    assert_eq!(
        std::fs::read_to_string(config.join("meerschaum.yaml")).unwrap(),
        "invalid: [unclosed\n"
    );
}
