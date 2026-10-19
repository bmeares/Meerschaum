//! The check every Python process the program starts (the worker host, the API host) runs before
//! it imports the `meerschaum` package, shared by the crates that start them.
//!
//! [`SCRIPT`] defines `_mrsm_guard()`; the starter prepends it to its `-c` program and calls it
//! first. It reads the package's version and worker protocol from the files (nothing is imported)
//! and exits with [`REFUSED_EXIT_CODE`] when the package cannot work with this program: another
//! major version, a copy that looks like v4, or another worker protocol. A different release of the
//! same major version starts, with one warning when [`WARN_ENV`] is set. The policy is a pure
//! function here too ([`judge`]), which `mrsm doctor` mirrors and the tests pin down.

/// The Python side of the check.
pub const SCRIPT: &str = include_str!("python_guard.py");

/// The worker protocol version both sides speak (`meerschaum/worker/__init__.py` repeats it, and
/// a test keeps the two equal). The worker announces it in `hello`.
pub const WORKER_PROTOCOL_VERSION: u32 = 1;

/// The exit code of a process that refused to start. Deliberately none of the codes Python
/// (1, 2), a shell (126 to 165) or uvicorn (3: `STARTUP_FAILURE`, a lifespan that raised or a
/// socket that cannot be bound) exits with, so that a host crash is never taken for a refusal.
/// `python_guard.py` repeats the number (a test keeps them equal).
pub const REFUSED_EXIT_CODE: i32 = 113;

/// Set (to anything) to let the guard print the same-major warning.
pub const WARN_ENV: &str = "MRSM_GUARD_WARN";

/// Set to the text that tells the user how to retry after fixing the install, when `mrsm reload`
/// does not reach the process (the API host of a running server).
pub const RETRY_HINT_ENV: &str = "MRSM_GUARD_RETRY_HINT";

/// The version the package must match, as the Python package writes it (PEP 440).
pub const EXPECT_VERSION_ENV: &str = "MRSM_WORKER_EXPECT_VERSION";

/// The protocol the package must speak.
pub const EXPECT_PROTOCOL_ENV: &str = "MRSM_WORKER_EXPECT_PROTOCOL";

/// This release as the Python package writes it (`5.0.0-rc.1` is `5.0.0rc1`).
pub fn expected_version() -> String {
    crate::version::parse_version(env!("CARGO_PKG_VERSION"))
        .map(|v| v.to_string())
        .unwrap_or_else(|_| env!("CARGO_PKG_VERSION").to_string())
}

/// The environment the guard reads.
pub fn expect_env() -> Vec<(String, String)> {
    vec![
        (EXPECT_VERSION_ENV.to_string(), expected_version()),
        (
            EXPECT_PROTOCOL_ENV.to_string(),
            WORKER_PROTOCOL_VERSION.to_string(),
        ),
    ]
}

/// The major version of a version string (`5.0.0rc1` is 5).
pub fn major(version: &str) -> Option<u64> {
    let digits: String = version
        .trim()
        .chars()
        .take_while(char::is_ascii_digit)
        .collect();
    digits.parse().ok()
}

/// What the guard decides about a package.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Verdict {
    /// The same release and protocol.
    Match,
    /// Another release of the same major version with the same protocol: starts, with a warning.
    OtherRelease,
    /// The package cannot be used: why.
    Refuse(Refusal),
}

/// Why a package is refused.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Refusal {
    /// Another major version.
    Major,
    /// A copy that looks like v4: no `worker` module and no readable version.
    LooksLikeV4,
    /// The worker protocol differs: the package's.
    Protocol(u32),
}

/// The policy: refuse another major version, a copy that looks like v4 and another worker
/// protocol; warn about another release of the same major version.
///
/// `found` is the package's version (`None`: unreadable), `has_worker` whether it has a `worker`
/// module, `protocol` the `PROTOCOL_VERSION` in it (`None`: unreadable).
pub fn judge(
    expected: &str,
    expected_protocol: u32,
    found: Option<&str>,
    has_worker: bool,
    protocol: Option<u32>,
) -> Verdict {
    if let (Some(found), Some(want)) = (found, major(expected))
        && let Some(have) = major(found)
        && have != want
    {
        return Verdict::Refuse(Refusal::Major);
    }
    if found.is_none() && !has_worker {
        return Verdict::Refuse(Refusal::LooksLikeV4);
    }
    if let Some(p) = protocol
        && p != expected_protocol
    {
        return Verdict::Refuse(Refusal::Protocol(p));
    }
    match found {
        Some(f) if !same_release(f, expected) => Verdict::OtherRelease,
        _ => Verdict::Match,
    }
}

fn same_release(a: &str, b: &str) -> bool {
    match (
        crate::version::parse_version(a.trim()),
        crate::version::parse_version(b.trim()),
    ) {
        (Ok(x), Ok(y)) => x == y,
        _ => a.trim() == b.trim(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn majors() {
        assert_eq!(major("5.0.0rc1"), Some(5));
        assert_eq!(major(" 4.1.4"), Some(4));
        assert_eq!(major("x"), None);
    }

    #[test]
    fn the_policy() {
        let j =
            |found: Option<&str>, worker, protocol| judge("5.0.0rc1", 1, found, worker, protocol);
        assert_eq!(j(Some("5.0.0rc1"), true, Some(1)), Verdict::Match);
        assert_eq!(j(Some("5.0.0-rc.1"), true, Some(1)), Verdict::Match);
        // Same major, other release, same protocol: starts with a warning.
        assert_eq!(j(Some("5.0.1"), true, Some(1)), Verdict::OtherRelease);
        assert_eq!(j(Some("5.1.0"), true, Some(1)), Verdict::OtherRelease);
        // Another major: refused, whatever else holds.
        assert_eq!(
            j(Some("4.1.4"), false, None),
            Verdict::Refuse(Refusal::Major)
        );
        assert_eq!(
            j(Some("6.0.0"), true, Some(1)),
            Verdict::Refuse(Refusal::Major)
        );
        // Another protocol: refused even for the same release.
        assert_eq!(
            j(Some("5.0.0rc1"), true, Some(2)),
            Verdict::Refuse(Refusal::Protocol(2))
        );
        assert_eq!(
            j(Some("5.0.1"), true, Some(0)),
            Verdict::Refuse(Refusal::Protocol(0))
        );
        // Unreadable version: v4 when there is no worker module, else give it the benefit.
        assert_eq!(j(None, false, None), Verdict::Refuse(Refusal::LooksLikeV4));
        assert_eq!(j(None, true, Some(1)), Verdict::Match);
        assert_eq!(j(None, true, None), Verdict::Match);
    }

    #[test]
    fn the_script_exits_with_the_refusal_code() {
        assert!(SCRIPT.contains(&format!("sys.exit({REFUSED_EXIT_CODE})")));
        // Never uvicorn's startup failure, Python's own codes or the shells'.
        assert!(![0, 1, 2, 3].contains(&REFUSED_EXIT_CODE) && REFUSED_EXIT_CODE < 126);
        // The warning switch does not leak to the processes the host starts.
        assert!(SCRIPT.contains("os.environ.pop('MRSM_GUARD_WARN', None)"));
        assert!(SCRIPT.contains("mrsm reload"));
        // Read once and dropped, like the warning switch.
        assert!(SCRIPT.contains(&format!("os.environ.pop('{RETRY_HINT_ENV}', None)")));
    }

    /// Run the guard against a fake v4 package: the refusal message carries the retry hint the
    /// starter gave (the API host's launcher: restart, not `reload`), or the default one.
    #[test]
    fn the_refusal_message_says_how_to_retry() {
        let Ok(python) = which_python() else {
            eprintln!("skipped: no python3 on PATH");
            return;
        };
        let dir = std::env::temp_dir().join(format!("w17c-guard-{}", std::process::id()));
        let package = dir.join("meerschaum");
        std::fs::create_dir_all(package.join("config")).unwrap();
        std::fs::write(
            package.join("__init__.py"),
            "raise SystemExit('imported')\n",
        )
        .unwrap();
        std::fs::write(
            package.join("config").join("_version.py"),
            "__version__ = \"4.1.4\"\n",
        )
        .unwrap();
        let program = format!("{SCRIPT}\n_mrsm_guard()\n");
        let run = |hint: Option<&str>| {
            let mut cmd = std::process::Command::new(&python);
            cmd.args(["-c", &program])
                .env("PYTHONPATH", &dir)
                .env(EXPECT_VERSION_ENV, "5.0.0")
                .env(EXPECT_PROTOCOL_ENV, "1")
                .env_remove(RETRY_HINT_ENV);
            if let Some(hint) = hint {
                cmd.env(RETRY_HINT_ENV, hint);
            }
            let out = cmd.output().unwrap();
            (
                out.status.code(),
                String::from_utf8_lossy(&out.stderr).to_string(),
            )
        };
        let (code, shown) = run(None);
        assert_eq!(code, Some(REFUSED_EXIT_CODE), "{shown}");
        assert!(
            shown.contains("mrsm reload") && shown.contains("asked again"),
            "{shown}"
        );
        let (code, shown) = run(Some(
            "Then restart `mrsm start api` (or it is retried within 5 minutes).",
        ));
        assert_eq!(code, Some(REFUSED_EXIT_CODE), "{shown}");
        assert!(shown.contains("restart `mrsm start api`"), "{shown}");
        assert!(!shown.contains("mrsm reload"), "{shown}");
        let _ = std::fs::remove_dir_all(&dir);
    }

    fn which_python() -> Result<std::path::PathBuf, ()> {
        std::env::var_os("PATH")
            .and_then(|paths| {
                std::env::split_paths(&paths)
                    .map(|d| d.join("python3"))
                    .find(|p| p.is_file())
            })
            .ok_or(())
    }

    #[test]
    fn the_expected_version_is_pep_440() {
        assert!(!expected_version().contains('-'));
        assert_eq!(expect_env().len(), 2);
    }
}
