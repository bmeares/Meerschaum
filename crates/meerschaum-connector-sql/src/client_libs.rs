//! Probes for the native client libraries a driver loads at run time (`mrsm show drivers`).
//!
//! A probe only looks for the file in the directories the platform loader would search; it never
//! loads (and so never runs the initializers of) a library it found on the system path.

#[cfg(all(unix, not(target_os = "macos")))]
use std::path::Path;
use std::path::PathBuf;

/// The file names the platform loader tries for Oracle Instant Client (what ODPI-C opens).
pub fn oracle_client_candidates() -> &'static [&'static str] {
    if cfg!(windows) {
        &["oci.dll"]
    } else if cfg!(target_os = "macos") {
        &["libclntsh.dylib"]
    } else {
        &["libclntsh.so"]
    }
}

/// The first Oracle client library found on the system library path, without loading it.
pub fn oracle_client_library() -> Option<PathBuf> {
    oracle_client_candidates()
        .iter()
        .find_map(|name| find_system_library(name))
}

fn absolute_dirs(list: &str, separator: char) -> Vec<PathBuf> {
    list.split(separator)
        .filter(|d| !d.is_empty())
        .map(PathBuf::from)
        .filter(|d| d.is_absolute())
        .collect()
}

/// The directories of `/etc/ld.so.conf` and the files it includes (`include /etc/ld.so.conf.d/*.conf`).
#[cfg(all(unix, not(target_os = "macos")))]
fn ld_so_conf_dirs() -> Vec<PathBuf> {
    ld_so_conf_dirs_from(Path::new("/etc/ld.so.conf"))
}

/// The library directories listed by the `ld.so.conf`-style file `root` and, recursively, by the
/// files its `include` lines match. Like `ldconfig`, a relative include pattern (Fedora and RHEL:
/// `include ld.so.conf.d/*.conf`) is resolved against the directory of the file that includes it.
#[cfg(all(unix, not(target_os = "macos")))]
fn ld_so_conf_dirs_from(root: &Path) -> Vec<PathBuf> {
    fn read(path: &Path, out: &mut Vec<PathBuf>, depth: usize) {
        let Ok(text) = std::fs::read_to_string(path) else {
            return;
        };
        let base = path.parent().unwrap_or(Path::new("/etc"));
        for line in text.lines() {
            let line = line.split('#').next().unwrap_or("").trim();
            if let Some(pattern) = line.strip_prefix("include") {
                // `include` must be a word: `includefoo` is a (relative, so ignored) directory.
                if !pattern.starts_with(char::is_whitespace) {
                    continue;
                }
                let pattern_path = base.join(pattern.trim());
                let (dir, name) = (
                    pattern_path.parent().unwrap_or(base).to_path_buf(),
                    pattern_path
                        .file_name()
                        .map(|n| n.to_string_lossy().into_owned())
                        .unwrap_or_default(),
                );
                if depth >= 3 {
                    continue;
                }
                let mut files: Vec<PathBuf> = std::fs::read_dir(&dir)
                    .into_iter()
                    .flatten()
                    .flatten()
                    .map(|e| e.path())
                    .filter(|p| {
                        let file = p.file_name().map(|n| n.to_string_lossy().into_owned());
                        file.is_some_and(|f| glob_matches(&name, &f))
                    })
                    .collect();
                files.sort();
                for file in files {
                    read(&file, out, depth + 1);
                }
            } else if !line.is_empty() && Path::new(line).is_absolute() {
                out.push(PathBuf::from(line));
            }
        }
    }
    let mut out = Vec::new();
    read(root, &mut out, 0);
    out
}

/// `*` and `?` globs of one file name (what `ld.so.conf` includes use).
#[cfg(all(unix, not(target_os = "macos")))]
fn glob_matches(pattern: &str, name: &str) -> bool {
    fn go(p: &[u8], n: &[u8]) -> bool {
        match (p.first(), n.first()) {
            (None, None) => true,
            (Some(b'*'), _) => go(&p[1..], n) || (!n.is_empty() && go(p, &n[1..])),
            (Some(b'?'), Some(_)) => go(&p[1..], &n[1..]),
            (Some(a), Some(b)) if a == b => go(&p[1..], &n[1..]),
            _ => false,
        }
    }
    go(pattern.as_bytes(), name.as_bytes())
}

/// The directories searched on macOS: the absolute entries of `DYLD_LIBRARY_PATH` and
/// `DYLD_FALLBACK_LIBRARY_PATH` (what the Oracle install hint tells users to set), then the
/// Homebrew and `/usr/local` library directories. Relative entries are never trusted. Only macOS
/// calls it outside the tests.
#[cfg_attr(not(target_os = "macos"), allow(dead_code))]
fn macos_library_dirs(env: &dyn Fn(&str) -> Option<String>) -> Vec<PathBuf> {
    let mut dirs = Vec::new();
    for var in ["DYLD_LIBRARY_PATH", "DYLD_FALLBACK_LIBRARY_PATH"] {
        dirs.extend(absolute_dirs(&env(var).unwrap_or_default(), ':'));
    }
    dirs.push(PathBuf::from("/opt/homebrew/lib"));
    dirs.push(PathBuf::from("/usr/local/lib"));
    dirs
}

/// The directories the platform loader searches for a library by bare name: on Linux
/// `LD_LIBRARY_PATH`, `ld.so.conf` and the standard directories; on macOS `DYLD_LIBRARY_PATH`,
/// `DYLD_FALLBACK_LIBRARY_PATH` and the Homebrew and `/usr/local` library directories; on Windows
/// the absolute entries of `PATH` (never the current directory).
pub fn system_library_dirs() -> Vec<PathBuf> {
    #[cfg(windows)]
    {
        absolute_dirs(&std::env::var("PATH").unwrap_or_default(), ';')
    }
    #[cfg(target_os = "macos")]
    {
        macos_library_dirs(&|name| std::env::var(name).ok())
    }
    #[cfg(all(unix, not(target_os = "macos")))]
    {
        let mut dirs = absolute_dirs(&std::env::var("LD_LIBRARY_PATH").unwrap_or_default(), ':');
        dirs.extend(ld_so_conf_dirs());
        for fixed in [
            "/lib64",
            "/usr/lib64",
            "/usr/local/lib64",
            "/lib",
            "/usr/lib",
            "/usr/local/lib",
            "/lib/x86_64-linux-gnu",
            "/usr/lib/x86_64-linux-gnu",
            "/lib/aarch64-linux-gnu",
            "/usr/lib/aarch64-linux-gnu",
        ] {
            dirs.push(PathBuf::from(fixed));
        }
        dirs
    }
    #[cfg(not(any(windows, unix)))]
    {
        let _ = absolute_dirs;
        Vec::new()
    }
}

/// The file `name` in the first system library directory that has it, without loading it.
pub fn find_system_library(name: &str) -> Option<PathBuf> {
    system_library_dirs()
        .into_iter()
        .map(|dir| dir.join(name))
        .find(|p| p.is_file())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn candidates_are_platform_names() {
        assert!(!oracle_client_candidates().is_empty());
        // Never panics, never loads, whether or not a client is installed.
        let _ = oracle_client_library();
    }

    /// The list separator of the platform's search path variables as `absolute_dirs` is called
    /// on this platform (`:` everywhere except Windows `PATH`).
    fn sep() -> char {
        if cfg!(windows) { ';' } else { ':' }
    }

    /// `export LD_LIBRARY_PATH=/x:$LD_LIBRARY_PATH` with the variable unset leaves an empty entry,
    /// which `dlopen` treats as the current directory: it must never become a search directory.
    #[test]
    fn empty_and_relative_path_entries_are_not_search_directories() {
        let tmp = tempfile::tempdir().unwrap();
        let (a, b) = (tmp.path().join("a"), tmp.path().join("b"));
        let join = |parts: &[&str]| parts.join(&sep().to_string());
        let (a_s, b_s) = (a.to_str().unwrap(), b.to_str().unwrap());
        assert_eq!(absolute_dirs(&join(&[a_s, ""]), sep()), vec![a.clone()]);
        assert!(absolute_dirs(&join(&["", ""]), sep()).is_empty());
        assert!(absolute_dirs(&join(&["", "", ".", "relative/dir", ""]), sep()).is_empty());
        assert_eq!(
            absolute_dirs(&join(&["", a_s, "", b_s, ""]), sep()),
            vec![a, b]
        );
    }

    #[test]
    fn macos_honors_the_dyld_variables_before_the_fixed_directories() {
        let tmp = tempfile::tempdir().unwrap();
        let (a, b, c) = (
            tmp.path().join("a"),
            tmp.path().join("b"),
            tmp.path().join("c"),
        );
        let (a_s, b_s, c_s) = (
            a.to_str().unwrap().to_string(),
            b.to_str().unwrap().to_string(),
            c.to_str().unwrap().to_string(),
        );
        // `macos_library_dirs` splits on `:`, the macOS separator; skip a path that has one.
        if [&a_s, &b_s, &c_s].iter().any(|p| p.contains(':')) {
            return;
        }
        let env = move |name: &str| match name {
            "DYLD_LIBRARY_PATH" => Some(format!("{a_s}::relative:{b_s}")),
            "DYLD_FALLBACK_LIBRARY_PATH" => Some(c_s.clone()),
            _ => None,
        };
        assert_eq!(
            macos_library_dirs(&env),
            vec![
                a,
                b,
                c,
                PathBuf::from("/opt/homebrew/lib"),
                PathBuf::from("/usr/local/lib")
            ]
        );
        assert_eq!(
            macos_library_dirs(&|_| None),
            vec![
                PathBuf::from("/opt/homebrew/lib"),
                PathBuf::from("/usr/local/lib")
            ]
        );
    }

    #[cfg(all(unix, not(target_os = "macos")))]
    #[test]
    fn relative_includes_resolve_against_the_including_file() {
        let tmp = tempfile::tempdir().unwrap();
        let etc = tmp.path().join("etc");
        let conf_d = etc.join("ld.so.conf.d");
        let nested = etc.join("more");
        std::fs::create_dir_all(&conf_d).unwrap();
        std::fs::create_dir_all(&nested).unwrap();
        let abs = |name: &str| tmp.path().join(name);
        std::fs::write(
            etc.join("ld.so.conf"),
            format!(
                "# a comment\ninclude ld.so.conf.d/*.conf\n{}  # trailing\nrelative/dir\ninclude {}/*.conf\n",
                abs("direct").display(),
                nested.display()
            ),
        )
        .unwrap();
        std::fs::write(
            conf_d.join("10-oracle.conf"),
            format!("{}\n", abs("oracle/lib").display()),
        )
        .unwrap();
        // A relative include inside an included file is relative to that file's directory.
        let chained = etc.join("chained");
        std::fs::create_dir_all(&chained).unwrap();
        std::fs::write(conf_d.join("20-chain.conf"), "include ../chained/*.conf\n").unwrap();
        std::fs::write(
            chained.join("c.conf"),
            format!("{}\n", abs("chained-lib").display()),
        )
        .unwrap();
        std::fs::write(conf_d.join("30-skip.conf.bak"), "/not/used\n").unwrap();
        std::fs::write(
            nested.join("a.conf"),
            format!("{}\n", abs("nested").display()),
        )
        .unwrap();
        assert_eq!(
            ld_so_conf_dirs_from(&etc.join("ld.so.conf")),
            vec![
                abs("oracle/lib"),
                abs("chained-lib"),
                abs("direct"),
                abs("nested")
            ]
        );
        // A missing file or an include of a missing directory is not an error.
        assert!(ld_so_conf_dirs_from(&etc.join("absent.conf")).is_empty());
    }

    #[test]
    fn a_planted_file_in_the_current_directory_is_not_a_system_library() {
        let dir = tempfile::tempdir().unwrap();
        let name = "libmrsm-planted-test.so";
        std::fs::write(dir.path().join(name), b"x").unwrap();
        // Searching is by absolute system directories only: the planted file is never found
        // through the working directory.
        assert!(find_system_library(name).is_none());
        assert!(
            system_library_dirs().iter().all(|d| d.is_absolute()),
            "{:?}",
            system_library_dirs()
        );
    }

    #[cfg(all(unix, not(target_os = "macos")))]
    #[test]
    fn globs_match_conf_names() {
        assert!(glob_matches("*.conf", "oracle.conf"));
        assert!(!glob_matches("*.conf", "oracle.conf.bak"));
        assert!(glob_matches("a?c", "abc"));
    }
}
