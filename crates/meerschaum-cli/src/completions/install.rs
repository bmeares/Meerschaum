//! `show completions`, `install completions` and `uninstall completions` (v5): the scripts, where
//! each shell looks for them, and writing them idempotently.
//!
//! `install` writes only these files (nothing else under the home directory is touched, no shell
//! start-up file except the marked PowerShell profile block):
//!
//! | Shell | Files |
//! |---|---|
//! | bash | `${XDG_DATA_HOME:-~/.local/share}/bash-completion/completions/{mrsm,meerschaum}` |
//! | fish | `${XDG_CONFIG_HOME:-~/.config}/fish/completions/{mrsm,meerschaum}.fish` |
//! | zsh | `${XDG_DATA_HOME:-~/.local/share}/zsh/site-functions/_mrsm` (the `fpath` line is printed) |
//! | PowerShell | `<data>/meerschaum/completions/mrsm.ps1` plus a marked block in the profile |
//!
//! Every file starts with a marker comment: `uninstall` removes a file only when it carries it,
//! and a file with the same name which `mrsm` did not write is never overwritten without
//! `--force`.

use std::path::{Path, PathBuf};
use std::time::{Duration, Instant};

use meerschaum_core::SuccessTuple;

use super::{SHELL_NAMES, Shell};
use crate::builtin::ActionResult;
use crate::dispatch::Runtime;
use crate::native::{flag, words};
use crate::value::ArgsMap;

/// A line every script carries; `uninstall` only removes files that have it.
const MARKER: &str = "mrsm shell completions (managed by `mrsm install completions`";

const PROFILE_BEGIN: &str = "# >>> mrsm completions >>>";
const PROFILE_END: &str = "# <<< mrsm completions <<<";

/// The script of `shell` for this version of the program.
pub fn script(shell: Shell) -> String {
    let template = match shell {
        Shell::Bash => include_str!("scripts/bash.sh"),
        Shell::Zsh => include_str!("scripts/zsh.zsh"),
        Shell::Fish => include_str!("scripts/fish.fish"),
        Shell::PowerShell => include_str!("scripts/powershell.ps1"),
    };
    template.replace("{{VERSION}}", crate::VERSION)
}

/// Where the per-user files live.
#[derive(Debug, Clone)]
pub struct Locations {
    /// The home directory (`HOME`, else `USERPROFILE`).
    pub home: PathBuf,
    /// `XDG_DATA_HOME` (or `~/.local/share`).
    pub data: PathBuf,
    /// `XDG_CONFIG_HOME` (or `~/.config`).
    pub config: PathBuf,
    /// `LOCALAPPDATA`, on Windows.
    pub local_app_data: Option<PathBuf>,
    /// `FPATH`, when the zsh variable is exported.
    pub fpath: Vec<PathBuf>,
}

impl Locations {
    /// From an environment lookup. Fails when there is no home directory.
    pub fn from_env(get: &dyn Fn(&str) -> Option<String>) -> Result<Locations, String> {
        let nonempty = |k: &str| get(k).filter(|v| !v.trim().is_empty());
        let home = nonempty("HOME")
            .or_else(|| nonempty("USERPROFILE"))
            .map(PathBuf::from)
            .ok_or("No home directory: set `HOME` (or `USERPROFILE` on Windows).")?;
        // The XDG specification: a relative path is invalid and ignored.
        let xdg = |k: &str, default: &str| {
            nonempty(k)
                .map(PathBuf::from)
                .filter(|p| p.is_absolute())
                .unwrap_or_else(|| home.join(default))
        };
        Ok(Locations {
            data: xdg("XDG_DATA_HOME", ".local/share"),
            config: xdg("XDG_CONFIG_HOME", ".config"),
            local_app_data: nonempty("LOCALAPPDATA").map(PathBuf::from),
            fpath: nonempty("FPATH")
                .map(|f| f.split(':').map(PathBuf::from).collect())
                .unwrap_or_default(),
            home,
        })
    }

    /// The files `install` writes for `shell`.
    pub fn files(&self, shell: Shell) -> Vec<PathBuf> {
        match shell {
            Shell::Bash => ["mrsm", "meerschaum"]
                .iter()
                .map(|n| {
                    self.data
                        .join("bash-completion")
                        .join("completions")
                        .join(n)
                })
                .collect(),
            Shell::Fish => ["mrsm.fish", "meerschaum.fish"]
                .iter()
                .map(|n| self.config.join("fish").join("completions").join(n))
                .collect(),
            Shell::Zsh => vec![self.zsh_dir().join("_mrsm")],
            Shell::PowerShell => vec![self.powershell_script()],
        }
    }

    fn zsh_dir(&self) -> PathBuf {
        self.data.join("zsh").join("site-functions")
    }

    fn powershell_script(&self) -> PathBuf {
        self.local_app_data
            .clone()
            .unwrap_or_else(|| self.data.clone())
            .join("meerschaum")
            .join("completions")
            .join("mrsm.ps1")
    }
}

/// Whether `content` is a file this program wrote.
fn is_ours(content: &str) -> bool {
    content.lines().take(5).any(|l| l.contains(MARKER))
}

/// Replace `path` with `content` (a temporary file in the same directory, then a rename). A
/// symbolic link (a profile managed by a dotfiles tool) is written through: the link stays and
/// its target changes, keeping the target's permissions.
fn write_file(path: &Path, content: &str) -> Result<(), String> {
    let resolved;
    let path = if std::fs::symlink_metadata(path).is_ok_and(|m| m.file_type().is_symlink()) {
        resolved = std::fs::canonicalize(path).map_err(|e| {
            format!(
                "'{}' is a symbolic link that cannot be followed: {e}",
                path.display()
            )
        })?;
        resolved.as_path()
    } else {
        path
    };
    let dir = path.parent().ok_or("path without a directory")?;
    std::fs::create_dir_all(dir).map_err(|e| format!("Cannot create '{}': {e}", dir.display()))?;
    let tmp = dir.join(format!(
        ".{}.{}.tmp",
        path.file_name().and_then(|n| n.to_str()).unwrap_or("mrsm"),
        std::process::id()
    ));
    std::fs::write(&tmp, content).map_err(|e| format!("Cannot write '{}': {e}", tmp.display()))?;
    if let Ok(meta) = std::fs::metadata(path) {
        let _ = std::fs::set_permissions(&tmp, meta.permissions());
    }
    std::fs::rename(&tmp, path).map_err(|e| {
        let _ = std::fs::remove_file(&tmp);
        format!("Cannot write '{}': {e}", path.display())
    })
}

/// The block that loads the PowerShell script from a profile.
fn profile_block(script: &Path) -> String {
    let quoted = script.display().to_string().replace('\'', "''");
    format!(
        "{PROFILE_BEGIN}\nif (Test-Path -LiteralPath '{quoted}') {{ . '{quoted}' }}\n{PROFILE_END}\n"
    )
}

/// `text` with the marked block removed (`None` when it has none).
fn without_block(text: &str) -> Option<String> {
    let start = text.find(PROFILE_BEGIN)?;
    let end_marker = text[start..].find(PROFILE_END)? + start + PROFILE_END.len();
    let mut end = end_marker;
    if text[end..].starts_with("\r\n") {
        end += 2;
    } else if text[end..].starts_with('\n') {
        end += 1;
    }
    let mut out = String::new();
    out.push_str(&text[..start]);
    out.push_str(&text[end..]);
    Some(out)
}

/// The profile files PowerShell reports for the current user and host: `pwsh` and Windows
/// PowerShell are both asked when they are on the path.
fn probe_profiles() -> Vec<PathBuf> {
    let mut out: Vec<PathBuf> = Vec::new();
    for exe in ["pwsh", "powershell"] {
        let spawned = std::process::Command::new(exe)
            .args([
                "-NoProfile",
                "-NoLogo",
                "-NonInteractive",
                "-Command",
                "$PROFILE.CurrentUserCurrentHost",
            ])
            .stdin(std::process::Stdio::null())
            .stdout(std::process::Stdio::piped())
            .stderr(std::process::Stdio::null())
            .spawn();
        let Ok(mut child) = spawned else { continue };
        let deadline = Instant::now() + Duration::from_secs(20);
        loop {
            match child.try_wait() {
                Ok(Some(_)) => break,
                Ok(None) if Instant::now() < deadline => {
                    std::thread::sleep(Duration::from_millis(20))
                }
                _ => {
                    let _ = child.kill();
                    let _ = child.wait();
                    break;
                }
            }
        }
        let Ok(output) = child.wait_with_output() else {
            continue;
        };
        if !output.status.success() {
            continue;
        }
        let path = String::from_utf8_lossy(&output.stdout).trim().to_string();
        let path = PathBuf::from(path);
        if path.is_absolute() && !out.contains(&path) {
            out.push(path);
        }
    }
    out
}

fn bash_completion_installed() -> bool {
    [
        "/usr/share/bash-completion/bash_completion",
        "/etc/bash_completion",
        "/usr/local/share/bash-completion/bash_completion",
        "/opt/homebrew/etc/profile.d/bash_completion.sh",
        "/usr/local/etc/profile.d/bash_completion.sh",
    ]
    .iter()
    .any(|p| Path::new(p).exists())
}

/// What an install or uninstall did, line by line.
#[derive(Debug, Default, PartialEq, Eq)]
pub struct Report {
    /// Each thing written, removed or left alone, then the instructions.
    pub lines: Vec<String>,
}

/// A file `install` may write, as found on disk.
#[derive(Debug, PartialEq, Eq)]
enum Existing {
    /// There is no such file.
    Missing,
    /// A file `mrsm` wrote, with its bytes.
    Ours(Vec<u8>),
    /// A file `mrsm` did not write (whatever its encoding).
    Foreign,
    /// A file that could not be read (permissions, a directory, ...): it is never replaced
    /// without `--force`.
    Unreadable(String),
}

fn classify(path: &Path) -> Existing {
    match std::fs::read(path) {
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => Existing::Missing,
        Err(e) => Existing::Unreadable(e.to_string()),
        Ok(bytes) if bytes.is_empty() => Existing::Missing,
        // The marker is ASCII: a lossy decode finds it in any ASCII-compatible encoding.
        Ok(bytes) if is_ours(&String::from_utf8_lossy(&bytes)) => Existing::Ours(bytes),
        Ok(_) => Existing::Foreign,
    }
}

/// A PowerShell profile as found on disk.
#[derive(Debug, PartialEq, Eq)]
enum Profile {
    Missing,
    Text(String),
    /// Not UTF-8 (UTF-16 as Windows PowerShell 5.1 writes it, another code page) or unreadable:
    /// never edited, since writing it back as UTF-8 would corrupt or wipe the user's profile.
    Unsupported(String),
}

fn read_profile(path: &Path) -> Profile {
    match std::fs::read(path) {
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => Profile::Missing,
        Err(e) => Profile::Unsupported(format!("it cannot be read: {e}")),
        Ok(bytes) => {
            if bytes.starts_with(&[0xFF, 0xFE]) || bytes.starts_with(&[0xFE, 0xFF]) {
                return Profile::Unsupported("it is UTF-16 encoded".into());
            }
            if bytes.contains(&0) {
                return Profile::Unsupported("it holds NUL bytes (UTF-16 or UTF-32?)".into());
            }
            match String::from_utf8(bytes) {
                Ok(text) => Profile::Text(text),
                Err(_) => Profile::Unsupported("it is not UTF-8 encoded".into()),
            }
        }
    }
}

/// `text` as a zsh single-quoted word.
fn zsh_quote(text: &str) -> String {
    format!("'{}'", text.replace('\'', "'\\''"))
}

/// The UTF-8 byte order mark: written at the start of a profile that `install` creates, because
/// Windows PowerShell 5.1 reads a BOM-less file as the ANSI code page. It also tells `uninstall`
/// that the file was created by `install`.
const BOM: &str = "\u{feff}";

/// Write the files of `shell` (idempotent).
pub fn install(
    shell: Shell,
    loc: &Locations,
    force: bool,
    profiles: &dyn Fn() -> Vec<PathBuf>,
) -> Result<Report, String> {
    let content = script(shell);
    let files = loc.files(shell);
    // Refuse before writing anything: a file of the user's own is not ours to replace.
    if !force {
        let foreign: Vec<String> = files
            .iter()
            .filter(|p| matches!(classify(p), Existing::Foreign | Existing::Unreadable(_)))
            .map(|p| format!("'{}'", p.display()))
            .collect();
        if !foreign.is_empty() {
            return Err(format!(
                "Refusing to overwrite {}: `mrsm` did not write {} (or it cannot be read). \
                 Use `--force` to replace {}.",
                foreign.join(", "),
                if foreign.len() == 1 { "it" } else { "them" },
                if foreign.len() == 1 { "it" } else { "them" },
            ));
        }
    }
    let mut report = Report::default();
    for path in &files {
        let status = match classify(path) {
            Existing::Ours(existing) if existing == content.as_bytes() => "Up to date:",
            Existing::Ours(_) => {
                write_file(path, &content)?;
                "Updated:"
            }
            Existing::Missing => {
                write_file(path, &content)?;
                "Wrote:"
            }
            Existing::Foreign | Existing::Unreadable(_) => {
                write_file(path, &content)?;
                "Replaced:"
            }
        };
        report.lines.push(format!("{status} '{}'", path.display()));
    }
    match shell {
        Shell::Bash => {
            if bash_completion_installed() {
                report.lines.push(
                    "Start a new bash session to use them (the bash-completion package loads them on demand)."
                        .into(),
                );
            } else {
                report.lines.push(
                    "The bash-completion package does not seem to be installed, so bash will not \
                     load these files by itself. Add this line to ~/.bashrc:\n    \
                     source <(mrsm show completions bash)"
                        .into(),
                );
            }
        }
        Shell::Fish => report
            .lines
            .push("Start a new fish session to use them (fish loads them on demand).".into()),
        Shell::Zsh => {
            let dir = loc.zsh_dir();
            if loc.fpath.iter().any(|p| p == &dir) {
                report.lines.push(
                    "That directory is on your `fpath`: start a new zsh session (or run `compinit`) to use them."
                        .into(),
                );
            } else {
                let quoted = zsh_quote(&dir.display().to_string());
                report.lines.push(format!(
                    "Add these lines to ~/.zshrc (before any `compinit`) unless {quoted} is \
                     already on your `fpath`:\n    \
                     fpath=({quoted} $fpath)\n    autoload -Uz compinit && compinit\n\
                     Then start a new zsh session. If the completions do not show up, \
                     delete ~/.zcompdump* once.",
                ));
            }
        }
        Shell::PowerShell => {
            let script_path = loc.powershell_script();
            let line = format!(
                ". '{}'",
                script_path.display().to_string().replace('\'', "''")
            );
            let block = profile_block(&script_path);
            let found = profiles();
            if found.is_empty() {
                report.lines.push(format!(
                    "PowerShell was not found to say where your profile is. Add this line to it:\n    {line}"
                ));
            }
            let mut edited = false;
            for profile in &found {
                let existing = match read_profile(profile) {
                    Profile::Missing => None,
                    Profile::Text(text) => Some(text),
                    Profile::Unsupported(why) => {
                        report.lines.push(format!(
                            "Did not edit the PowerShell profile '{}' ({why}). Add this line to it yourself:\n    {line}",
                            profile.display()
                        ));
                        continue;
                    }
                };
                let status = match existing {
                    None => {
                        write_file(profile, &format!("{BOM}{block}"))?;
                        "Created the PowerShell profile with a marked block:"
                    }
                    Some(existing) => {
                        let base = without_block(&existing).unwrap_or_else(|| existing.clone());
                        let mut next = base;
                        if !next.is_empty() && !next.ends_with('\n') {
                            next.push('\n');
                        }
                        next.push_str(&block);
                        if next == existing {
                            "Up to date (it already loads the script):"
                        } else if existing.contains(PROFILE_BEGIN) {
                            write_file(profile, &next)?;
                            "Updated the marked block in the PowerShell profile:"
                        } else {
                            write_file(profile, &next)?;
                            "Added a marked block to the end of the PowerShell profile:"
                        }
                    }
                };
                edited = true;
                report
                    .lines
                    .push(format!("{status} '{}'", profile.display()));
            }
            if edited {
                report
                    .lines
                    .push("Start a new PowerShell session to use them.".into());
            }
        }
    }
    Ok(report)
}

/// Remove what [`install`] wrote for `shell`, and only that.
pub fn uninstall(
    shell: Shell,
    loc: &Locations,
    profiles: &dyn Fn() -> Vec<PathBuf>,
) -> Result<Report, String> {
    let mut report = Report::default();
    let mut removed_any = false;
    for path in loc.files(shell) {
        match classify(&path) {
            Existing::Ours(_) => {
                std::fs::remove_file(&path)
                    .map_err(|e| format!("Cannot remove '{}': {e}", path.display()))?;
                removed_any = true;
                report.lines.push(format!("Removed: '{}'", path.display()));
            }
            Existing::Foreign => report.lines.push(format!(
                "Left alone (`mrsm` did not write it): '{}'",
                path.display()
            )),
            Existing::Unreadable(why) => report.lines.push(format!(
                "Left alone (it cannot be read: {why}): '{}'",
                path.display()
            )),
            Existing::Missing => {}
        }
    }
    if shell == Shell::PowerShell {
        for profile in profiles() {
            let existing = match read_profile(&profile) {
                Profile::Missing => continue,
                Profile::Text(text) => text,
                Profile::Unsupported(why) => {
                    report.lines.push(format!(
                        "Left the PowerShell profile alone ({why}); remove the block between \
                         `{PROFILE_BEGIN}` and `{PROFILE_END}` yourself if it has one: '{}'",
                        profile.display()
                    ));
                    continue;
                }
            };
            let Some(rest) = without_block(&existing) else {
                continue;
            };
            let is_link =
                std::fs::symlink_metadata(&profile).is_ok_and(|m| m.file_type().is_symlink());
            if rest == BOM && !is_link {
                // `install` created this file (it starts with the byte order mark it writes) and
                // nothing else is in it.
                std::fs::remove_file(&profile)
                    .map_err(|e| format!("Cannot remove '{}': {e}", profile.display()))?;
                report.lines.push(format!(
                    "Removed the PowerShell profile that `install` created: '{}'",
                    profile.display()
                ));
            } else {
                write_file(&profile, &rest)?;
                report.lines.push(format!(
                    "Removed the marked block from the PowerShell profile: '{}'",
                    profile.display()
                ));
            }
            removed_any = true;
        }
    }
    if !removed_any && report.lines.is_empty() {
        report.lines.push(format!(
            "No {} completions installed by `mrsm` were found.",
            shell.name()
        ));
    }
    if shell == Shell::Zsh && removed_any {
        report
            .lines
            .push("Remove the `fpath` line you added to ~/.zshrc if nothing else needs it.".into());
    }
    Ok(report)
}

/// The shell the command names, else the one running (`$SHELL`).
fn chosen_shell(args: &ArgsMap, rt: &Runtime<'_>) -> Result<Shell, String> {
    let names = words(args, "action");
    if names.len() > 1 {
        return Err(format!(
            "Expected at most one shell, got {}. Choose one: {}.",
            names.len(),
            SHELL_NAMES.join(", ")
        ));
    }
    if let Some(name) = names.first() {
        return Shell::parse(name).ok_or_else(|| {
            format!(
                "Unknown shell '{name}'. Choose one: {}.",
                SHELL_NAMES.join(", ")
            )
        });
    }
    Shell::detect((rt.env)("SHELL").as_deref()).ok_or_else(|| {
        format!(
            "Cannot tell which shell you use from `SHELL`. Name one: {}.",
            SHELL_NAMES.join(", ")
        )
    })
}

/// `show completions [shell]`: print the script.
pub fn show_completions(args: &ArgsMap, rt: &mut Runtime<'_>) -> ActionResult {
    let shell = match chosen_shell(args, rt) {
        Ok(shell) => shell,
        Err(message) => return Ok(SuccessTuple::fail(message)),
    };
    rt.print(&script(shell));
    Ok(SuccessTuple::ok("Success"))
}

/// `install completions [shell]`.
pub fn install_completions(args: &ArgsMap, rt: &mut Runtime<'_>) -> ActionResult {
    let shell = match chosen_shell(args, rt) {
        Ok(shell) => shell,
        Err(message) => return Ok(SuccessTuple::fail(message)),
    };
    let env = rt.env;
    let loc = match Locations::from_env(&|k| env(k)) {
        Ok(loc) => loc,
        Err(message) => return Ok(SuccessTuple::fail(message)),
    };
    Ok(
        match install(shell, &loc, flag(args, "force"), &probe_profiles) {
            Ok(report) => SuccessTuple::ok(report.lines.join("\n")),
            Err(message) => SuccessTuple::fail(message),
        },
    )
}

/// `uninstall completions [shell]`.
pub fn uninstall_completions(args: &ArgsMap, rt: &mut Runtime<'_>) -> ActionResult {
    let shell = match chosen_shell(args, rt) {
        Ok(shell) => shell,
        Err(message) => return Ok(SuccessTuple::fail(message)),
    };
    let env = rt.env;
    let loc = match Locations::from_env(&|k| env(k)) {
        Ok(loc) => loc,
        Err(message) => return Ok(SuccessTuple::fail(message)),
    };
    Ok(match uninstall(shell, &loc, &probe_profiles) {
        Ok(report) => SuccessTuple::ok(report.lines.join("\n")),
        Err(message) => SuccessTuple::fail(message),
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    fn loc(home: &Path) -> Locations {
        Locations::from_env(&|k| (k == "HOME").then(|| home.display().to_string())).unwrap()
    }

    fn no_profiles() -> Vec<PathBuf> {
        Vec::new()
    }

    #[test]
    fn locations_follow_the_xdg_variables() {
        let l = Locations::from_env(&|k| match k {
            "HOME" => Some("/h".into()),
            "XDG_DATA_HOME" => Some("/data".into()),
            "XDG_CONFIG_HOME" => Some("relative".into()),
            _ => None,
        })
        .unwrap();
        assert_eq!(
            l.files(Shell::Bash),
            [
                PathBuf::from("/data/bash-completion/completions/mrsm"),
                PathBuf::from("/data/bash-completion/completions/meerschaum")
            ]
        );
        // A relative XDG path is ignored.
        assert_eq!(
            l.files(Shell::Fish)[0],
            PathBuf::from("/h/.config/fish/completions/mrsm.fish")
        );
        assert_eq!(
            l.files(Shell::Zsh),
            [PathBuf::from("/data/zsh/site-functions/_mrsm")]
        );
        assert!(Locations::from_env(&|_| None).is_err());
    }

    #[test]
    fn every_script_has_the_marker_and_the_version() {
        for name in SHELL_NAMES {
            let s = script(Shell::parse(name).unwrap());
            assert!(is_ours(&s), "{name}");
            assert!(s.contains(crate::VERSION), "{name}");
            assert!(!s.contains("{{"), "{name}");
            assert!(s.contains("meerschaum"), "{name}");
        }
    }

    #[test]
    fn install_is_idempotent_and_uninstall_removes_only_ours() {
        let tmp = tempfile::tempdir().unwrap();
        let l = loc(tmp.path());
        for shell in [Shell::Bash, Shell::Fish, Shell::Zsh] {
            let first = install(shell, &l, false, &no_profiles).unwrap();
            assert!(first.lines[0].contains("Wrote:"), "{first:?}");
            let again = install(shell, &l, false, &no_profiles).unwrap();
            assert!(again.lines[0].contains("Up to date"), "{again:?}");
            for f in l.files(shell) {
                assert_eq!(std::fs::read_to_string(&f).unwrap(), script(shell));
            }
            let gone = uninstall(shell, &l, &no_profiles).unwrap();
            assert!(gone.lines[0].starts_with("Removed"), "{gone:?}");
            for f in l.files(shell) {
                assert!(!f.exists());
            }
            let nothing = uninstall(shell, &l, &no_profiles).unwrap();
            assert!(nothing.lines[0].contains("No "), "{nothing:?}");
        }
    }

    #[test]
    fn an_older_script_is_updated_and_a_foreign_file_is_protected() {
        let tmp = tempfile::tempdir().unwrap();
        let l = loc(tmp.path());
        let path = l.files(Shell::Fish)[0].clone();
        std::fs::create_dir_all(path.parent().unwrap()).unwrap();
        std::fs::write(&path, format!("# {MARKER}; version 0.0.1)\nold\n")).unwrap();
        let r = install(Shell::Fish, &l, false, &no_profiles).unwrap();
        assert!(r.lines[0].contains("Updated:"), "{r:?}");

        // The user's own file with the same name (the other program's file is not there yet).
        std::fs::remove_file(&l.files(Shell::Fish)[1]).unwrap();
        std::fs::write(&path, "complete -c mrsm -a mine\n").unwrap();
        let e = install(Shell::Fish, &l, false, &no_profiles).unwrap_err();
        assert!(
            e.contains("Refusing to overwrite") && e.contains("--force"),
            "{e}"
        );
        assert_eq!(
            std::fs::read_to_string(&path).unwrap(),
            "complete -c mrsm -a mine\n"
        );
        // Nothing was written for the other program name either.
        assert!(!l.files(Shell::Fish)[1].exists());
        // Uninstall leaves it alone.
        let r = uninstall(Shell::Fish, &l, &no_profiles).unwrap();
        assert!(r.lines[0].contains("Left alone"), "{r:?}");
        assert!(path.exists());
        // `--force` replaces it.
        let r = install(Shell::Fish, &l, true, &no_profiles).unwrap();
        assert!(r.lines[0].contains("Replaced:"), "{r:?}");
        assert_eq!(std::fs::read_to_string(&path).unwrap(), script(Shell::Fish));
    }

    #[test]
    fn zsh_prints_the_fpath_line_unless_it_is_set_up() {
        let tmp = tempfile::tempdir().unwrap();
        let mut l = loc(tmp.path());
        let r = install(Shell::Zsh, &l, false, &no_profiles).unwrap();
        let text = r.lines.join("\n");
        assert!(
            text.contains("fpath=(") && text.contains("compinit"),
            "{text}"
        );
        l.fpath = vec![l.zsh_dir()];
        let r = install(Shell::Zsh, &l, false, &no_profiles).unwrap();
        assert!(!r.lines.join("\n").contains("fpath=("), "{r:?}");
    }

    #[test]
    fn powershell_gets_a_marked_profile_block_once() {
        let tmp = tempfile::tempdir().unwrap();
        let l = loc(tmp.path());
        let profile = tmp.path().join("pwsh").join("profile.ps1");
        let p = profile.clone();
        let profiles = move || vec![p.clone()];
        // A profile with the user's own content.
        std::fs::create_dir_all(profile.parent().unwrap()).unwrap();
        std::fs::write(&profile, "Set-Alias ll Get-ChildItem").unwrap();
        install(Shell::PowerShell, &l, false, &profiles).unwrap();
        let text = std::fs::read_to_string(&profile).unwrap();
        assert!(text.starts_with("Set-Alias ll Get-ChildItem\n"), "{text}");
        assert_eq!(text.matches(PROFILE_BEGIN).count(), 1);
        assert!(text.contains(&l.powershell_script().display().to_string()));
        // Again: nothing changes.
        let r = install(Shell::PowerShell, &l, false, &profiles).unwrap();
        assert!(r.lines.iter().any(|l| l.contains("Up to date")), "{r:?}");
        assert_eq!(std::fs::read_to_string(&profile).unwrap(), text);
        assert!(l.powershell_script().exists());
        // Uninstall restores the profile and removes the script.
        uninstall(Shell::PowerShell, &l, &profiles).unwrap();
        assert_eq!(
            std::fs::read_to_string(&profile).unwrap(),
            "Set-Alias ll Get-ChildItem\n"
        );
        assert!(!l.powershell_script().exists());
    }

    #[test]
    fn a_profile_created_by_install_is_removed_when_empty() {
        let tmp = tempfile::tempdir().unwrap();
        let l = loc(tmp.path());
        let profile = tmp.path().join("new").join("profile.ps1");
        let p = profile.clone();
        let profiles = move || vec![p.clone()];
        install(Shell::PowerShell, &l, false, &profiles).unwrap();
        assert!(profile.exists());
        uninstall(Shell::PowerShell, &l, &profiles).unwrap();
        assert!(!profile.exists());
    }

    #[test]
    fn powershell_without_a_profile_prints_the_line() {
        let tmp = tempfile::tempdir().unwrap();
        let l = loc(tmp.path());
        let r = install(Shell::PowerShell, &l, false, &no_profiles).unwrap();
        assert!(r.lines.join("\n").contains(". '"), "{r:?}");
    }

    #[test]
    fn the_block_is_found_and_removed_with_crlf() {
        let text = format!("a\r\n{PROFILE_BEGIN}\r\nx\r\n{PROFILE_END}\r\nb\r\n");
        assert_eq!(without_block(&text).unwrap(), "a\r\nb\r\n");
        assert!(without_block("nothing").is_none());
    }

    #[test]
    fn a_new_profile_has_a_byte_order_mark_and_a_preexisting_empty_one_is_kept() {
        let tmp = tempfile::tempdir().unwrap();
        let l = loc(tmp.path());
        let created = tmp.path().join("new").join("profile.ps1");
        let c = created.clone();
        let profiles = move || vec![c.clone()];
        install(Shell::PowerShell, &l, false, &profiles).unwrap();
        let bytes = std::fs::read(&created).unwrap();
        assert!(bytes.starts_with(&[0xEF, 0xBB, 0xBF]), "{bytes:?}");
        uninstall(Shell::PowerShell, &l, &profiles).unwrap();
        assert!(!created.exists());

        // An empty profile the user already had stays after `uninstall`.
        let empty = tmp.path().join("empty.ps1");
        std::fs::write(&empty, "").unwrap();
        let e = empty.clone();
        let profiles = move || vec![e.clone()];
        install(Shell::PowerShell, &l, false, &profiles).unwrap();
        assert!(
            std::fs::read_to_string(&empty)
                .unwrap()
                .contains(PROFILE_BEGIN)
        );
        uninstall(Shell::PowerShell, &l, &profiles).unwrap();
        assert_eq!(std::fs::read(&empty).unwrap(), b"");
    }

    #[test]
    fn a_utf16_profile_is_never_edited() {
        let tmp = tempfile::tempdir().unwrap();
        let l = loc(tmp.path());
        // What Windows PowerShell 5.1's `Out-File` writes: a BOM, then UTF-16LE.
        let mut utf16 = vec![0xFF, 0xFE];
        for unit in "Set-Alias ll Get-ChildItem\r\n".encode_utf16() {
            utf16.extend(unit.to_le_bytes());
        }
        let profile = tmp.path().join("profile.ps1");
        std::fs::write(&profile, &utf16).unwrap();
        let p = profile.clone();
        let profiles = move || vec![p.clone()];
        let r = install(Shell::PowerShell, &l, false, &profiles).unwrap();
        let text = r.lines.join("\n");
        assert!(
            text.contains("Did not edit") && text.contains("UTF-16"),
            "{text}"
        );
        assert!(text.contains(". '"), "{text}");
        assert_eq!(std::fs::read(&profile).unwrap(), utf16);
        // BOM-less UTF-16 (NUL bytes) and Latin-1 are refused too, and uninstall leaves them.
        for bytes in [b"S\0e\0t\0".to_vec(), b"Write-Host 'caf\xe9'\n".to_vec()] {
            std::fs::write(&profile, &bytes).unwrap();
            let r = install(Shell::PowerShell, &l, false, &profiles).unwrap();
            assert!(r.lines.join("\n").contains("Did not edit"), "{r:?}");
            uninstall(Shell::PowerShell, &l, &profiles).unwrap();
            assert_eq!(std::fs::read(&profile).unwrap(), bytes);
        }
    }

    #[test]
    fn a_foreign_file_that_is_not_utf8_is_protected_too() {
        let tmp = tempfile::tempdir().unwrap();
        let l = loc(tmp.path());
        let path = l.files(Shell::Fish)[0].clone();
        std::fs::create_dir_all(path.parent().unwrap()).unwrap();
        let mine = b"# caf\xe9 completions for mrsm\n".to_vec();
        std::fs::write(&path, &mine).unwrap();
        let e = install(Shell::Fish, &l, false, &no_profiles).unwrap_err();
        assert!(e.contains("Refusing to overwrite"), "{e}");
        assert_eq!(std::fs::read(&path).unwrap(), mine);
        // A directory in the way cannot be read either: refused, not replaced.
        let dir_path = l.files(Shell::Fish)[1].clone();
        std::fs::create_dir_all(&dir_path).unwrap();
        std::fs::remove_file(&path).unwrap();
        let e = install(Shell::Fish, &l, false, &no_profiles).unwrap_err();
        assert!(e.contains("Refusing to overwrite"), "{e}");
        let r = uninstall(Shell::Fish, &l, &no_profiles).unwrap();
        assert!(r.lines.join("\n").contains("Left alone"), "{r:?}");
    }

    #[test]
    fn the_zsh_line_quotes_a_quote_in_the_path() {
        assert_eq!(zsh_quote("/h/o'brien/x"), "'/h/o'\\''brien/x'");
        let tmp = tempfile::tempdir().unwrap();
        let home = tmp.path().join("o'brien");
        std::fs::create_dir_all(&home).unwrap();
        let r = install(Shell::Zsh, &loc(&home), false, &no_profiles).unwrap();
        assert!(r.lines.join("\n").contains("o'\\''brien"), "{r:?}");
    }

    #[test]
    fn scripts_are_ascii() {
        // Windows PowerShell 5.1 reads a BOM-less file as the ANSI code page.
        for name in SHELL_NAMES {
            let s = script(Shell::parse(name).unwrap());
            assert!(s.is_ascii(), "{name}");
        }
    }

    #[cfg(unix)]
    #[test]
    fn a_symlinked_profile_is_written_through_and_stays_a_link() {
        use std::os::unix::fs::PermissionsExt;
        let tmp = tempfile::tempdir().unwrap();
        let l = loc(tmp.path());
        let dotfiles = tmp.path().join("dotfiles");
        std::fs::create_dir_all(&dotfiles).unwrap();
        let target = dotfiles.join("profile.ps1");
        std::fs::write(&target, "Set-Alias ll Get-ChildItem\n").unwrap();
        std::fs::set_permissions(&target, std::fs::Permissions::from_mode(0o640)).unwrap();
        let link = tmp.path().join("pwsh").join("profile.ps1");
        std::fs::create_dir_all(link.parent().unwrap()).unwrap();
        std::os::unix::fs::symlink(&target, &link).unwrap();
        let p = link.clone();
        let profiles = move || vec![p.clone()];
        install(Shell::PowerShell, &l, false, &profiles).unwrap();
        assert!(
            std::fs::symlink_metadata(&link)
                .unwrap()
                .file_type()
                .is_symlink()
        );
        assert_eq!(std::fs::read_link(&link).unwrap(), target);
        let text = std::fs::read_to_string(&target).unwrap();
        assert!(text.starts_with("Set-Alias ll Get-ChildItem\n") && text.contains(PROFILE_BEGIN));
        assert_eq!(
            std::fs::metadata(&target).unwrap().permissions().mode() & 0o777,
            0o640
        );
        uninstall(Shell::PowerShell, &l, &profiles).unwrap();
        assert!(
            std::fs::symlink_metadata(&link)
                .unwrap()
                .file_type()
                .is_symlink()
        );
        assert_eq!(
            std::fs::read_to_string(&target).unwrap(),
            "Set-Alias ll Get-ChildItem\n"
        );
        // A link whose target is gone cannot be written through.
        std::fs::remove_file(&target).unwrap();
        let r = install(Shell::PowerShell, &l, false, &profiles);
        assert!(r.is_err(), "{r:?}");
    }
}
