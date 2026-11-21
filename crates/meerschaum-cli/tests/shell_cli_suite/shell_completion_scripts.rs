//! Shell completions for `mrsm` (v5): the answer protocol through the real binary, the
//! guarantee that answering writes nothing, `install` / `uninstall completions` in a scratch
//! home, and real shells (bash, zsh and fish when they are installed; PowerShell when `pwsh` is)
//! completing `mrsm sh<TAB>`, `mrsm show <TAB>`, `mrsm show pipes -<TAB>`, connector keys, jobs
//! and a plugin action.
//!
//! Isolation: a fresh temporary `HOME`, `XDG_*`, `MRSM_ROOT_DIR` and empty `MRSM_PLUGINS_DIR`
//! for every test, SQLite connectors only, no update checks. A shell that is missing skips its
//! test (the skip is printed); nothing is written outside the scratch directories.

use std::path::{Path, PathBuf};
use std::process::Command;
#[cfg(unix)]
use std::process::Output;

const BIN: &str = env!("CARGO_BIN_EXE_mrsm-cli");

struct Env {
    _tmp: tempfile::TempDir,
    base: PathBuf,
    home: PathBuf,
    root: PathBuf,
    plugins: PathBuf,
    /// A directory holding `mrsm` and `meerschaum` (links to the binary), first on `PATH`.
    bin: PathBuf,
}

impl Env {
    fn new() -> Env {
        let tmp = tempfile::tempdir().unwrap();
        let base = tmp.path().to_path_buf();
        let home = base.join("home");
        let root = base.join("root");
        let plugins = base.join("plugins");
        let bin = base.join("bin");
        for d in [&home, &root, &plugins, &bin] {
            std::fs::create_dir_all(d).unwrap();
        }
        #[cfg(unix)]
        for name in ["mrsm", "meerschaum"] {
            std::os::unix::fs::symlink(BIN, bin.join(name)).unwrap();
        }
        Env {
            _tmp: tmp,
            base,
            home,
            root,
            plugins,
            bin,
        }
    }

    /// The environment every child gets (no inherited `MRSM_*`, no `TMUX`).
    fn apply(&self, command: &mut Command) {
        for (key, _) in std::env::vars() {
            if key.starts_with("MRSM_")
                || key.starts_with("_MRSM_")
                || matches!(
                    key.as_str(),
                    "TMUX" | "TMUX_PANE" | "RUST_LOG" | "ZDOTDIR" | "INPUTRC" | "FPATH"
                )
                || key.starts_with("XDG_")
            {
                command.env_remove(key);
            }
        }
        let path = std::env::var_os("PATH").unwrap_or_default();
        let mut paths = vec![self.bin.clone()];
        paths.extend(std::env::split_paths(&path));
        command
            .env("PATH", std::env::join_paths(paths).unwrap())
            .env("HOME", &self.home)
            .env("MRSM_ROOT_DIR", &self.root)
            .env("MRSM_PLUGINS_DIR", &self.plugins)
            .env("MRSM_NO_UPDATE_CHECK", "1")
            .env("MRSM_NO_AUTO_INSTALL", "1")
            .env(
                "MRSM_SQL_MAIN",
                format!("sqlite:///{}", self.base.join("main.db").display()),
            )
            .env(
                "MRSM_SQL_ALPHA",
                format!("sqlite:///{}", self.base.join("alpha.db").display()),
            )
            .env(
                "MRSM_CONFIG",
                r#"{"meerschaum":{"instance":"sql:alpha"},"shell":{"updates":{"check":false}},"formatting":{"ansi":false,"unicode":false}}"#,
            );
    }

    /// `mrsm <args>` with the scratch environment.
    #[cfg(unix)]
    fn run(&self, args: &[&str]) -> Output {
        let mut command = Command::new(BIN);
        self.apply(&mut command);
        command.args(args).output().unwrap()
    }

    /// The raw answer for `line` over `_MRSM_COMPLETE_LINE`.
    fn answer(&self, line: &str) -> String {
        self.answer_with(line, &[])
    }

    /// [`Env::answer`] with more environment variables.
    fn answer_with(&self, line: &str, extra: &[(&str, &Path)]) -> String {
        let mut command = Command::new(BIN);
        self.apply(&mut command);
        for (k, v) in extra {
            command.env(k, v);
        }
        let out = command
            .env("_MRSM_COMPLETE", "bash")
            .env("_MRSM_COMPLETE_LINE", line)
            .output()
            .unwrap();
        assert!(out.status.success(), "{out:?}");
        assert!(
            out.stderr.is_empty(),
            "{}",
            String::from_utf8_lossy(&out.stderr)
        );
        String::from_utf8(out.stdout).unwrap()
    }

    /// The candidates for `line` (the descriptions dropped).
    fn candidates(&self, line: &str) -> Vec<String> {
        let answer = self.answer(line);
        let mut lines = answer.lines();
        assert!(
            lines.next().unwrap().starts_with("candidates"),
            "{answer:?}"
        );
        lines
            .map(|l| l.split('\t').next().unwrap().to_string())
            .collect()
    }

    /// Every file and directory under `dir`, relative and sorted, with the size of files.
    fn tree(dir: &Path) -> Vec<String> {
        fn walk(dir: &Path, base: &Path, out: &mut Vec<String>) {
            let Ok(entries) = std::fs::read_dir(dir) else {
                return;
            };
            for e in entries.flatten() {
                let path = e.path();
                let rel = path.strip_prefix(base).unwrap().display().to_string();
                if path.is_dir() {
                    out.push(format!("{rel}/"));
                    walk(&path, base, out);
                } else {
                    out.push(format!("{rel} {}", e.metadata().unwrap().len()));
                }
            }
        }
        let mut out = Vec::new();
        walk(dir, dir, &mut out);
        out.sort();
        out
    }
}

#[cfg(unix)]
fn text(out: &Output) -> String {
    format!(
        "{}{}",
        String::from_utf8_lossy(&out.stdout),
        String::from_utf8_lossy(&out.stderr)
    )
}

#[test]
fn the_protocol_answers_over_the_line_and_the_words() {
    let env = Env::new();
    assert_eq!(env.answer("mrsm sh"), "candidates\nsh\nshow\n");
    // The unquoted words, `U+001F` separated; the last is the current word.
    let mut command = Command::new(BIN);
    env.apply(&mut command);
    let out = command
        .env("_MRSM_COMPLETE", "fish")
        .env("_MRSM_COMPLETE_WORDS", "mrsm\u{1f}show\u{1f}gr")
        .output()
        .unwrap();
    assert_eq!(String::from_utf8_lossy(&out.stdout), "candidates\ngraph\n");
    // Spaces inside quotes belong to one word.
    assert_eq!(env.candidates("mrsm show pipes -c 'sql:ma"), ["sql:main"]);
    assert_eq!(env.answer("mrsm compose --file "), "files\n");
    assert!(
        env.answer("mrsm show pipes -m ")
            .starts_with("candidates\n")
    );
    // Without a line the variable alone is not a request: the program runs normally.
    let mut command = Command::new(BIN);
    env.apply(&mut command);
    let out = command
        .env("_MRSM_COMPLETE", "bash")
        .arg("--version")
        .output()
        .unwrap();
    assert!(
        String::from_utf8_lossy(&out.stdout).contains("5."),
        "{out:?}"
    );
}

#[test]
fn candidates_come_from_the_configuration() {
    let env = Env::new();
    let sql = env.candidates("mrsm show pipes -i sql:");
    assert!(sql.contains(&"sql:main".to_string()) && sql.contains(&"sql:alpha".to_string()));
    assert!(!sql.iter().any(|c| c.starts_with("api:")));
    assert_eq!(env.candidates("mrsm -c sql:al"), ["sql:alpha"]);
    assert_eq!(
        env.candidates("mrsm show pipes --instance=sql:al"),
        ["--instance=sql:alpha"]
    );
    let flags = env.candidates("mrsm show pipes -");
    for flag in ["-c", "--connector-keys", "-i", "--yes", "--begin"] {
        assert!(flags.contains(&flag.to_string()), "{flag}");
    }
    let subs = env.candidates("mrsm show ");
    for sub in ["pipes", "graph", "drivers", "completions"] {
        assert!(subs.contains(&sub.to_string()), "{sub}");
    }
    assert_eq!(
        env.candidates("mrsm install completions "),
        ["bash", "zsh", "fish", "powershell"]
    );
    assert_eq!(env.candidates("mrsm show completions p"), ["powershell"]);
    // Config keys and plugin names (a plugin module in the scratch plugins directory).
    assert!(
        env.candidates("mrsm show config ")
            .contains(&"meerschaum".to_string())
    );
    std::fs::write(env.plugins.join("zzplug.py"), "").unwrap();
    assert_eq!(env.candidates("mrsm edit plugins zz"), ["zzplug"]);
}

#[test]
fn jobs_complete_by_state() {
    let env = Env::new();
    let mut out = Vec::new();
    let mut rt = meerschaum_cli::dispatch::Runtime::new(
        meerschaum_cli::types::DateContext::system(),
        80,
        &mut out,
        |_| None,
    );
    let mut opts = meerschaum_core::config::ConfigOptions::with_env(vec![
        ("MRSM_ROOT_DIR".into(), env.root.display().to_string()),
        ("MRSM_PLUGINS_DIR".into(), env.plugins.display().to_string()),
    ]);
    opts.home = env.home.clone();
    rt.config = Some(meerschaum_core::config::Config::new(opts).unwrap());
    let ctx = meerschaum_cli::native::jobs::jobs_context(&mut rt);
    for name in ["nightly-load", "hourly-sync"] {
        let mut daemon = meerschaum_jobs::Daemon::new(
            name,
            vec!["show".into(), "version".into()],
            None,
            Default::default(),
            &ctx,
        );
        assert!(daemon.write_properties().success);
    }
    drop(rt);
    // Both are stopped: `start jobs` offers them, `stop jobs` (running or paused) does not.
    let mut started = env.candidates("mrsm start jobs ");
    started.sort();
    assert_eq!(started, ["hourly-sync", "nightly-load"]);
    assert_eq!(env.candidates("mrsm start jobs n"), ["nightly-load"]);
    assert!(env.candidates("mrsm stop jobs ").is_empty());
    // Answering touched nothing: the job directories are as they were.
    let before = Env::tree(&env.root);
    env.candidates("mrsm delete jobs ");
    assert_eq!(Env::tree(&env.root), before);
}

#[test]
fn answering_writes_nothing_and_does_not_create_the_root() {
    let env = Env::new();
    // An existing, empty root stays empty (no config files, no directories, no manifest).
    assert!(Env::tree(&env.root).is_empty());
    for line in [
        "mrsm sh",
        "mrsm show ",
        "mrsm show pipes -c ",
        "mrsm show pipes -",
        "mrsm stop jobs ",
        "mrsm edit plugins ",
        "mrsm show config ",
        "mrsm compose --file ",
    ] {
        env.answer(line);
        assert_eq!(Env::tree(&env.root), Vec::<String>::new(), "{line}");
        assert_eq!(Env::tree(&env.home), Vec::<String>::new(), "{line}");
    }
    // A root that does not exist is not created either; the answer still names the actions.
    let missing = env.base.join("no-such-root");
    let mut command = Command::new(BIN);
    env.apply(&mut command);
    let out = command
        .env("MRSM_ROOT_DIR", &missing)
        .env("_MRSM_COMPLETE", "bash")
        .env("_MRSM_COMPLETE_LINE", "mrsm sh")
        .output()
        .unwrap();
    assert_eq!(
        String::from_utf8_lossy(&out.stdout),
        "candidates\nsh\nshow\n"
    );
    assert!(String::from_utf8_lossy(&out.stderr).is_empty());
    assert!(!missing.exists());
}

/// A manifest as the last command would have cached it.
fn write_manifest(env: &Env) -> PathBuf {
    use meerschaum_worker::manifest::{
        ActionManifest, ArgumentManifest, Manifest, PluginManifest, SubactionManifest,
        manifest_path,
    };
    let mut manifest = Manifest {
        schema_version: meerschaum_worker::manifest::MANIFEST_SCHEMA_VERSION,
        meerschaum_version: meerschaum_cli::VERSION.into(),
        ..Manifest::default()
    };
    manifest.plugins.insert(
        "greeter".into(),
        PluginManifest {
            name: "greeter".into(),
            data_functions: vec!["fetch".into()],
            actions: vec![ActionManifest {
                name: "greet".into(),
                subactions: vec![SubactionManifest {
                    name: "loudly".into(),
                    doc: None,
                }],
                ..ActionManifest::default()
            }],
            arguments: vec![ArgumentManifest {
                option_strings: vec!["--mood".into()],
                dest: Some("mood".into()),
                action: "_StoreAction".into(),
                choices: serde_json::json!(["calm", "loud"]),
                help: Some("How to greet.".into()),
                ..ArgumentManifest::default()
            }],
            ..PluginManifest::default()
        },
    );
    let path = manifest_path(&env.root);
    manifest.write(&path).unwrap();
    path
}

#[test]
fn plugin_actions_flags_and_connectors_come_from_the_cached_manifest() {
    let env = Env::new();
    let path = write_manifest(&env);
    let before = Env::tree(&env.root);
    let manifest_before = std::fs::read(&path).unwrap();
    assert_eq!(env.candidates("mrsm gre"), ["greet"]);
    assert_eq!(env.candidates("mrsm greet l"), ["loudly"]);
    assert_eq!(env.candidates("mrsm show pipes --mo"), ["--mood"]);
    assert_eq!(env.candidates("mrsm greet --mood "), ["calm", "loud"]);
    assert!(
        env.candidates("mrsm show pipes -c plugin:")
            .contains(&"plugin:greeter".to_string())
    );
    // Nothing rebuilt or rewrote the manifest, and nothing else appeared.
    assert_eq!(std::fs::read(&path).unwrap(), manifest_before);
    assert_eq!(Env::tree(&env.root), before);
}

#[cfg(unix)]
#[test]
fn install_and_uninstall_round_trip_in_a_scratch_home() {
    let env = Env::new();
    let data = env.home.join(".local/share");
    let config = env.home.join(".config");
    for (shell, files) in [
        (
            "bash",
            vec![
                data.join("bash-completion/completions/mrsm"),
                data.join("bash-completion/completions/meerschaum"),
            ],
        ),
        (
            "fish",
            vec![
                config.join("fish/completions/mrsm.fish"),
                config.join("fish/completions/meerschaum.fish"),
            ],
        ),
        ("zsh", vec![data.join("zsh/site-functions/_mrsm")]),
    ] {
        let first = env.run(&["install", "completions", shell]);
        let output = text(&first);
        assert!(first.status.success(), "{output}");
        for f in &files {
            assert!(output.contains(&f.display().to_string()), "{output}");
            assert!(f.exists(), "{}", f.display());
        }
        let shown = env.run(&["show", "completions", shell]);
        for f in &files {
            assert_eq!(
                std::fs::read(f).unwrap(),
                shown.stdout,
                "installed file == `show completions {shell}`"
            );
        }
        let after_first = Env::tree(&env.home);
        let again = env.run(&["install", "completions", shell]);
        assert!(text(&again).contains("Up to date"), "{}", text(&again));
        assert_eq!(Env::tree(&env.home), after_first);
        let gone = env.run(&["uninstall", "completions", shell]);
        assert!(gone.status.success(), "{}", text(&gone));
        for f in &files {
            assert!(!f.exists());
        }
    }
    // Only directories remain: no stray files outside what install wrote.
    let leftovers: Vec<String> = Env::tree(&env.home)
        .into_iter()
        .filter(|l| !l.ends_with('/'))
        .collect();
    assert!(leftovers.is_empty(), "{leftovers:?}");
    // The shell is taken from $SHELL when it is not named.
    let mut command = Command::new(BIN);
    env.apply(&mut command);
    let out = command
        .env("SHELL", "/usr/bin/fish")
        .args(["install", "completions"])
        .output()
        .unwrap();
    assert!(
        text(&out).contains("fish/completions/mrsm.fish"),
        "{}",
        text(&out)
    );
    // A file of the user's with the same name is never replaced without --force.
    let mine = config.join("fish/completions/mrsm.fish");
    std::fs::write(&mine, "complete -c mrsm -a mine\n").unwrap();
    let refused = env.run(&["install", "completions", "fish"]);
    assert!(!refused.status.success(), "{}", text(&refused));
    assert_eq!(
        std::fs::read_to_string(&mine).unwrap(),
        "complete -c mrsm -a mine\n"
    );
    env.run(&["uninstall", "completions", "fish"]);
    assert!(mine.exists());
    let unknown = env.run(&["install", "completions", "nushell"]);
    assert!(
        text(&unknown).contains("Unknown shell"),
        "{}",
        text(&unknown)
    );
}

#[cfg(unix)]
#[test]
fn powershell_install_adds_one_marked_block_to_the_reported_profile() {
    use std::os::unix::fs::PermissionsExt;
    let env = Env::new();
    // A stand-in `pwsh` that reports a profile inside the scratch home.
    let profile = env.home.join("pwsh").join("profile.ps1");
    let fake = env.bin.join("pwsh");
    std::fs::write(&fake, format!("#!/bin/sh\necho '{}'\n", profile.display())).unwrap();
    std::fs::set_permissions(&fake, std::fs::Permissions::from_mode(0o755)).unwrap();
    std::fs::create_dir_all(profile.parent().unwrap()).unwrap();
    std::fs::write(&profile, "Set-Alias ll Get-ChildItem\n").unwrap();
    let out = env.run(&["install", "completions", "powershell"]);
    assert!(out.status.success(), "{}", text(&out));
    let profile_text = std::fs::read_to_string(&profile).unwrap();
    assert_eq!(
        profile_text.matches("# >>> mrsm completions >>>").count(),
        1
    );
    let again = env.run(&["install", "completions", "powershell"]);
    assert!(text(&again).contains("Up to date"), "{}", text(&again));
    assert_eq!(std::fs::read_to_string(&profile).unwrap(), profile_text);
    let gone = env.run(&["uninstall", "completions", "powershell"]);
    assert!(gone.status.success(), "{}", text(&gone));
    assert_eq!(
        std::fs::read_to_string(&profile).unwrap(),
        "Set-Alias ll Get-ChildItem\n"
    );
}

/// Job directories with these names (hostile to shells) in the scratch root.
#[cfg(unix)]
const NASTY: &[&str] = &[
    "a$(touch pwn)",
    "b`touch pwn2`",
    "c'd",
    "e;f",
    "g\"h",
    "i j",
    "k#l&m|n",
    "o!p",
];

#[cfg(unix)]
fn make_job_dirs(env: &Env, names: &[&str]) {
    for name in names {
        let dir = env.root.join("jobs").join(name);
        std::fs::create_dir_all(&dir).unwrap();
        std::fs::write(dir.join("properties.json"), "{}").unwrap();
    }
}

#[cfg(unix)]
#[test]
fn names_with_shell_specials_are_candidates_verbatim() {
    let env = Env::new();
    make_job_dirs(&env, NASTY);
    std::fs::write(env.plugins.join("p$(x)`y`;z.py"), "").unwrap();
    let mut jobs = env.candidates("mrsm start jobs ");
    jobs.sort();
    let mut expected: Vec<String> = NASTY.iter().map(|s| s.to_string()).collect();
    expected.sort();
    assert_eq!(jobs, expected);
    assert_eq!(env.candidates("mrsm edit plugins p"), ["p$(x)`y`;z"]);
}

#[cfg(unix)]
#[test]
fn completions_do_not_touch_pid_files_or_create_the_venvs_directory() {
    let env = Env::new();
    make_job_dirs(&env, &["stale", "live"]);
    let jobs = env.root.join("jobs");
    // A PID that names no process, and one that does (this test process).
    std::fs::write(jobs.join("stale/process.pid"), "999999").unwrap();
    std::fs::write(
        jobs.join("live/process.pid"),
        std::process::id().to_string(),
    )
    .unwrap();
    // A job with a lost PID file that the supervisor says began: `Job::status` would rewrite it.
    make_job_dirs(&env, &["lost"]);
    std::fs::write(
        jobs.join("lost/properties.json"),
        r#"{"process":{"began":"2999-01-01T00:00:00"}}"#,
    )
    .unwrap();
    let venvs = env.base.join("venvs-that-does-not-exist");
    let before = Env::tree(&env.base);
    for line in [
        "mrsm start jobs ",
        "mrsm stop jobs ",
        "mrsm show jobs ",
        "mrsm sh",
    ] {
        env.answer_with(line, &[("MRSM_VENVS_DIR", &venvs)]);
    }
    assert!(!venvs.exists(), "a completion created MRSM_VENVS_DIR");
    assert!(jobs.join("stale/process.pid").exists());
    assert!(!jobs.join("lost/process.pid").exists());
    assert_eq!(Env::tree(&env.base), before);
    // The state is still read: the live PID is running, the stale one is stopped.
    let mut started = env.candidates("mrsm start jobs ");
    started.sort();
    assert_eq!(started, ["lost", "stale"]);
    assert_eq!(env.candidates("mrsm stop jobs "), ["live"]);
}

#[test]
fn show_completions_is_answered_before_the_dispatcher() {
    let env = Env::new();
    // A root that does not exist would stop a normal run; the script still prints, and nothing
    // is created.
    let missing = env.base.join("no-such-root");
    let before = Env::tree(&env.base);
    for shell in ["bash", "zsh", "fish", "powershell"] {
        let mut command = Command::new(BIN);
        env.apply(&mut command);
        let out = command
            .env("MRSM_ROOT_DIR", &missing)
            .args(["show", "completions", shell])
            .output()
            .unwrap();
        assert!(out.status.success(), "{out:?}");
        let script = String::from_utf8(out.stdout).unwrap();
        assert!(
            script.contains("mrsm shell completions (managed by"),
            "{shell}"
        );
        assert!(out.stderr.is_empty());
    }
    let mut command = Command::new(BIN);
    env.apply(&mut command);
    let out = command
        .env("MRSM_ROOT_DIR", &missing)
        .env("SHELL", "/bin/zsh")
        .args(["show", "completion"])
        .output()
        .unwrap();
    assert!(String::from_utf8_lossy(&out.stdout).starts_with("#compdef"));
    assert!(!missing.exists());
    assert_eq!(Env::tree(&env.base), before);
}

#[cfg(unix)]
#[test]
fn a_utf16_powershell_profile_is_left_alone() {
    use std::os::unix::fs::PermissionsExt;
    let env = Env::new();
    let profile = env.home.join("pwsh").join("profile.ps1");
    let fake = env.bin.join("pwsh");
    std::fs::write(&fake, format!("#!/bin/sh\necho '{}'\n", profile.display())).unwrap();
    std::fs::set_permissions(&fake, std::fs::Permissions::from_mode(0o755)).unwrap();
    std::fs::create_dir_all(profile.parent().unwrap()).unwrap();
    // What `Out-File` of Windows PowerShell 5.1 writes: a BOM, then UTF-16LE.
    let mut utf16 = vec![0xFF, 0xFE];
    for unit in "Set-Alias ll Get-ChildItem\r\n".encode_utf16() {
        utf16.extend(unit.to_le_bytes());
    }
    std::fs::write(&profile, &utf16).unwrap();
    let out = env.run(&["install", "completions", "powershell"]);
    let output = text(&out);
    assert!(out.status.success(), "{output}");
    assert!(output.contains("Did not edit"), "{output}");
    assert!(output.contains("UTF-16"), "{output}");
    assert!(output.contains(". '"), "{output}");
    assert_eq!(std::fs::read(&profile).unwrap(), utf16);
    let out = env.run(&["uninstall", "completions", "powershell"]);
    assert!(out.status.success(), "{}", text(&out));
    assert_eq!(std::fs::read(&profile).unwrap(), utf16);
}

#[cfg(unix)]
#[test]
fn a_foreign_completion_file_that_is_not_utf8_is_not_replaced() {
    let env = Env::new();
    let mine = env.home.join(".config/fish/completions/mrsm.fish");
    std::fs::create_dir_all(mine.parent().unwrap()).unwrap();
    std::fs::write(&mine, b"# caf\xe9 mine\n").unwrap();
    let out = env.run(&["install", "completions", "fish"]);
    assert!(!out.status.success(), "{}", text(&out));
    assert_eq!(std::fs::read(&mine).unwrap(), b"# caf\xe9 mine\n");
}

// ------------------------------------------------------------------ real shells

#[cfg(unix)]
mod shells {
    use super::*;
    use std::io::{Read, Write};
    use std::sync::mpsc::{Receiver, channel};
    use std::time::{Duration, Instant};

    use portable_pty::{CommandBuilder, PtySize, native_pty_system};

    fn which(program: &str) -> Option<PathBuf> {
        std::env::split_paths(&std::env::var_os("PATH")?)
            .map(|d| d.join(program))
            .find(|p| p.is_file())
    }

    /// An interactive shell in a pseudo-terminal, rendered with `vt100`.
    struct Terminal {
        writer: Box<dyn Write + Send>,
        output: Receiver<Vec<u8>>,
        parser: vt100::Parser,
        _child: Box<dyn portable_pty::Child + Send + Sync>,
    }

    impl Terminal {
        fn spawn(env: &Env, program: &Path, args: &[&str], extra: &[(&str, String)]) -> Terminal {
            let pair = native_pty_system()
                .openpty(PtySize {
                    rows: 300,
                    cols: 120,
                    pixel_width: 0,
                    pixel_height: 0,
                })
                .unwrap();
            let mut command = CommandBuilder::new(program);
            command.args(args);
            command.env_clear();
            let path = std::env::var_os("PATH").unwrap_or_default();
            let mut paths = vec![env.bin.clone()];
            paths.extend(std::env::split_paths(&path));
            command.env("PATH", std::env::join_paths(paths).unwrap());
            command.env("HOME", &env.home);
            command.env("TERM", "xterm");
            command.env("MRSM_ROOT_DIR", &env.root);
            command.env("MRSM_PLUGINS_DIR", &env.plugins);
            command.env("MRSM_NO_UPDATE_CHECK", "1");
            command.env("MRSM_NO_AUTO_INSTALL", "1");
            command.env(
                "MRSM_SQL_MAIN",
                format!("sqlite:///{}", env.base.join("main.db").display()),
            );
            command.env("MRSM_CONFIG", r#"{"meerschaum":{"instance":"sql:main"}}"#);
            command.env("LANG", "C.UTF-8");
            for (k, v) in extra {
                command.env(k, v);
            }
            command.cwd(&env.base);
            let child = pair.slave.spawn_command(command).unwrap();
            drop(pair.slave);
            let mut reader = pair.master.try_clone_reader().unwrap();
            let writer = pair.master.take_writer().unwrap();
            let (tx, rx) = channel();
            std::thread::spawn(move || {
                let mut buf = [0u8; 4096];
                while let Ok(n) = reader.read(&mut buf) {
                    if n == 0 || tx.send(buf[..n].to_vec()).is_err() {
                        break;
                    }
                }
            });
            // Keep the master alive for the life of the terminal.
            std::mem::forget(pair.master);
            Terminal {
                writer,
                output: rx,
                parser: vt100::Parser::new(300, 120, 0),
                _child: child,
            }
        }

        /// Read until the shell has been quiet for `quiet`.
        fn settle(&mut self, quiet: Duration) {
            let deadline = Instant::now() + Duration::from_secs(20);
            while let Ok(bytes) = self.output.recv_timeout(quiet) {
                self.parser.process(&bytes);
                if Instant::now() > deadline {
                    break;
                }
            }
        }

        fn send(&mut self, text: &str) {
            self.writer.write_all(text.as_bytes()).unwrap();
            self.writer.flush().unwrap();
        }

        /// Clear the screen, type `typed`, press Tab `tabs` times and return what is on screen.
        fn tab(&mut self, typed: &str, tabs: usize) -> String {
            self.send("\x15\x0c");
            self.settle(Duration::from_millis(250));
            self.send(typed);
            self.settle(Duration::from_millis(150));
            for _ in 0..tabs {
                self.send("\t");
                self.settle(Duration::from_millis(900));
            }
            let screen = self.parser.screen().contents();
            // Leave the line empty for the next call.
            self.send("\x15");
            self.settle(Duration::from_millis(150));
            screen
        }
    }

    /// Like [`Terminal::tab`], then readline's `history-expand-line` (Alt-^) on what was
    /// completed: a `!` that history would expand changes the line or reports "event not found".
    impl Terminal {
        fn tab_expanded(&mut self, typed: &str) -> String {
            self.send("\x15\x0c");
            self.settle(Duration::from_millis(250));
            self.send(typed);
            self.settle(Duration::from_millis(150));
            self.send("\t");
            self.settle(Duration::from_millis(900));
            self.send("\x1b^");
            self.settle(Duration::from_millis(600));
            let screen = self.parser.screen().contents();
            self.send("\x15");
            self.settle(Duration::from_millis(150));
            screen
        }
    }

    fn words_of(screen: &str) -> Vec<String> {
        screen
            .split_whitespace()
            .map(str::to_string)
            .collect::<Vec<_>>()
    }

    fn start_bash(env: &Env, extra_rc: &str) -> Terminal {
        let rc = env.home.join("bashrc");
        std::fs::write(
            env.home.join("inputrc"),
            "set show-all-if-ambiguous on\nset bell-style none\nset colored-stats off\n\
             set page-completions off\nset completion-query-items 1000\n",
        )
        .unwrap();
        std::fs::write(&rc, format!("PS1='$ '\n{extra_rc}\n")).unwrap();
        let bash = which("bash").unwrap();
        let mut t = Terminal::spawn(
            env,
            &bash,
            &["--noprofile", "--rcfile", rc.to_str().unwrap(), "-i"],
            &[("INPUTRC", env.home.join("inputrc").display().to_string())],
        );
        t.settle(Duration::from_millis(700));
        t
    }

    #[test]
    fn bash_completes_through_a_real_terminal() {
        if which("bash").is_none() {
            eprintln!("skipped: bash is not installed");
            return;
        }
        let env = Env::new();
        std::fs::write(env.base.join("compose-mrsm.yaml"), "").unwrap();
        let mut t = start_bash(&env, "source <(mrsm show completions bash)");
        // The manifest a later command cached (after the real `mrsm` run that loaded the script).
        write_manifest(&env);
        let s = t.tab("mrsm sh", 1);
        assert!(words_of(&s).contains(&"show".to_string()), "{s}");
        assert!(
            words_of(&s).iter().filter(|w| *w == "sh").count() >= 2,
            "{s}"
        );
        let s = t.tab("mrsm show gra", 1);
        assert!(s.contains("mrsm show graph"), "{s}");
        let s = t.tab("meerschaum show gra", 1);
        assert!(s.contains("meerschaum show graph"), "{s}");
        let s = t.tab("mrsm show ", 1);
        for sub in ["pipes", "graph", "completions", "drivers", "version"] {
            assert!(words_of(&s).contains(&sub.to_string()), "{sub}: {s}");
        }
        let s = t.tab("mrsm show pipes -", 1);
        for flag in ["--connector-keys", "--yes", "--begin"] {
            assert!(words_of(&s).contains(&flag.to_string()), "{flag}: {s}");
        }
        let s = t.tab("mrsm show pipes -c ", 1);
        assert!(words_of(&s).contains(&"sql:main".to_string()), "{s}");
        // `:` splits words in bash: the completed word is still `sql:main`.
        let s = t.tab("mrsm show pipes -c sql:ma", 1);
        assert!(s.contains("-c sql:main"), "{s}");
        assert!(!s.contains("sql:sql:") && !s.contains("sql:main:"), "{s}");
        let s = t.tab("mrsm show pipes --instance=sql:ma", 1);
        assert!(s.contains("--instance=sql:main"), "{s}");
        // A plugin action from the cached manifest, and its flag's choices.
        let s = t.tab("mrsm gre", 1);
        assert!(s.contains("mrsm greet"), "{s}");
        let s = t.tab("mrsm greet --mood ", 1);
        assert!(words_of(&s).contains(&"calm".to_string()), "{s}");
        // A file flag completes file names.
        let s = t.tab("mrsm compose --file compose-m", 1);
        assert!(s.contains("compose-mrsm.yaml"), "{s}");
        // A flag whose values are not completed offers no files.
        let s = t.tab("mrsm show pipes -m compose-m", 1);
        assert!(!s.contains("compose-mrsm.yaml"), "{s}");
        // Quoted words keep working.
        let s = t.tab("mrsm show pipes -c 'sql:ma", 1);
        assert!(s.contains("sql:main"), "{s}");
    }

    /// The word `mrsm start jobs <typed><TAB>` completed to, as it is on screen.
    fn completed_word(screen: &str, typed_prefix: &str) -> String {
        let line = screen
            .lines()
            .find(|l| l.starts_with("$ mrsm start jobs "))
            .unwrap_or_else(|| panic!("no command line in\n{screen}"));
        let word = line.trim_start_matches("$ mrsm start jobs ").trim_end();
        assert!(!word.is_empty(), "{typed_prefix:?}: {screen}");
        word.to_string()
    }

    /// What `shell -c "<print> <word>"` prints for `word`: the name the shell would pass on.
    fn evaluated(env: &Env, shell: &str, print: &str, word: &str) -> String {
        let out = Command::new(shell)
            .args(["-c", &format!("{print} {word}")])
            .current_dir(&env.base)
            .output()
            .unwrap();
        String::from_utf8_lossy(&out.stdout).to_string()
    }

    #[test]
    fn bash_quotes_names_with_shell_specials() {
        if which("bash").is_none() {
            eprintln!("skipped: bash is not installed");
            return;
        }
        let env = Env::new();
        make_job_dirs(&env, NASTY);
        let mut t = start_bash(&env, "source <(mrsm show completions bash)");
        // Each name starts with a distinct character; try the bare word and both quotes.
        for name in NASTY {
            let first = name.chars().next().unwrap().to_string();
            for (opening, form) in [("", "bare"), ("'", "single"), ("\"", "double")] {
                let typed = format!("mrsm start jobs {opening}{first}");
                let s = t.tab(&typed, 1);
                let word = completed_word(&s, &typed);
                let got = evaluated(&env, "bash", "printf %s", &word);
                assert_eq!(&got, name, "{form}: {word:?} on\n{s}");
                if name.contains('!') {
                    // History expansion must leave the completed line alone.
                    let expanded = t.tab_expanded(&typed);
                    assert!(!expanded.contains("not found"), "{form}: {expanded}");
                    assert_eq!(completed_word(&expanded, &typed), word, "{form}");
                }
            }
        }
        // Nothing was executed by completing or by evaluating the quoted words.
        for name in ["pwn", "pwn2"] {
            assert!(!env.base.join(name).exists(), "{name} was created");
        }
    }

    #[test]
    fn bash_completes_after_an_open_quote_following_a_flag() {
        if which("bash").is_none() {
            eprintln!("skipped: bash is not installed");
            return;
        }
        let env = Env::new();
        let mut t = start_bash(&env, "source <(mrsm show completions bash)");
        let s = t.tab("mrsm show pipes --instance='sql:ma", 1);
        assert!(s.contains("--instance='sql:main"), "{s}");
        assert!(!s.contains("--instance='--instance"), "{s}");
    }

    /// A `mrsm` that does not know the protocol (an older release) and reads its terminal: a TAB
    /// must not hand it the keyboard.
    #[test]
    fn a_program_that_ignores_the_protocol_does_not_swallow_keystrokes() {
        use std::os::unix::fs::PermissionsExt;
        if which("bash").is_none() {
            eprintln!("skipped: bash is not installed");
            return;
        }
        let env = Env::new();
        let script = Command::new(BIN)
            .args(["show", "completions", "bash"])
            .env("MRSM_ROOT_DIR", &env.root)
            .env("MRSM_PLUGINS_DIR", &env.plugins)
            .output()
            .unwrap()
            .stdout;
        std::fs::write(env.base.join("mrsm-completion.bash"), script).unwrap();
        let stub = env.bin.join("mrsm");
        std::fs::remove_file(&stub).unwrap();
        // Waits for a line on stdin, like an interactive shell would.
        std::fs::write(&stub, "#!/bin/sh\nread line\necho \"interactive:$line\"\n").unwrap();
        std::fs::set_permissions(&stub, std::fs::Permissions::from_mode(0o755)).unwrap();
        let mut t = start_bash(
            &env,
            &format!(
                "source '{}'",
                env.base.join("mrsm-completion.bash").display()
            ),
        );
        let s = t.tab("mrsm sh", 1);
        assert!(!s.contains("interactive:"), "{s}");
        // The terminal is still ours: what we type next reaches the prompt.
        t.send("\x15echo alive-$((40+2))\n");
        t.settle(Duration::from_millis(700));
        let screen = t.parser.screen().contents();
        assert!(screen.contains("alive-42"), "{screen}");
    }

    #[test]
    fn zsh_does_not_hand_the_terminal_to_a_program_that_ignores_the_protocol() {
        use std::os::unix::fs::PermissionsExt;
        let Some(zsh) = which("zsh") else {
            eprintln!("skipped: zsh is not installed");
            return;
        };
        let env = Env::new();
        let script = Command::new(BIN)
            .args(["show", "completions", "zsh"])
            .env("MRSM_ROOT_DIR", &env.root)
            .env("MRSM_PLUGINS_DIR", &env.plugins)
            .output()
            .unwrap()
            .stdout;
        std::fs::write(env.base.join("mrsm-completion.zsh"), script).unwrap();
        let stub = env.bin.join("mrsm");
        std::fs::remove_file(&stub).unwrap();
        std::fs::write(&stub, "#!/bin/sh\nread line\necho \"interactive:$line\"\n").unwrap();
        std::fs::set_permissions(&stub, std::fs::Permissions::from_mode(0o755)).unwrap();
        std::fs::write(
            env.home.join(".zshrc"),
            format!(
                "autoload -Uz compinit\ncompinit -u -d '{}'\nsource '{}'\nPS1='$ '\n\
                 setopt NO_BEEP\nunsetopt AUTO_MENU\n",
                env.home.join(".zcompdump").display(),
                env.base.join("mrsm-completion.zsh").display(),
            ),
        )
        .unwrap();
        let mut t = Terminal::spawn(
            &env,
            &zsh,
            &["-i"],
            &[("ZDOTDIR", env.home.display().to_string())],
        );
        t.settle(Duration::from_millis(1500));
        let s = t.tab("mrsm sh", 1);
        assert!(!s.contains("interactive:"), "{s}");
        t.send("\x15echo alive-$((40+2))\n");
        t.settle(Duration::from_millis(700));
        assert!(t.parser.screen().contents().contains("alive-42"));
    }

    #[test]
    fn zsh_quotes_names_with_shell_specials() {
        let Some(zsh) = which("zsh") else {
            eprintln!("skipped: zsh is not installed");
            return;
        };
        let env = Env::new();
        make_job_dirs(&env, NASTY);
        std::fs::write(
            env.home.join(".zshrc"),
            format!(
                "autoload -Uz compinit\ncompinit -u -d '{}'\nsource <(mrsm show completions zsh)\n\
                 PS1='$ '\nsetopt NO_BEEP\nunsetopt AUTO_MENU\n",
                env.home.join(".zcompdump").display(),
            ),
        )
        .unwrap();
        let mut t = Terminal::spawn(
            &env,
            &zsh,
            &["-i"],
            &[("ZDOTDIR", env.home.display().to_string())],
        );
        t.settle(Duration::from_millis(1500));
        for name in NASTY {
            let first = name.chars().next().unwrap().to_string();
            for opening in ["", "'"] {
                let typed = format!("mrsm start jobs {opening}{first}");
                let s = t.tab(&typed, 1);
                let word = completed_word(&s, &typed);
                let got = evaluated(&env, zsh.to_str().unwrap(), "print -rn --", &word);
                assert_eq!(&got, name, "{word:?} on\n{s}");
            }
        }
        for name in ["pwn", "pwn2"] {
            assert!(!env.base.join(name).exists(), "{name} was created");
        }
    }

    #[test]
    fn bash_completion_loads_the_installed_files_on_demand() {
        let loader = Path::new("/usr/share/bash-completion/bash_completion");
        if which("bash").is_none() || !loader.exists() {
            eprintln!("skipped: bash or the bash-completion package is not installed");
            return;
        }
        let env = Env::new();
        let out = env.run(&["install", "completions", "bash"]);
        assert!(out.status.success(), "{}", text(&out));
        let mut t = start_bash(
            &env,
            &format!(
                "export XDG_DATA_HOME={}\nsource {}",
                env.home.join(".local/share").display(),
                loader.display()
            ),
        );
        let s = t.tab("mrsm show gra", 1);
        assert!(s.contains("mrsm show graph"), "{s}");
        let s = t.tab("meerschaum show pipes -c sql:ma", 1);
        assert!(s.contains("-c sql:main"), "{s}");
    }

    #[test]
    fn root_stays_empty_while_bash_completes() {
        if which("bash").is_none() {
            eprintln!("skipped: bash is not installed");
            return;
        }
        let env = Env::new();
        let mut t = start_bash(&env, "source <(mrsm show completions bash)");
        // Loading the script ran `mrsm` for real, which sets the root up; completing adds nothing.
        let before = Env::tree(&env.root);
        t.tab("mrsm show pipes -c ", 1);
        t.tab("mrsm sh", 1);
        t.tab("mrsm edit plugins ", 1);
        t.tab("mrsm stop jobs ", 1);
        assert_eq!(Env::tree(&env.root), before);
    }

    #[test]
    fn zsh_completes_through_a_real_terminal() {
        let Some(zsh) = which("zsh") else {
            eprintln!("skipped: zsh is not installed");
            return;
        };
        let env = Env::new();
        let out = env.run(&["install", "completions", "zsh"]);
        assert!(out.status.success(), "{}", text(&out));
        write_manifest(&env);
        std::fs::write(
            env.home.join(".zshrc"),
            format!(
                "fpath=('{}' $fpath)\nautoload -Uz compinit\ncompinit -u -d '{}'\nPS1='$ '\n\
                 setopt NO_BEEP\nLISTMAX=1000\nzstyle ':completion:*' menu no\n\
                 zstyle ':completion:*' list-colors ''\nunsetopt AUTO_MENU\n",
                env.home.join(".local/share/zsh/site-functions").display(),
                env.home.join(".zcompdump").display(),
            ),
        )
        .unwrap();
        let mut t = Terminal::spawn(
            &env,
            &zsh,
            &["-i"],
            &[("ZDOTDIR", env.home.display().to_string())],
        );
        t.settle(Duration::from_millis(1500));
        let s = t.tab("mrsm sh", 1);
        assert!(words_of(&s).contains(&"show".to_string()), "{s}");
        let s = t.tab("mrsm show gra", 1);
        assert!(s.contains("mrsm show graph"), "{s}");
        let s = t.tab("meerschaum show gra", 1);
        assert!(s.contains("meerschaum show graph"), "{s}");
        let s = t.tab("mrsm show ", 2);
        for sub in ["pipes", "graph", "completions"] {
            assert!(s.contains(sub), "{sub}: {s}");
        }
        let s = t.tab("mrsm show pipes -", 2);
        assert!(s.contains("--connector-keys"), "{s}");
        let s = t.tab("mrsm show pipes -c ", 2);
        assert!(s.contains("sql:main"), "{s}");
        let s = t.tab("mrsm show pipes -c sql:ma", 1);
        assert!(s.contains("-c sql:main"), "{s}");
        let s = t.tab("mrsm gre", 1);
        assert!(s.contains("mrsm greet"), "{s}");
        let s = t.tab("mrsm greet --mood ", 2);
        assert!(s.contains("calm") && s.contains("loud"), "{s}");
    }

    #[test]
    fn zsh_loads_the_script_with_source() {
        let Some(zsh) = which("zsh") else {
            eprintln!("skipped: zsh is not installed");
            return;
        };
        let env = Env::new();
        std::fs::write(
            env.home.join(".zshrc"),
            format!(
                "autoload -Uz compinit\ncompinit -u -d '{}'\nsource <(mrsm show completions zsh)\n\
                 PS1='$ '\nsetopt NO_BEEP\nLISTMAX=1000\nunsetopt AUTO_MENU\n",
                env.home.join(".zcompdump").display(),
            ),
        )
        .unwrap();
        let mut t = Terminal::spawn(
            &env,
            &zsh,
            &["-i"],
            &[("ZDOTDIR", env.home.display().to_string())],
        );
        t.settle(Duration::from_millis(1500));
        let s = t.tab("mrsm show gra", 1);
        assert!(s.contains("mrsm show graph"), "{s}");
    }

    #[test]
    fn fish_completes_installed_files() {
        let Some(fish) = which("fish") else {
            eprintln!("skipped: fish is not installed");
            return;
        };
        let env = Env::new();
        let out = env.run(&["install", "completions", "fish"]);
        assert!(out.status.success(), "{}", text(&out));
        write_manifest(&env);
        let complete = |line: &str| -> Vec<String> {
            let mut command = Command::new(&fish);
            env.apply(&mut command);
            let out = command
                .env("XDG_CONFIG_HOME", env.home.join(".config"))
                .args(["-c", &format!("complete -C '{line}'")])
                .output()
                .unwrap();
            assert!(out.status.success(), "{}", text(&out));
            String::from_utf8_lossy(&out.stdout)
                .lines()
                .map(|l| l.split('\t').next().unwrap().to_string())
                .collect()
        };
        let s = complete("mrsm sh");
        assert!(
            s.contains(&"show".to_string()) && s.contains(&"sh".to_string()),
            "{s:?}"
        );
        assert_eq!(complete("meerschaum show gra"), ["graph"]);
        let s = complete("mrsm show ");
        assert!(
            s.contains(&"pipes".to_string()) && s.contains(&"completions".to_string()),
            "{s:?}"
        );
        assert!(complete("mrsm show pipes -").contains(&"--connector-keys".to_string()));
        assert!(complete("mrsm show pipes -c ").contains(&"sql:main".to_string()));
        assert_eq!(complete("mrsm show pipes -c sql:ma"), ["sql:main"]);
        assert_eq!(complete("mrsm gre"), ["greet"]);
        assert_eq!(complete("mrsm greet --mood "), ["calm", "loud"]);
    }

    #[test]
    fn powershell_completes_the_loaded_script() {
        let Some(pwsh) = which("pwsh") else {
            eprintln!("skipped: pwsh is not installed");
            return;
        };
        let env = Env::new();
        let script = env.base.join("mrsm.ps1");
        let shown = env.run(&["show", "completions", "powershell"]);
        std::fs::write(&script, &shown.stdout).unwrap();
        write_manifest(&env);
        let complete = |line: &str| -> Vec<String> {
            let mut command = Command::new(&pwsh);
            env.apply(&mut command);
            let code = format!(
                ". '{}'; [System.Management.Automation.CommandCompletion]::CompleteInput('{line}', {}, $null).CompletionMatches | ForEach-Object {{ $_.CompletionText }}",
                script.display(),
                line.len()
            );
            let out = command
                .args(["-NoProfile", "-NonInteractive", "-Command", &code])
                .output()
                .unwrap();
            assert!(out.status.success(), "{}", text(&out));
            String::from_utf8_lossy(&out.stdout)
                .lines()
                .map(str::to_string)
                .collect()
        };
        let s = complete("mrsm sh");
        assert!(s.contains(&"show".to_string()), "{s:?}");
        assert!(complete("mrsm show pipes -c ").contains(&"sql:main".to_string()));
        assert_eq!(complete("mrsm gre"), ["greet"]);
    }

    #[test]
    fn powershell_quotes_names_with_shell_specials() {
        let Some(pwsh) = which("pwsh") else {
            eprintln!("skipped: pwsh is not installed");
            return;
        };
        let env = Env::new();
        make_job_dirs(&env, NASTY);
        let script = env.base.join("mrsm.ps1");
        let shown = Command::new(BIN)
            .args(["show", "completions", "powershell"])
            .output()
            .unwrap();
        std::fs::write(&script, &shown.stdout).unwrap();
        let mut command = Command::new(&pwsh);
        env.apply(&mut command);
        let line = "mrsm start jobs ";
        let code = format!(
            ". '{}'; [System.Management.Automation.CommandCompletion]::CompleteInput('{line}', {}, $null).CompletionMatches | ForEach-Object {{ Write-Output $_.CompletionText; Write-Output (Invoke-Expression $_.CompletionText) }}",
            script.display(),
            line.len()
        );
        let out = command
            .args(["-NoProfile", "-NonInteractive", "-Command", &code])
            .output()
            .unwrap();
        assert!(out.status.success(), "{}", text(&out));
        let lines: Vec<String> = String::from_utf8_lossy(&out.stdout)
            .lines()
            .map(str::to_string)
            .collect();
        // Pairs of (inserted text, what PowerShell makes of it).
        let mut names: Vec<String> = lines.chunks(2).map(|c| c[1].clone()).collect();
        names.sort();
        let mut expected: Vec<String> = NASTY.iter().map(|s| s.to_string()).collect();
        expected.sort();
        assert_eq!(names, expected, "{lines:?}");
        for pair in lines.chunks(2) {
            assert!(pair[0].starts_with('\''), "{pair:?}");
        }
        for name in ["pwn", "pwn2"] {
            assert!(!env.base.join(name).exists(), "{name} was created");
        }
    }

    #[test]
    fn fish_offers_names_with_shell_specials_verbatim() {
        let Some(fish) = which("fish") else {
            eprintln!("skipped: fish is not installed");
            return;
        };
        let env = Env::new();
        let out = env.run(&["install", "completions", "fish"]);
        assert!(out.status.success(), "{}", text(&out));
        make_job_dirs(&env, NASTY);
        let mut command = Command::new(&fish);
        env.apply(&mut command);
        let out = command
            .env("XDG_CONFIG_HOME", env.home.join(".config"))
            .args(["-c", "complete -C 'mrsm start jobs '"])
            .output()
            .unwrap();
        let mut got: Vec<String> = String::from_utf8_lossy(&out.stdout)
            .lines()
            .map(|l| l.split('\t').next().unwrap().to_string())
            .collect();
        got.sort();
        let mut expected: Vec<String> = NASTY.iter().map(|s| s.to_string()).collect();
        expected.sort();
        assert_eq!(got, expected);
    }
}
