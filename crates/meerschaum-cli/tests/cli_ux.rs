//! rc15 CLI usability: `mrsm help`, the grouped action list, `show actions` without truncated
//! names, one error print, the undefined-connector message, `mrsm python file.py` and the
//! colors rules (`NO_COLOR`, `FORCE_COLOR`, piped output, jobs).
//!
//! The binary runs against a temporary root with an empty plugins directory and `MRSM_PYTHON`
//! pointing at a program that does not exist (nothing here may start a Python worker); the
//! `python` action runs in process against a recording host.
#![cfg(unix)]

use std::cell::RefCell;
use std::path::Path;
use std::process::{Command, Stdio};
use std::rc::Rc;

use meerschaum_cli::dispatch::{self, CommandSpec, Host, RecordingHost, Runtime};
use meerschaum_cli::format::FormatSettings;
use meerschaum_cli::types::DateContext;
use meerschaum_core::SuccessTuple;
use meerschaum_core::config::{Config, ConfigOptions};

struct Home {
    root: tempfile::TempDir,
    plugins: tempfile::TempDir,
}

struct Out {
    code: i32,
    stdout: String,
    stderr: String,
}

impl Home {
    fn new() -> Home {
        Home {
            root: tempfile::tempdir().unwrap(),
            plugins: tempfile::tempdir().unwrap(),
        }
    }

    fn command(&self, env: &[(&str, &str)]) -> Command {
        let mut command = Command::new(env!("CARGO_BIN_EXE_mrsm-cli"));
        command
            .env_clear()
            .env("PATH", std::env::var_os("PATH").unwrap_or_default())
            .env("HOME", self.root.path())
            .env("MRSM_ROOT_DIR", self.root.path())
            .env("MRSM_PLUGINS_DIR", self.plugins.path())
            .env("MRSM_PYTHON", self.root.path().join("no-python"))
            .env("COLUMNS", "100");
        for (key, value) in env {
            command.env(key, value);
        }
        command
    }

    /// Run `mrsm <args>` with stdout and stderr piped (never a terminal).
    fn run(&self, args: &[&str], env: &[(&str, &str)]) -> Out {
        let output = self
            .command(env)
            .args(args)
            .stdin(Stdio::null())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .output()
            .unwrap();
        Out {
            code: output.status.code().unwrap_or(-1),
            stdout: String::from_utf8_lossy(&output.stdout).into_owned(),
            stderr: String::from_utf8_lossy(&output.stderr).into_owned(),
        }
    }
}

#[test]
fn mrsm_help_prints_the_grouped_actions_then_the_options() {
    let home = Home::new();
    let help = home.run(&["help"], &[]);
    assert_eq!(help.code, 0, "{}{}", help.stdout, help.stderr);
    let text = &help.stdout;
    // Not the system shell's `help` builtin.
    assert!(!text.contains("GNU bash"), "{text}");
    assert!(
        text.starts_with("usage: mrsm [actions] {options}\n"),
        "{text}"
    );
    let mut last = 0;
    for group in [
        "\nCreate:\n",
        "\nSync:\n",
        "\nRun:\n",
        "\nInspect:\n",
        "\nMaintain:\n",
        "\nConfigure:\n",
        "\nActions options:\n",
        "\nSync options:\n",
        "\nPipeline options:\n",
        "\nMiscellaneous options:\n",
    ] {
        let at = text
            .find(group)
            .unwrap_or_else(|| panic!("{group:?}: {text}"));
        assert!(at > last, "{group:?} is out of order:\n{text}");
        last = at;
    }
    assert!(
        text.contains(
            "  bootstrap    Create pipes, connectors, jobs, Pipelines or plugins step by step."
        ),
        "{text}"
    );
    assert!(
        text.contains("Run `mrsm <action> -h` for an action's details"),
        "{text}"
    );
    // `-h`, `--help` and `show help` print the same page.
    for args in [&["-h"][..], &["--help"], &["show", "help"]] {
        let other = home.run(args, &[]);
        assert_eq!(&other.stdout, text, "{args:?}");
    }
    // Per-action help through `help`.
    let sync = home.run(&["help", "sync", "pipes"], &[]);
    assert!(
        sync.stdout
            .starts_with("\nFetch and sync new data for pipes."),
        "{}",
        sync.stdout
    );
    assert!(sync.stdout.contains("--parents"), "{}", sync.stdout);
    assert!(!sync.stdout.contains("v5 additions"), "{}", sync.stdout);
}

#[test]
fn show_actions_never_cuts_a_name_off() {
    let home = Home::new();
    let out = home.run(&["show", "actions"], &[("COLUMNS", "80")]);
    assert_eq!(out.code, 0, "{}", out.stderr);
    assert!(!out.stdout.contains('…'), "{}", out.stdout);
    for name in ["decompress", "deduplicate", "uninstall", "help"] {
        assert!(
            out.stdout.split_whitespace().any(|w| w == name),
            "{name}: {}",
            out.stdout
        );
    }
    assert!(
        out.stdout.lines().all(|l| l.chars().count() <= 80),
        "{}",
        out.stdout
    );
}

#[test]
fn an_undefined_connector_is_reported_once_and_says_where_to_define_it() {
    let home = Home::new();
    let out = home.run(&["show", "pipes", "-i", "sql:nope"], &[]);
    assert_eq!(out.code, 1);
    let all = format!("{}{}", out.stdout, out.stderr);
    let message = "Connector 'sql:nope' is not defined. Add it to the config (`mrsm bootstrap connectors`), set `MRSM_SQL_NOPE`, or install the plugin that provides it.";
    assert_eq!(all.matches(message).count(), 1, "{all}");
    assert!(!all.contains("Missing flavor"), "{all}");
    // The error is not printed a second time before the failure block.
    assert!(!out.stderr.contains("ValueError:"), "{}", out.stderr);
}

#[test]
fn piped_output_is_plain_unless_forced_or_in_a_job() {
    let home = Home::new();
    let has_escape = |out: &Out| out.stdout.contains('\x1b') || out.stderr.contains('\x1b');
    let piped = home.run(&["show", "version"], &[]);
    assert!(!has_escape(&piped), "{piped:?}", piped = piped.stdout);
    assert!(piped.stdout.contains("Meerschaum v"), "{}", piped.stdout);
    let forced = home.run(&["show", "version"], &[("FORCE_COLOR", "1")]);
    assert!(forced.stdout.contains('\x1b'), "{:?}", forced.stdout);
    let in_job = home.run(&["show", "version"], &[("MRSM_DAEMON_ID", "my-job")]);
    assert!(in_job.stdout.contains('\x1b'), "{:?}", in_job.stdout);
    // `NO_COLOR` wins over `FORCE_COLOR`: no styles at all (not even bold).
    let no_color = home.run(
        &["show", "version"],
        &[("FORCE_COLOR", "1"), ("NO_COLOR", "1")],
    );
    assert!(!has_escape(&no_color), "{:?}", no_color.stdout);
    assert_eq!(no_color.stdout, piped.stdout);
    // A failure block too.
    let failed = home.run(&["show", "pipes", "-i", "sql:nope"], &[]);
    assert!(
        !has_escape(&failed),
        "{:?} {:?}",
        failed.stdout,
        failed.stderr
    );
}

#[derive(Clone, Default)]
struct SharedHost(Rc<RefCell<RecordingHost>>);

impl Host for SharedHost {
    fn run(&mut self, spec: &CommandSpec) -> std::io::Result<i32> {
        self.0.borrow_mut().run(spec)
    }

    fn read_line(&mut self, prompt: &str) -> Option<String> {
        self.0.borrow_mut().read_line(prompt)
    }
}

fn config(root: &Path) -> Config {
    let mut opts = ConfigOptions::with_env(vec![
        ("MRSM_ROOT_DIR".into(), root.to_string_lossy().into_owned()),
        (
            "MRSM_PLUGINS_DIR".into(),
            root.join("plugins").to_string_lossy().into_owned(),
        ),
    ]);
    opts.allow_writes = true;
    opts.home = root.to_path_buf();
    Config::new(opts).unwrap()
}

fn run_in_process(root: &Path, sysargs: &[&str], host: &SharedHost) -> (SuccessTuple, String) {
    fn env(key: &str) -> Option<String> {
        (key == "MRSM_PYTHON").then(|| "/opt/py/bin/python".into())
    }
    let mut out = Vec::new();
    let result = {
        let mut rt = Runtime::new(DateContext::system(), 80, &mut out, env);
        rt.config = Some(config(root));
        rt.format = Some(FormatSettings {
            ansi: false,
            unicode: true,
            width: 80,
            cols: 80,
            no_color: false,
        });
        rt.host = Box::new(host.clone());
        rt.cwd = Some(root.to_path_buf());
        let sysargs: Vec<String> = sysargs.iter().map(|s| s.to_string()).collect();
        dispatch::entry(&sysargs, &mut rt)
    };
    (result, String::from_utf8(out).unwrap())
}

#[test]
fn mrsm_python_runs_a_file_with_its_arguments() {
    let tmp = tempfile::tempdir().unwrap();
    std::fs::write(
        tmp.path().join("script.py"),
        "import sys; print(sys.argv)\n",
    )
    .unwrap();
    let host = SharedHost::default();
    // Every word after the file is the script's, Meerschaum's own flags included: none is
    // parsed (`-d` starts no job, `--loop` does not repeat, `-h` prints no help).
    let script_words = [
        "first",
        "--dry-run",
        "-y",
        "-d",
        "--loop",
        "-h",
        "--begin",
        "2024-01-01",
        "-A",
        "--verbose",
        "[x]",
    ];
    let mut sysargs = vec!["python", "script.py"];
    sysargs.extend(script_words);
    let (result, _) = run_in_process(tmp.path(), &sysargs, &host);
    assert_eq!(result, SuccessTuple::success());
    let commands = host.0.borrow().commands.clone();
    assert_eq!(
        commands.len(),
        2,
        "the import probe, then the file: {commands:?}"
    );
    assert_eq!(commands[0].args, ["-c", "import meerschaum"]);
    let run = &commands[1];
    assert_eq!(run.program, "/opt/py/bin/python");
    let mut expected = vec!["script.py"];
    expected.extend(script_words);
    assert_eq!(run.args, expected);
    assert_eq!(run.cwd.as_deref(), Some(tmp.path()));

    // Meerschaum's flags go before the file; `[...]` there are the interpreter's.
    host.0.borrow_mut().commands.clear();
    let (result, _) = run_in_process(
        tmp.path(),
        &["python", "--noask", "[-u]", "script.py", "--noask"],
        &host,
    );
    assert_eq!(result, SuccessTuple::success());
    let commands = host.0.borrow().commands.clone();
    assert_eq!(
        commands.last().unwrap().args,
        ["-u", "script.py", "--noask"]
    );

    host.0.borrow_mut().commands.clear();
    // A `.py` name that does not exist is an error, not code for the REPL.
    let (result, _) = run_in_process(tmp.path(), &["python", "missing.py"], &host);
    assert_eq!(
        result,
        SuccessTuple::fail("No such Python file: 'missing.py'.")
    );
    assert!(host.0.borrow().commands.is_empty(), "nothing ran");

    // Code is still code: the REPL init script.
    let (result, _) = run_in_process(tmp.path(), &["python", "print(1)", "--noask"], &host);
    assert!(result.success, "{result:?}");
    let commands = host.0.borrow().commands.clone();
    assert!(commands.last().unwrap().args[0].ends_with(".py"));
    assert_ne!(commands.last().unwrap().args[0], "print(1)");
}

#[test]
fn mrsm_python_reports_the_exit_code_of_the_file() {
    let tmp = tempfile::tempdir().unwrap();
    std::fs::write(tmp.path().join("fails.py"), "raise SystemExit(3)\n").unwrap();
    // The probe succeeds (exit 0 for the first command), then the file "exits" with 3.
    struct Scripted(Vec<i32>, Rc<RefCell<Vec<CommandSpec>>>);
    impl Host for Scripted {
        fn run(&mut self, spec: &CommandSpec) -> std::io::Result<i32> {
            self.1.borrow_mut().push(spec.clone());
            Ok(if self.0.is_empty() {
                0
            } else {
                self.0.remove(0)
            })
        }
        fn read_line(&mut self, _prompt: &str) -> Option<String> {
            None
        }
    }
    let seen = Rc::new(RefCell::new(Vec::new()));
    let mut out = Vec::new();
    let result = {
        let mut rt = Runtime::new(DateContext::system(), 80, &mut out, |_| None);
        rt.config = Some(config(tmp.path()));
        rt.host = Box::new(Scripted(vec![0, 3], seen.clone()));
        rt.cwd = Some(tmp.path().to_path_buf());
        dispatch::entry(&["python".into(), "fails.py".into()], &mut rt)
    };
    assert_eq!(
        result,
        SuccessTuple::fail("Python file 'fails.py' exited with code 3.")
    );
    assert_eq!(seen.borrow().len(), 2);
}

/// `compose ... --allow-protected` forwards the flag, like `--debug`, to every command it runs
/// (in-process steps, subprocesses and the jobs it starts), and the flag does not change a job's
/// identity.
#[test]
fn compose_forwards_allow_protected_like_debug() {
    use meerschaum_compose::command::{RunOptions, build_command_with_env};
    use meerschaum_core::value::{Map, Value};
    let mut config = Map::new();
    config.insert("project_name".into(), Value::Str("demo".into()));
    let process_env = meerschaum_compose::env::ProcessEnv::new();
    let args: Vec<String> = ["sync", "pipes"].iter().map(|s| s.to_string()).collect();
    let mut msgs = meerschaum_compose::Messages::default();
    let forwarded = build_command_with_env(
        &args,
        &config,
        &process_env,
        None,
        RunOptions {
            allow_protected: true,
            debug: true,
            ..RunOptions::default()
        },
        &mut msgs,
    )
    .unwrap();
    assert!(
        forwarded.sysargs.iter().any(|a| a == "--allow-protected"),
        "{:?}",
        forwarded.sysargs
    );
    let plain = build_command_with_env(
        &args,
        &config,
        &process_env,
        None,
        RunOptions::default(),
        &mut msgs,
    )
    .unwrap();
    assert!(!plain.sysargs.iter().any(|a| a == "--allow-protected"));
    // Given twice, it is not repeated.
    let mut given = args.clone();
    given.push("--allow-protected".into());
    let once = build_command_with_env(
        &given,
        &config,
        &process_env,
        None,
        RunOptions {
            allow_protected: true,
            ..RunOptions::default()
        },
        &mut msgs,
    )
    .unwrap();
    assert_eq!(
        once.sysargs
            .iter()
            .filter(|a| *a == "--allow-protected")
            .count(),
        1
    );
    assert!(meerschaum_compose::jobs::NON_IDENTIFYING_FLAGS.contains(&"--allow-protected"));
}

/// A job keeps its styles (its process is a job's: `MRSM_DAEMON_ID`), so its log is colored for
/// the console and `show logs` on a terminal; `show logs` off a terminal prints it plain.
#[test]
fn job_logs_keep_their_colors_and_print_plain_off_a_terminal() {
    let home = Home::new();
    let started = home.run(
        &["show", "version", "-d", "--name", "colorful", "--noask"],
        &[("MRSM_NO_UPDATE_CHECK", "1")],
    );
    assert_eq!(started.code, 0, "{}{}", started.stdout, started.stderr);
    // The rotating log's files: `logs/colorful.log.0`, ...
    let logs = home.root.path().join("logs");
    let read_logs = || -> String {
        let mut text = String::new();
        for entry in std::fs::read_dir(&logs).into_iter().flatten().flatten() {
            if entry
                .file_name()
                .to_string_lossy()
                .starts_with("colorful.log")
            {
                text.push_str(&std::fs::read_to_string(entry.path()).unwrap_or_default());
            }
        }
        text
    };
    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(60);
    let raw = loop {
        let text = read_logs();
        if text.contains("Meerschaum v") {
            break text;
        }
        assert!(
            std::time::Instant::now() < deadline,
            "the job never logged its output: {text:?}"
        );
        std::thread::sleep(std::time::Duration::from_millis(200));
    };
    assert!(
        raw.contains('\x1b'),
        "the job's log keeps its colors: {raw:?}"
    );
    let shown = home.run(&["show", "logs", "colorful", "--nopretty"], &[]);
    assert!(shown.stdout.contains("Meerschaum v"), "{}", shown.stdout);
    assert!(!shown.stdout.contains('\x1b'), "{:?}", shown.stdout);
    let forced = home.run(
        &["show", "logs", "colorful", "--nopretty"],
        &[("FORCE_COLOR", "1")],
    );
    assert!(forced.stdout.contains('\x1b'), "{:?}", forced.stdout);
    let deleted = home.run(&["delete", "jobs", "colorful", "-y"], &[]);
    assert_eq!(deleted.code, 0, "{}{}", deleted.stdout, deleted.stderr);
}

/// A writer the test reads back.
#[derive(Clone, Default)]
struct Buf(Rc<RefCell<Vec<u8>>>);

impl std::io::Write for Buf {
    fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
        self.0.borrow_mut().extend_from_slice(buf);
        Ok(buf.len())
    }

    fn flush(&mut self) -> std::io::Result<()> {
        Ok(())
    }
}

/// Warnings and debug output are styled by stderr's own terminal check (rc15 Q14): `2> err.log`
/// gets plain text while stdout is a terminal, and a terminal stderr keeps its styles while
/// stdout goes to a file.
#[test]
fn stderr_styles_follow_stderr() {
    let tmp = tempfile::tempdir().unwrap();
    for (out_terminal, err_terminal) in [(true, false), (false, true)] {
        let err = Buf::default();
        let mut out = Vec::new();
        {
            let mut rt = Runtime::new(DateContext::system(), 80, &mut out, |_| None);
            rt.config = Some(config(tmp.path()));
            rt.err = Box::new(err.clone());
            rt.out_terminal = Some(out_terminal);
            rt.err_terminal = Some(err_terminal);
            assert_eq!(rt.settings().ansi, out_terminal);
            assert_eq!(rt.err_settings().ansi, err_terminal);
            meerschaum_cli::native::warn(&mut rt, "A warning.");
            // Text styled for stdout loses its styles on a stderr that takes none.
            let styled = rt.with_formatter(|f| f.warn("Styled for stdout."));
            rt.eprint(&styled);
            let debug =
                rt.with_err_formatter(meerschaum_cli::debug_log::DprintStyle::from_formatter);
            assert_eq!(debug.style.is_some(), err_terminal);
        }
        let text = String::from_utf8(err.0.borrow().clone()).unwrap();
        assert!(
            text.contains("A warning.") && text.contains("Styled for stdout."),
            "{text}"
        );
        assert_eq!(text.contains("\x1b["), err_terminal, "{text:?}");
    }
    // A runtime with explicit settings keeps them for both streams (tests, embedded hosts).
    let err = Buf::default();
    let mut out = Vec::new();
    {
        let mut rt = Runtime::new(DateContext::system(), 80, &mut out, |_| None);
        rt.config = Some(config(tmp.path()));
        rt.err = Box::new(err.clone());
        rt.err_terminal = Some(false);
        rt.format = Some(FormatSettings {
            ansi: true,
            unicode: true,
            width: 80,
            cols: 80,
            no_color: false,
        });
        meerschaum_cli::native::warn(&mut rt, "Forced.");
    }
    assert!(
        String::from_utf8(err.0.borrow().clone())
            .unwrap()
            .contains("\x1b[")
    );
}

/// `FORCE_COLOR` for a relayed child follows this process's own decision.
#[test]
fn relayed_children_style_like_this_process() {
    let settings = |ansi| FormatSettings {
        ansi,
        unicode: true,
        width: 80,
        cols: 80,
        no_color: false,
    };
    assert_eq!(
        meerschaum_cli::format::relay_env(&settings(true)),
        Some(("FORCE_COLOR", "1"))
    );
    assert_eq!(meerschaum_cli::format::relay_env(&settings(false)), None);
}
