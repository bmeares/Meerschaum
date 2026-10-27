//! Shell behavior: `precmd` routing (shell commands, `!` escapes, `?` help, chained and
//! pipeline commands, the session instance patched into each step), the non-interactive loop,
//! completion, and the prompt and banner with ANSI disabled.

use std::cell::RefCell;
use std::io::Write;
use std::rc::Rc;

use meerschaum_cli::dispatch::{self, CommandSpec, Host, RecordingHost, Runtime};
use meerschaum_cli::format::FormatSettings;
use meerschaum_cli::shell::{self, Control, PromptState, ShellState};
use meerschaum_cli::types::DateContext;
use meerschaum_core::config::{Config, ConfigOptions};

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

#[derive(Clone, Default)]
struct SharedBuf(Rc<RefCell<Vec<u8>>>);

impl Write for SharedBuf {
    fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
        self.0.borrow_mut().extend_from_slice(buf);
        Ok(buf.len())
    }

    fn flush(&mut self) -> std::io::Result<()> {
        Ok(())
    }
}

fn config(root: &std::path::Path) -> Config {
    let mut opts = ConfigOptions::with_env(vec![
        ("MRSM_ROOT_DIR".into(), root.to_string_lossy().into_owned()),
        (
            "MRSM_PLUGINS_DIR".into(),
            root.join("plugins").to_string_lossy().into_owned(),
        ),
    ]);
    opts.allow_writes = false;
    opts.home = root.to_path_buf();
    Config::new(opts).unwrap()
}

fn settings(ansi: bool) -> FormatSettings {
    FormatSettings {
        ansi,
        unicode: true,
        width: 80,
        cols: 80,
        no_color: false,
    }
}

struct Harness {
    _tmp: tempfile::TempDir,
    root: std::path::PathBuf,
    host: SharedHost,
    err: SharedBuf,
}

impl Harness {
    fn new() -> Harness {
        let tmp = tempfile::tempdir().unwrap();
        let root = tmp.path().to_path_buf();
        Harness {
            _tmp: tmp,
            root,
            host: SharedHost::default(),
            err: SharedBuf::default(),
        }
    }

    fn runtime<'a>(&self, out: &'a mut Vec<u8>, env: fn(&str) -> Option<String>) -> Runtime<'a> {
        let mut rt = Runtime::new(DateContext::system(), 80, out, env);
        rt.config = Some(config(&self.root));
        rt.format = Some(settings(false));
        rt.host = Box::new(self.host.clone());
        rt.err = Box::new(self.err.clone());
        rt
    }

    fn commands(&self) -> Vec<CommandSpec> {
        self.host.0.borrow().commands.clone()
    }
}

fn no_env(_: &str) -> Option<String> {
    None
}

#[test]
fn shell_commands_change_the_session() {
    let h = Harness::new();
    let mut out = Vec::new();
    let mut rt = h.runtime(&mut out, no_env);
    let mut state = ShellState::new(&mut rt, None);
    assert_eq!(state.instance_keys, "sql:main");

    assert_eq!(
        shell::precmd("instance sql:local", &mut state, &mut rt),
        Control::Continue
    );
    assert_eq!(state.instance_keys, "sql:local");
    // An unconfigured connector falls back to the configured default instance.
    shell::precmd("instance nope", &mut state, &mut rt);
    assert_eq!(state.instance_keys, "sql:main");
    shell::precmd("instance api", &mut state, &mut rt);
    assert_eq!(state.instance_keys, "api:main");

    shell::precmd("debug", &mut state, &mut rt);
    assert!(state.debug);
    shell::precmd("debug off", &mut state, &mut rt);
    assert!(!state.debug);
    shell::precmd("repo mrsm", &mut state, &mut rt);
    assert_eq!(state.repo_keys, "api:mrsm");

    assert_eq!(shell::precmd("exit", &mut state, &mut rt), Control::Exit);
    assert_eq!(shell::precmd("quit", &mut state, &mut rt), Control::Exit);
    drop(rt);
    let out = String::from_utf8(out).unwrap();
    assert!(
        out.contains(" 💬 Default instance for the current shell: sql:local\n"),
        "{out}"
    );
    assert!(
        out.contains(" 💬 Default instance for the current shell: api:main\n"),
        "{out}"
    );
    assert!(out.contains(" 💬 Debug mode is on.\n"), "{out}");
}

#[test]
fn bang_and_unknown_actions_run_through_sh() {
    let h = Harness::new();
    let mut out = Vec::new();
    let mut rt = h.runtime(&mut out, no_env);
    let mut state = ShellState::new(&mut rt, None);
    shell::precmd("!echo hi", &mut state, &mut rt);
    shell::precmd("ls -la", &mut state, &mut rt);
    drop(rt);
    let bash: Vec<Vec<String>> = h
        .commands()
        .into_iter()
        .filter(|c| c.program == "bash")
        .map(|c| c.args)
        .collect();
    assert_eq!(
        bash,
        vec![
            vec!["-c".to_string(), "echo hi".to_string()],
            // PARITY: `-la` parses as `-l a` (location keys), so only `ls` reaches `sh`.
            vec!["-c".to_string(), "ls".to_string()],
        ]
    );
}

#[test]
fn help_shortcuts_print_the_docstrings() {
    let h = Harness::new();
    let mut expected = Vec::new();
    {
        let mut rt = h.runtime(&mut expected, no_env);
        dispatch::parse_help(&["show".into(), "pipes".into(), "--help".into()], &mut rt);
    }
    for line in ["help show pipes", "? show pipes", "show pipes --help"] {
        let mut out = Vec::new();
        let mut rt = h.runtime(&mut out, no_env);
        rt.format = Some(settings(false));
        let mut state = ShellState::new(&mut rt, None);
        shell::precmd(line, &mut state, &mut rt);
        drop(rt);
        assert_eq!(
            String::from_utf8(out).unwrap(),
            String::from_utf8(expected.clone()).unwrap(),
            "{line}"
        );
    }
}

#[test]
fn session_instance_is_patched_into_each_step() {
    let h = Harness::new();
    let mut out = Vec::new();
    let mut rt = h.runtime(&mut out, no_env);
    let mut state = ShellState::new(&mut rt, Some("sql:local"));
    shell::precmd(
        "show arguments + show version --nopretty",
        &mut state,
        &mut rt,
    );
    drop(rt);
    let out = String::from_utf8(out).unwrap();
    assert!(out.contains("'mrsm_instance': 'sql:local'"), "{out}");
    assert!(out.contains("'shell': True"), "{out}");
    assert!(
        out.contains(&format!("{}\n", meerschaum_cli::VERSION)),
        "{out}"
    );
}

#[test]
fn noninteractive_loop_reads_lines_and_closes() {
    fn env(k: &str) -> Option<String> {
        (k == "MRSM_NONINTERACTIVE").then(|| "1".to_string())
    }
    let h = Harness::new();
    h.host.0.borrow_mut().input = vec!["show version".into(), "exit".into()];
    let mut out = Vec::new();
    {
        let mut rt = h.runtime(&mut out, env);
        shell::run(&[], None, &mut rt);
    }
    let out = String::from_utf8(out).unwrap();
    assert!(out.starts_with("\n █▄ ▄█ ██▀"), "{out}");
    assert!(
        out.contains(&format!("Meerschaum v{}", meerschaum_cli::VERSION)),
        "{out}"
    );
    assert!(
        out.ends_with("\n MRSM{formatting:emoji:hand} Thank you for using Meerschaum! \n")
            || out.ends_with("\n 👋 Thank you for using Meerschaum! \n"),
        "{out:?}"
    );
}

#[test]
fn prompt_and_banner_without_ansi() {
    let h = Harness::new();
    let cfg = config(&h.root);
    let f = meerschaum_cli::format::Formatter::new(settings(false), &cfg);
    assert_eq!(
        shell::render_prompt(&f, &PromptState::new(&f, "sql:main", None)),
        "\n [ mrsm@sql:main ] ➤ "
    );
    assert_eq!(
        shell::render_prompt(&f, &PromptState::new(&f, "sql:local", Some("proj"))),
        "\n [ (no username)@sql:local | proj ] ➤ "
    );
    assert_eq!(
        shell::shell_intro(&f, true, "5.0.0"),
        "\n █▄ ▄█ ██▀ ██▀ █▀▄ ▄▀▀ ▄▀▀ █▄█ ▄▀▄ █ █ █▄ ▄█\n █ ▀ █ █▄▄ █▄▄ █▀▄ ▄██ ▀▄▄ █ █ █▀█ ▀▄█ █ ▀ █\n\n                                      v5.0.0"
    );
}

/// v4's `test_compose_shell_prompt_shows_the_active_project`: the prompt names the Compose
/// project of the environment (`MRSM__COMPOSE_CONFIG`), even under a pre-4.0 prompt template
/// which has no `{compose}` slot, and shows nothing outside a project.
#[test]
fn the_prompt_names_the_active_compose_project() {
    fn in_project(key: &str) -> Option<String> {
        (key == "MRSM__COMPOSE_CONFIG")
            .then(|| r#"{"__file__": "/tmp/awesome/mrsm-compose.yaml"}"#.to_string())
    }
    fn named(key: &str) -> Option<String> {
        (key == "MRSM__COMPOSE_CONFIG").then(|| r#"{"project_name": "explicit"}"#.to_string())
    }
    let h = Harness::new();
    let prompt = |env: fn(&str) -> Option<String>| {
        let mut out = Vec::new();
        let mut rt = h.runtime(&mut out, env);
        let state = ShellState::new(&mut rt, Some("sql:local"));
        shell::current_prompt(&state, &mut rt)
    };
    // The project is the directory of the compose file, or its `project_name`.
    assert!(
        prompt(in_project).contains(" | awesome "),
        "{}",
        prompt(in_project)
    );
    assert!(prompt(named).contains(" | explicit "), "{}", prompt(named));
    assert!(!prompt(no_env).contains("awesome"), "{}", prompt(no_env));
    assert!(!prompt(no_env).contains(" | "), "{}", prompt(no_env));

    // A prompt written before 4.0 (`... | {executor_keys}`) gets the project in that slot.
    let mut opts = ConfigOptions::with_env(vec![
        (
            "MRSM_ROOT_DIR".into(),
            h.root.to_string_lossy().into_owned(),
        ),
        (
            "MRSM_PLUGINS_DIR".into(),
            h.root.join("plugins").to_string_lossy().into_owned(),
        ),
        (
            "MRSM_CONFIG".into(),
            r#"{"shell": {"unicode": {"prompt": "\n [ {username}@{instance} | {executor_keys} ] ➤ "}}}"#
                .into(),
        ),
    ]);
    opts.allow_writes = false;
    opts.home = h.root.clone();
    let cfg = Config::new(opts).unwrap();
    let f = meerschaum_cli::format::Formatter::new(settings(false), &cfg);
    let template = shell::prompt_template(&f);
    assert!(
        template.contains("{compose}") && !template.contains("{executor_keys}"),
        "{template}"
    );
    assert_eq!(
        shell::render_prompt(&f, &PromptState::new(&f, "sql:local", Some("awesome"))),
        "\n [ (no username)@sql:local | awesome ] ➤ "
    );
}

#[test]
fn completion_candidates() {
    let h = Harness::new();
    let mut out = Vec::new();
    let mut rt = h.runtime(&mut out, no_env);
    let state = ShellState::new(&mut rt, None);
    let data = shell::completion_data(&state, &mut rt);
    let dates = DateContext::system();
    let texts = |line: &str| -> Vec<String> {
        data.get_completions(line, &dates)
            .into_iter()
            .map(|c| c.text)
            .collect()
    };
    assert_eq!(texts("syn"), vec!["sync"]);
    assert_eq!(texts("show pi"), vec!["pipelines", "pipes"]);
    assert_eq!(texts("show pipes -i sql:l"), vec!["sql:local"]);
    assert!(texts("sync pipes --lo").contains(&"--loop".to_string()));
    assert_eq!(texts("show config sys"), vec!["system"]);
    assert_eq!(texts("compose ex"), vec!["explain"]);
    assert!(texts("instance ").contains(&"sql:main".to_string()));
}

/// `python file.py ...` in the shell: the shell adds its instance and debug flags to the line it
/// runs, and they stay Meerschaum's; every word after the file still reaches the script
/// unchanged (`-h` included).
#[test]
fn python_files_get_their_own_words_in_the_shell() {
    let h = Harness::new();
    let script = h.root.join("s.py");
    std::fs::write(&script, "import sys; print(sys.argv)\n").unwrap();
    let script = script.display().to_string();
    let mut out = Vec::new();
    let mut rt = h.runtime(&mut out, no_env);
    let mut state = ShellState::new(&mut rt, Some("sql:local"));
    shell::precmd("debug", &mut state, &mut rt);
    let line = format!("python {script} --x 'a b' -i sql:other -h");
    shell::precmd(&line, &mut state, &mut rt);
    drop(rt);
    let runs: Vec<Vec<String>> = h
        .commands()
        .into_iter()
        .filter(|c| c.args.first() == Some(&script))
        .map(|c| c.args)
        .collect();
    assert_eq!(
        runs,
        vec![vec![
            script.clone(),
            "--x".into(),
            "a b".into(),
            "-i".into(),
            "sql:other".into(),
            "-h".into()
        ]],
        "{}",
        String::from_utf8_lossy(&out)
    );
    let out = String::from_utf8(out).unwrap();
    assert!(
        !out.contains("Open a Python interpreter with Meerschaum imported"),
        "no help was printed: {out}"
    );
}
