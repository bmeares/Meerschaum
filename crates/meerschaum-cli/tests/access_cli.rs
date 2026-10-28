//! The access commands of rc15 wave 2 (AC10, FX2, the `copy pipes` rule of AC4) on SQLite
//! instances in a temporary root:
//!
//! - in process, through the dispatcher with scripted answers and a scripted editor: `register
//!   users` (the first user becomes an admin), `show users`, `register|edit|show|delete teams`
//!   (the editor renames a team, and its pipes follow), `edit pipes access` (flags and prompts),
//!   `lock|unlock pipes`, `edit users` (role and primary team) and `copy pipes`;
//! - through the `mrsm` binary: the same commands driven by flags, and `show connectors`, which
//!   never prints a secret.
//!
//! The data is synthetic; nothing outside the temporary directories is touched.

use std::path::{Path, PathBuf};

use meerschaum_cli::dispatch::{self, CommandSpec, Host, Runtime};
use meerschaum_cli::format::FormatSettings;
use meerschaum_cli::types::DateContext;
use meerschaum_core::SuccessTuple;
use meerschaum_core::access::{EditMode, Locks, PipeAccess, Visibility};
use meerschaum_core::config::{Config, ConfigOptions};
use meerschaum_core::value::Map;

fn no_env(_: &str) -> Option<String> {
    None
}

/// A `Write` the test reads back (the runtime's stderr).
#[derive(Clone, Default)]
struct Shared(std::rc::Rc<std::cell::RefCell<Vec<u8>>>);

impl std::io::Write for Shared {
    fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
        self.0.borrow_mut().extend_from_slice(buf);
        Ok(buf.len())
    }

    fn flush(&mut self) -> std::io::Result<()> {
        Ok(())
    }
}

/// A host that answers prompts from a script and plays the editor: each run of a program edits
/// the file it was given with the next list of replacements.
#[derive(Default)]
struct ScriptedHost {
    input: Vec<String>,
    edits: Vec<Vec<(String, String)>>,
    prompts: std::rc::Rc<std::cell::RefCell<Vec<String>>>,
}

impl Host for ScriptedHost {
    fn run(&mut self, spec: &CommandSpec) -> std::io::Result<i32> {
        let path = PathBuf::from(spec.args.last().cloned().unwrap_or_default());
        if !self.edits.is_empty() {
            let mut text = std::fs::read_to_string(&path)?;
            for (old, new) in self.edits.remove(0) {
                assert!(
                    text.contains(&old),
                    "the editor file has no {old:?}:\n{text}"
                );
                text = text.replace(&old, &new);
            }
            std::fs::write(&path, text)?;
        }
        Ok(0)
    }

    fn read_line(&mut self, prompt: &str) -> Option<String> {
        self.prompts.borrow_mut().push(prompt.to_string());
        if self.input.is_empty() {
            None
        } else {
            Some(self.input.remove(0))
        }
    }
}

struct Home {
    _dir: tempfile::TempDir,
    root: PathBuf,
    db: PathBuf,
    db2: PathBuf,
}

impl Home {
    fn new() -> Home {
        let dir = tempfile::tempdir().unwrap();
        let root = dir.path().join("root");
        std::fs::create_dir_all(root.join("plugins")).unwrap();
        Home {
            db: dir.path().join("p2.db"),
            db2: dir.path().join("p2b.db"),
            root,
            _dir: dir,
        }
    }

    fn env(&self) -> Vec<(String, String)> {
        vec![
            ("MRSM_ROOT_DIR".into(), self.root.to_string_lossy().into_owned()),
            (
                "MRSM_PLUGINS_DIR".into(),
                self.root.join("plugins").to_string_lossy().into_owned(),
            ),
            (
                "MRSM_SQL_P2".into(),
                format!("sqlite:///{}", self.db.to_string_lossy()),
            ),
            (
                "MRSM_SQL_P2B".into(),
                format!("sqlite:///{}", self.db2.to_string_lossy()),
            ),
            (
                "MRSM_SQL_SECRETIVE".into(),
                "postgresql://mrsm:hunter2pw@db.example.com:5432/meerschaum".into(),
            ),
            // A connector type no plugin declares lives in the configuration (here a patch).
            (
                "MRSM_CONFIG".into(),
                r#"{"meerschaum": {"connectors": {"webhook": {"alerts": {"url": "https://hooks.example.com/services/T0/B1/XyZsecretpath", "api_key": "k3y-abc-123", "channel": "alerts"}}}}}"#
                    .into(),
            ),
        ]
    }

    /// `mrsm <sysargs>` in process: the result, stdout then stderr, and the prompts asked.
    fn run(
        &self,
        sysargs: &[&str],
        answers: &[&str],
        edits: Vec<Vec<(&str, &str)>>,
    ) -> (SuccessTuple, String, Vec<String>) {
        let mut out = Vec::new();
        let err = Shared::default();
        let prompts = std::rc::Rc::new(std::cell::RefCell::new(Vec::new()));
        let result = {
            let mut rt = Runtime::new(DateContext::system(), 160, &mut out, no_env);
            rt.err = Box::new(err.clone());
            let mut opts = ConfigOptions::with_env(self.env());
            opts.allow_writes = true;
            opts.home = self.root.clone();
            rt.config = Some(Config::new(opts).unwrap());
            rt.format = Some(FormatSettings {
                ansi: false,
                unicode: true,
                width: 160,
                cols: 160,
                no_color: true,
            });
            rt.host = Box::new(ScriptedHost {
                input: answers.iter().map(|a| a.to_string()).collect(),
                edits: edits
                    .into_iter()
                    .map(|e| {
                        e.into_iter()
                            .map(|(a, b)| (a.to_string(), b.to_string()))
                            .collect()
                    })
                    .collect(),
                prompts: prompts.clone(),
            });
            rt.cwd = Some(self.root.clone());
            let sysargs: Vec<String> = sysargs.iter().map(|s| s.to_string()).collect();
            dispatch::entry(&sysargs, &mut rt)
        };
        let prompts = prompts.borrow().clone();
        let mut text = String::from_utf8(out).unwrap();
        text.push_str(&String::from_utf8_lossy(&err.0.borrow()));
        (result, text, prompts)
    }

    fn ok(&self, sysargs: &[&str], answers: &[&str]) -> String {
        let (result, out, _) = self.run(sysargs, answers, Vec::new());
        assert!(
            result.success,
            "{sysargs:?} failed: {}\n{out}",
            result.message
        );
        out
    }

    /// The registered parameters of a pipe on a database.
    fn parameters(&self, db: &Path, ck: &str, mk: &str) -> Map {
        let rt = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap();
        rt.block_on(async {
            let conn = meerschaum_connector_sql::SqlConnector::from_uri(
                "p2_check",
                &format!("sqlite:///{}", db.to_string_lossy()),
            )
            .await
            .unwrap();
            let pipe = meerschaum_core::Pipe::builder(ck, mk)
                .instance("sql:p2_check")
                .build()
                .unwrap();
            let attrs = conn.get_pipe_attributes(&pipe).await.unwrap();
            attrs
                .get("parameters")
                .and_then(|p| p.as_map())
                .cloned()
                .unwrap_or_default()
        })
    }
}

#[test]
fn teams_users_access_and_locks_in_process() {
    let home = Home::new();
    let with_i = |args: &[&'static str]| -> Vec<&'static str> {
        let mut v = args.to_vec();
        v.extend(["-i", "sql:p2"]);
        v
    };

    // The first user of an empty instance becomes an admin (the default answer); the others
    // follow the default role.
    let (result, out, prompts) = home.run(
        &with_i(&["register", "users", "alice", "bob", "carol"]),
        &[
            "",
            "password123",
            "password123",
            "alice@example.com",
            "password123",
            "password123",
            "",
            "password123",
            "password123",
            "carol@example.com",
        ],
        Vec::new(),
    );
    assert!(result.success, "{}\n{out}", result.message);
    assert!(
        prompts[0]
            .contains("No users are registered on instance 'sql:p2' yet. Make 'alice' an admin?"),
        "{prompts:?}"
    );
    assert_eq!(
        prompts.iter().filter(|p| p.contains("an admin?")).count(),
        1,
        "only the first user is offered: {prompts:?}"
    );

    let out = home.ok(&with_i(&["show", "users"]), &[]);
    for header in ["User", "Type", "Role", "Teams", "Email"] {
        assert!(out.contains(header), "{out}");
    }
    let line = |name: &str| -> String {
        out.lines()
            .find(|l| l.contains(&format!(" {name} ")))
            .unwrap_or_else(|| panic!("no row for {name}:\n{out}"))
            .to_string()
    };
    assert!(
        line("alice").contains("admin") && line("alice").contains("Admin"),
        "{out}"
    );
    assert!(line("alice").contains("alice@example.com"), "{out}");
    assert!(
        line("bob").contains("regular") && line("bob").contains("Editor (default)"),
        "{out}"
    );
    // `--nopretty`: one JSON object per user.
    let plain = home.ok(&with_i(&["show", "users", "--nopretty"]), &[]);
    let rows: Vec<serde_json::Value> = plain
        .lines()
        .map(|l| serde_json::from_str(l).unwrap())
        .collect();
    assert_eq!(rows.len(), 3, "{plain}");
    assert_eq!(rows[0]["username"], "alice");
    assert_eq!(rows[0]["type"], "admin");
    assert_eq!(rows[0]["role"], "admin");
    assert_eq!(rows[1]["role"], "editor");
    assert_eq!(rows[1]["default_role"], true);
    assert_eq!(rows[1]["email"], serde_json::Value::Null);

    // Teams.
    home.ok(
        &with_i(&[
            "register",
            "teams",
            "analytics",
            "--members",
            "bob",
            "--managers",
            "carol",
        ]),
        &[],
    );
    let (result, out, _) = home.run(
        &with_i(&["register", "teams", "ops", "--members", "nobody"]),
        &[],
        Vec::new(),
    );
    assert!(!result.success);
    assert!(
        result.message.contains("nobody"),
        "{}\n{out}",
        result.message
    );
    let (result, _, _) = home.run(&with_i(&["register", "team", "everyone"]), &[], Vec::new());
    assert!(
        !result.success && result.message.contains("built-in"),
        "{}",
        result.message
    );

    let out = home.ok(&with_i(&["show", "teams"]), &[]);
    assert!(
        out.contains("Everyone") && out.contains("every user"),
        "{out}"
    );
    let analytics = out.lines().find(|l| l.contains("analytics")).unwrap();
    assert!(
        analytics.contains("carol") && analytics.contains("bob"),
        "{out}"
    );
    let json = home.ok(&with_i(&["show", "teams", "analytics", "--nopretty"]), &[]);
    let team: serde_json::Value = serde_json::from_str(json.trim()).unwrap();
    assert_eq!(team["name"], "analytics");
    assert_eq!(team["managers"], serde_json::json!(["carol"]));
    assert_eq!(team["members"], serde_json::json!(["bob", "carol"]));

    let out = home.ok(&with_i(&["show", "users"]), &[]);
    let carol = out.lines().find(|l| l.contains(" carol ")).unwrap();
    assert!(carol.contains("analytics (manager)"), "{out}");

    // Pipes and their access.
    home.ok(
        &with_i(&[
            "register", "pipes", "-c", "sql:p2", "-m", "weather", "temps",
        ]),
        &[],
    );
    home.ok(
        &with_i(&[
            "edit",
            "pipes",
            "access",
            "-c",
            "sql:p2",
            "-m",
            "weather",
            "--owner",
            "bob",
            "--team",
            "Analytics",
            "--visibility",
            "private",
            "--edit",
            "owner",
        ]),
        &[],
    );
    let access = PipeAccess::from_parameters(&home.parameters(&home.db, "sql:p2", "weather"));
    assert_eq!(
        access,
        PipeAccess {
            owner: Some("bob".into()),
            // Team names compare without case; the registered name is stored.
            team: Some("analytics".into()),
            visibility: Visibility::Private,
            edit: EditMode::Owner,
        }
    );
    // An unregistered owner or team is refused.
    let (result, _, _) = home.run(
        &with_i(&[
            "edit", "pipes", "access", "-m", "temps", "--owner", "zed", "--noask",
        ]),
        &[],
        Vec::new(),
    );
    assert!(
        !result.success && result.message.contains("zed"),
        "{}",
        result.message
    );
    let (result, _, _) = home.run(
        &with_i(&[
            "edit", "pipes", "access", "-m", "temps", "--team", "nope", "--noask",
        ]),
        &[],
        Vec::new(),
    );
    assert!(
        !result.success && result.message.contains("nope"),
        "{}",
        result.message
    );

    // The fields the flags leave out are asked for: owner, team (numbered), visibility, edit.
    let (result, out, prompts) = home.run(
        &with_i(&["edit", "pipes", "access", "-c", "sql:p2", "-m", "temps"]),
        &["carol", "2", "everyone", "team"],
        Vec::new(),
    );
    assert!(result.success, "{}\n{out}", result.message);
    assert!(
        prompts[0].contains("Owner (a username, or none):"),
        "{prompts:?}"
    );
    let access = PipeAccess::from_parameters(&home.parameters(&home.db, "sql:p2", "temps"));
    assert_eq!(access.owner.as_deref(), Some("carol"));
    assert_eq!(access.team.as_deref(), Some("analytics"));
    assert_eq!(access.visibility, Visibility::Everyone);
    assert_eq!(access.edit, EditMode::Team);
    // `--noask` keeps what the flags leave out.
    home.ok(
        &with_i(&[
            "edit", "pipes", "access", "-m", "temps", "--owner", "none", "--noask",
        ]),
        &[],
    );
    let access = PipeAccess::from_parameters(&home.parameters(&home.db, "sql:p2", "temps"));
    assert_eq!(access.owner, None);
    assert_eq!(access.visibility, Visibility::Everyone);

    // Locks.
    home.ok(
        &with_i(&[
            "lock",
            "pipes",
            "-c",
            "sql:p2",
            "-m",
            "weather",
            "--locks",
            "drop,sync",
        ]),
        &[],
    );
    let locks = Locks::from_parameters(&home.parameters(&home.db, "sql:p2", "weather"));
    assert_eq!(locks.names(), ["sync", "drop"]);
    let (result, out, _) = home.run(
        &with_i(&["lock", "pipes", "-m", "weather", "--locks", "drop,rename"]),
        &[],
        Vec::new(),
    );
    assert!(
        !result.success && result.message.contains("'rename'"),
        "{}\n{out}",
        result.message
    );
    // A lock refuses the operation, `-y` / `--force` included.
    let (result, out, _) = home.run(
        &with_i(&[
            "drop", "pipes", "-c", "sql:p2", "-m", "weather", "-y", "--force",
        ]),
        &[],
        Vec::new(),
    );
    let text = format!("{}\n{out}", result.message);
    assert!(text.contains("locked (drop)"), "{text}");
    assert!(text.contains("mrsm unlock pipes"), "{text}");
    home.ok(
        &with_i(&["unlock", "pipes", "-m", "weather", "--locks", "sync"]),
        &[],
    );
    let locks = Locks::from_parameters(&home.parameters(&home.db, "sql:p2", "weather"));
    assert_eq!(locks.names(), ["drop"]);

    // A copy keeps the locks and never the access.
    home.ok(
        &with_i(&[
            "copy", "pipes", "sql:p2b", "-c", "sql:p2", "-m", "weather", "-y",
        ]),
        &[],
    );
    let copied = home.parameters(&home.db2, "sql:p2", "weather");
    assert_eq!(Locks::from_parameters(&copied).names(), ["drop"]);
    assert!(!copied.contains_key("access"), "{copied:?}");

    // The editor renames a team: its pipes follow.
    let (result, out, prompts) = home.run(
        &with_i(&["edit", "teams", "analytics"]),
        &["y"],
        vec![vec![("name: analytics", "name: research")]],
    );
    assert!(result.success, "{}\n{out}", result.message);
    assert!(
        prompts
            .iter()
            .any(|p| p
                .contains("Rename team 'analytics' to 'research'? 2 pipes and 0 jobs name it.")),
        "{prompts:?}"
    );
    let access = PipeAccess::from_parameters(&home.parameters(&home.db, "sql:p2", "weather"));
    assert_eq!(access.team.as_deref(), Some("research"));
    // Flags set the members and managers without the editor.
    home.ok(
        &with_i(&["edit", "team", "research", "--managers", "carol,alice"]),
        &[],
    );
    let json = home.ok(&with_i(&["show", "teams", "research", "--nopretty"]), &[]);
    let team: serde_json::Value = serde_json::from_str(json.trim()).unwrap();
    assert_eq!(team["managers"], serde_json::json!(["alice", "carol"]));
    assert_eq!(
        team["members"],
        serde_json::json!(["alice", "bob", "carol"])
    );

    // `edit users`: the role (Viewer), then the primary team; the rest unchanged.
    let (result, out, _) = home.run(
        &with_i(&["edit", "users", "bob"]),
        &["n", "n", "y", "1", "y", "1", "n"],
        Vec::new(),
    );
    assert!(result.success, "{}\n{out}", result.message);
    let out = home.ok(&with_i(&["show", "users"]), &[]);
    let bob = out.lines().find(|l| l.contains(" bob ")).unwrap();
    assert!(
        bob.contains("Viewer") && !bob.contains("(default)"),
        "{out}"
    );
    // Custom permissions, then back to the instance's default.
    home.ok(
        &with_i(&["edit", "users", "bob"]),
        &["n", "n", "y", "4", " actions:execute", "n", "n"],
    );
    let out = home.ok(&with_i(&["show", "users"]), &[]);
    let bob = out.lines().find(|l| l.contains(" bob ")).unwrap();
    assert!(bob.contains("Custom"), "{out}");
    home.ok(
        &with_i(&["edit", "users", "bob"]),
        &["n", "n", "y", "5", "n", "n"],
    );
    let out = home.ok(&with_i(&["show", "users"]), &[]);
    let bob = out.lines().find(|l| l.contains(" bob ")).unwrap();
    assert!(bob.contains("Editor (default)"), "{out}");

    // Deleting a team moves its pipes to Everyone (the confirmation counts them).
    let (result, out, prompts) = home.run(
        &with_i(&["delete", "teams", "research"]),
        &["y"],
        Vec::new(),
    );
    assert!(result.success, "{}\n{out}", result.message);
    assert!(
        prompts
            .iter()
            .any(|p| p.contains("research (2 pipes and 0 jobs)")),
        "{prompts:?}"
    );
    let access = PipeAccess::from_parameters(&home.parameters(&home.db, "sql:p2", "weather"));
    assert_eq!(access.team, None);
    assert_eq!(access.owner.as_deref(), Some("bob"));
    let (result, _, _) = home.run(
        &with_i(&["delete", "teams", "Everyone", "-y"]),
        &[],
        Vec::new(),
    );
    assert!(!result.success);

    // Deleting a user clears their ownership (the confirmation says so).
    let (result, out, prompts) = home.run(&with_i(&["delete", "users", "bob"]), &["y"], Vec::new());
    assert!(result.success, "{}\n{out}", result.message);
    assert!(
        prompts
            .iter()
            .any(|p| p.contains("bob (owns 1 pipe; they stay with their teams)")),
        "{prompts:?}"
    );
    let access = PipeAccess::from_parameters(&home.parameters(&home.db, "sql:p2", "weather"));
    assert_eq!(access.owner, None);
}

/// `mrsm` itself, with only flags and piped answers.
mod binary {
    use super::*;
    use std::io::Write;
    use std::process::{Command, Stdio};

    fn mrsm(home: &Home, args: &[&str], stdin: &str) -> (i32, String) {
        let mut command = Command::new(env!("CARGO_BIN_EXE_mrsm-cli"));
        command
            .env_clear()
            .env("PATH", std::env::var_os("PATH").unwrap_or_default())
            .env("HOME", &home.root)
            .env("MRSM_PYTHON", home.root.join("no-python"))
            .env("NO_COLOR", "1")
            .args(args)
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped());
        for (key, value) in home.env() {
            command.env(key, value);
        }
        let mut child = command.spawn().unwrap();
        child
            .stdin
            .take()
            .unwrap()
            .write_all(stdin.as_bytes())
            .unwrap();
        let output = child.wait_with_output().unwrap();
        let text = format!(
            "{}{}",
            String::from_utf8_lossy(&output.stdout),
            String::from_utf8_lossy(&output.stderr)
        );
        (output.status.code().unwrap_or(-1), text)
    }

    fn ok(home: &Home, args: &[&str], stdin: &str) -> String {
        let (code, text) = mrsm(home, args, stdin);
        assert_eq!(code, 0, "{args:?}:\n{text}");
        text
    }

    #[test]
    fn the_binary_runs_the_access_commands() {
        let home = Home::new();
        ok(
            &home,
            &["register", "users", "dana", "erin", "-i", "sql:p2"],
            "n\npassword123\npassword123\ndana@example.com\npassword123\npassword123\n\n",
        );
        let out = ok(&home, &["show", "users", "-i", "sql:p2"], "");
        let dana = out.lines().find(|l| l.contains(" dana ")).unwrap();
        assert!(
            dana.contains("regular") && dana.contains("Editor (default)"),
            "{out}"
        );
        assert!(dana.contains("dana@example.com"), "{out}");

        ok(
            &home,
            &[
                "register",
                "team",
                "field",
                "--members",
                "erin",
                "-i",
                "sql:p2",
            ],
            "",
        );
        let out = ok(&home, &["show", "teams", "-i", "sql:p2"], "");
        assert!(out.contains("field") && out.contains("erin"), "{out}");

        ok(
            &home,
            &[
                "register", "pipes", "-c", "sql:p2", "-m", "counts", "-i", "sql:p2",
            ],
            "",
        );
        ok(
            &home,
            &[
                "edit",
                "pipes",
                "access",
                "-m",
                "counts",
                "--team",
                "field",
                "--owner",
                "erin",
                "--visibility",
                "everyone",
                "--edit",
                "team",
                "-i",
                "sql:p2",
            ],
            "",
        );
        let access = PipeAccess::from_parameters(&home.parameters(&home.db, "sql:p2", "counts"));
        assert_eq!(access.team.as_deref(), Some("field"));
        assert_eq!(access.owner.as_deref(), Some("erin"));
        assert_eq!(access.visibility, Visibility::Everyone);
        // A bad choice is an argument error.
        let (code, text) = mrsm(
            &home,
            &[
                "edit",
                "pipes",
                "access",
                "-m",
                "counts",
                "--visibility",
                "nobody",
                "-i",
                "sql:p2",
            ],
            "",
        );
        assert_ne!(code, 0, "{text}");
        assert!(text.contains("invalid choice"), "{text}");

        let out = ok(
            &home,
            &["lock", "pipes", "-m", "counts", "-i", "sql:p2"],
            "",
        );
        assert!(
            out.contains("Locked Pipe('sql:p2', 'counts', instance='sql:p2') (all)."),
            "{out}"
        );
        let (code, text) = mrsm(
            &home,
            &[
                "delete", "pipes", "-m", "counts", "-y", "-f", "-i", "sql:p2",
            ],
            "",
        );
        assert_ne!(code, 0, "{text}");
        assert!(text.contains("it is locked (all)"), "{text}");
        let out = ok(
            &home,
            &["unlock", "pipes", "-m", "counts", "-i", "sql:p2"],
            "",
        );
        assert!(
            out.contains("Unlocked Pipe('sql:p2', 'counts', instance='sql:p2')."),
            "{out}"
        );
        assert!(
            !home
                .parameters(&home.db, "sql:p2", "counts")
                .contains_key("locks")
        );

        // Help lists the actions and the access flags.
        let help = ok(&home, &["-h"], "");
        assert!(help.contains("Access options"), "{help}");
        for flag in ["--owner USER", "--locks LOCKS", "--members USERS"] {
            assert!(help.contains(flag), "{help}");
        }
        assert!(
            help.lines().any(|l| l.trim_start().starts_with("lock ")),
            "{help}"
        );
    }

    #[test]
    fn show_connectors_never_prints_a_secret() {
        let home = Home::new();
        for args in [
            vec!["show", "connectors"],
            vec!["show", "connectors", "sql:secretive"],
            vec!["show", "connectors", "webhook:alerts"],
            vec!["show", "connectors", "--nopretty"],
            vec!["show", "connectors", "sql:secretive", "--nopretty"],
        ] {
            let out = ok(&home, &args, "");
            for secret in ["hunter2pw", "k3y-abc-123", "XyZsecretpath", "T0/B1"] {
                assert!(!out.contains(secret), "{args:?} printed {secret}:\n{out}");
            }
            assert!(out.contains("******"), "{args:?}:\n{out}");
        }
        let out = ok(&home, &["show", "connectors", "sql:secretive"], "");
        assert!(
            out.contains("postgresql://mrsm:******@db.example.com:5432/meerschaum"),
            "{out}"
        );
        let out = ok(
            &home,
            &["show", "connectors", "webhook:alerts", "--nopretty"],
            "",
        );
        let attributes: serde_json::Value =
            serde_json::from_str(out.trim().lines().last().unwrap()).unwrap();
        // Not connection settings: masked whole, whatever their names.
        for key in ["url", "api_key", "channel"] {
            assert_eq!(attributes[key], "******", "{key}: {out}");
        }
    }
}

/// The same commands over an `api:` instance, where the server decides (AC6): teams, a pipe's
/// access and its locks go through the server's routes (which check who may), `show users`
/// shows what the server shows the caller (other users' emails only to administrators and the
/// managers of a team they are in, AC13), and `delete teams` counts the server's pipes and jobs.
mod api {
    use super::*;
    use std::io::Write;
    use std::net::SocketAddr;
    use std::process::{Command, Stdio};
    use std::sync::Arc;

    use meerschaum_connector::InstanceConnector;
    use meerschaum_connector_sql::SqlConnector;
    use meerschaum_core::users::User;
    use meerschaum_server::{AppState, ServerConfig};

    const SECRET: &[u8] = b"0123456789abcdef0123456789abcdef0123456789abcdef";
    const PASSWORD: &str = "password123";

    /// The registered parameters of `Pipe('sql:p2', 'weather')`, read from the server's SQLite
    /// file (off the test's runtime: `Home::parameters` runs its own).
    fn weather(home: &Home) -> Map {
        tokio::task::block_in_place(|| home.parameters(&home.db, "sql:p2", "weather"))
    }

    /// `mrsm <args> -i api:p2api` as `user`, through a server on `port`.
    fn mrsm(home: &Home, port: u16, user: &str, args: &[&str], stdin: &str) -> (i32, String) {
        let mut command = Command::new(env!("CARGO_BIN_EXE_mrsm-cli"));
        command
            .env_clear()
            .env("PATH", std::env::var_os("PATH").unwrap_or_default())
            .env("HOME", &home.root)
            .env("MRSM_PYTHON", home.root.join("no-python"))
            .env("NO_COLOR", "1")
            .args(args)
            .args(["-i", "api:p2api"])
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped());
        for (key, value) in home.env() {
            command.env(key, value);
        }
        command.env(
            "MRSM_API_P2API",
            serde_json::json!({
                "host": "127.0.0.1",
                "port": port,
                "protocol": "http",
                "username": user,
                "password": PASSWORD,
            })
            .to_string(),
        );
        tokio::task::block_in_place(|| {
            let mut child = command.spawn().unwrap();
            child
                .stdin
                .take()
                .unwrap()
                .write_all(stdin.as_bytes())
                .unwrap();
            let output = child.wait_with_output().unwrap();
            let text = format!(
                "{}{}",
                String::from_utf8_lossy(&output.stdout),
                String::from_utf8_lossy(&output.stderr)
            );
            (output.status.code().unwrap_or(-1), text)
        })
    }

    fn ok(home: &Home, port: u16, user: &str, args: &[&str]) -> String {
        let (code, text) = mrsm(home, port, user, args, "");
        assert_eq!(code, 0, "{user}: {args:?}:\n{text}");
        text
    }

    /// The JSON objects of a `--nopretty` listing.
    fn objects(text: &str) -> Vec<serde_json::Value> {
        text.lines()
            .filter(|l| l.trim_start().starts_with('{'))
            .map(|l| serde_json::from_str(l).unwrap())
            .collect()
    }

    /// Each user's email as the caller sees it.
    fn emails(text: &str) -> Vec<(String, serde_json::Value)> {
        objects(text)
            .into_iter()
            .map(|u| {
                (
                    u["username"].as_str().unwrap().to_string(),
                    u["email"].clone(),
                )
            })
            .collect()
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn the_server_decides_over_an_api_instance() {
        let home = Home::new();
        // The server's instance (the same SQLite file as `sql:p2`) with an administrator, a
        // team manager and a member.
        let conn = Arc::new(
            SqlConnector::from_uri("p2", &format!("sqlite:///{}", home.db.display()))
                .await
                .unwrap(),
        );
        for (name, user_type) in [("root", Some("admin")), ("carol", None), ("bob", None)] {
            let mut user = User::new(name, Some(PASSWORD));
            user.user_type = user_type.map(str::to_string);
            user.email = Some(format!("{name}@example.com"));
            let res = conn.register_user(&mut user).await;
            assert!(res.success, "{}", res.message);
        }
        let mut cfg = ServerConfig::for_root(&conn.keys(), &home.root);
        cfg.no_dash = true;
        cfg.no_webterm = true;
        let instance: Arc<dyn InstanceConnector> = conn.clone();
        let state = AppState::builder(cfg, instance)
            .secret(SECRET.to_vec())
            .build()
            .unwrap();
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let port = listener.local_addr().unwrap().port();
        let app = meerschaum_server::app(state);
        let server = tokio::spawn(async move {
            let _ = axum::serve(
                listener,
                app.into_make_service_with_connect_info::<SocketAddr>(),
            )
            .await;
        });

        // An administrator registers a team and a pipe and sets the pipe's access and locks.
        ok(
            &home,
            port,
            "root",
            &[
                "register",
                "teams",
                "analytics",
                "--members",
                "bob",
                "--managers",
                "carol",
            ],
        );
        let teams = objects(&ok(&home, port, "root", &["show", "teams", "--nopretty"]));
        let analytics = teams
            .iter()
            .find(|t| t["name"] == "analytics")
            .unwrap_or_else(|| panic!("no team 'analytics': {teams:?}"));
        assert_eq!(analytics["managers"], serde_json::json!(["carol"]));
        assert_eq!(analytics["members"], serde_json::json!(["bob", "carol"]));

        ok(
            &home,
            port,
            "root",
            &["register", "pipes", "-c", "sql:p2", "-m", "weather"],
        );
        ok(
            &home,
            port,
            "root",
            &[
                "edit",
                "pipes",
                "access",
                "-c",
                "sql:p2",
                "-m",
                "weather",
                "--team",
                "analytics",
                "--visibility",
                "private",
                "--noask",
            ],
        );
        let access = PipeAccess::from_parameters(&weather(&home));
        assert_eq!(access.team.as_deref(), Some("analytics"));
        assert_eq!(access.visibility, Visibility::Private);
        ok(
            &home,
            port,
            "root",
            &[
                "lock", "pipes", "-c", "sql:p2", "-m", "weather", "--locks", "drop",
            ],
        );
        assert_eq!(Locks::from_parameters(&weather(&home)).names(), ["drop"]);
        let (code, text) = mrsm(
            &home,
            port,
            "root",
            &["drop", "pipes", "-c", "sql:p2", "-m", "weather", "-y", "-f"],
            "",
        );
        assert_ne!(code, 0, "{text}");
        assert!(text.contains("it is locked (drop)"), "{text}");

        // Emails: an administrator sees every address; a member only their own; a manager
        // also those of their team's members.
        let email = |name: &str| serde_json::json!(format!("{name}@example.com"));
        let null = serde_json::Value::Null;
        let seen = emails(&ok(&home, port, "root", &["show", "users", "--nopretty"]));
        assert_eq!(
            seen,
            vec![
                ("bob".to_string(), email("bob")),
                ("carol".to_string(), email("carol")),
                ("root".to_string(), email("root")),
            ],
            "root"
        );
        let seen = emails(&ok(&home, port, "bob", &["show", "users", "--nopretty"]));
        assert_eq!(
            seen,
            vec![
                ("bob".to_string(), email("bob")),
                ("carol".to_string(), null.clone()),
                ("root".to_string(), null.clone()),
            ],
            "bob"
        );
        let seen = emails(&ok(&home, port, "carol", &["show", "users", "--nopretty"]));
        assert_eq!(
            seen,
            vec![
                ("bob".to_string(), email("bob")),
                ("carol".to_string(), email("carol")),
                ("root".to_string(), null.clone()),
            ],
            "carol"
        );
        let table = ok(&home, port, "carol", &["show", "users"]);
        assert!(
            table
                .lines()
                .any(|l| l.contains(" carol ") && l.contains("analytics (manager)")),
            "{table}"
        );

        // A member may not unlock a private pipe of the team; its manager may.
        let (code, text) = mrsm(
            &home,
            port,
            "bob",
            &["unlock", "pipes", "-c", "sql:p2", "-m", "weather"],
            "",
        );
        assert_ne!(code, 0, "{text}");
        assert_eq!(Locks::from_parameters(&weather(&home)).names(), ["drop"]);
        ok(
            &home,
            port,
            "carol",
            &["unlock", "pipes", "-c", "sql:p2", "-m", "weather"],
        );
        assert!(Locks::from_parameters(&weather(&home)).is_empty());

        // Deleting the team: the question counts the server's pipes and jobs.
        let (code, text) = mrsm(
            &home,
            port,
            "root",
            &["delete", "teams", "analytics"],
            "y\n",
        );
        assert_eq!(code, 0, "{text}");
        assert!(text.contains("analytics (1 pipe and 0 jobs)"), "{text}");
        let access = PipeAccess::from_parameters(&weather(&home));
        assert_eq!(access.team, None);

        server.abort();
    }
}
