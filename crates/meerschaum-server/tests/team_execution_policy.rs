//! 5.1: `may_run_system_commands` across callers and conditions. Administrators may run system
//! commands anywhere; a member of an isolated team whose members may install plugins (a team
//! that runs code) may run them only as team execution, and only when every condition holds
//! on the server; everyone else keeps the deny-by-default policy. The host's facts (the sandbox
//! mode and the probe of a team process) come from a fake actions backend; the kernel's Landlock
//! support and the container socket are this host's.

mod common;

use std::sync::{Arc, Mutex};

use async_trait::async_trait;
use common::{MemoryInstance, SECRET};
use meerschaum_connector::InstanceConnector;
use meerschaum_core::access::{Team, TeamRole};
use meerschaum_core::team_scope::sandbox::{self, Canary, Mode, Report};
use meerschaum_core::users::User;
use meerschaum_core::{Map, SuccessTuple, Value};
use meerschaum_server::AppState;
use meerschaum_server::auth::{Principal, Scopes};
use meerschaum_server::backends::ActionsBackend;
use meerschaum_server::execute_policy::{
    Place, SERVER_PROCESS_REFUSAL, SystemCommands, check_command, may_run_system_commands,
};
use meerschaum_server::team_execution::{HostFacts, Probe};
use serde_json::json;

fn map(v: serde_json::Value) -> Map {
    match meerschaum_server::json::to_core(&v) {
        Value::Map(m) => m,
        _ => Map::new(),
    }
}

/// `show` and `sh` are actions; the host's facts are what the test sets.
#[derive(Default)]
struct Actions {
    facts: Mutex<Option<HostFacts>>,
    remote: Mutex<Option<Result<(), String>>>,
}

#[async_trait]
impl ActionsBackend for Actions {
    fn list(&self) -> Vec<String> {
        vec!["show".into(), "sh".into()]
    }
    async fn execute(&self, _: &str, _: Map) -> SuccessTuple {
        SuccessTuple::fail("not here")
    }
    async fn team_execution_probe(&self, _team: &Team) -> Option<HostFacts> {
        self.facts.lock().unwrap().clone()
    }
    async fn executor_ready(&self, _team: &Team) -> Result<(), String> {
        self.remote
            .lock()
            .unwrap()
            .clone()
            .unwrap_or_else(|| Err("remote unavailable".into()))
    }
}

fn good_report() -> Report {
    Report {
        mode: Mode::RequireSockets,
        enforced: true,
        abi: 9,
        kernel_abi: 11,
        features: sandbox::features_of(9).0,
        canary: Canary::Denied,
        ..Report::default()
    }
}

fn good() -> HostFacts {
    HostFacts {
        mode: Mode::Auto,
        probe: Ok(Probe {
            sandbox: good_report(),
            python_error: None,
        }),
    }
}

struct Fixture {
    state: AppState,
    actions: Arc<Actions>,
    _dir: tempfile::TempDir,
}

async fn team(instance: &MemoryInstance, name: &str, member: &str, attributes: serde_json::Value) {
    let mut t = Team::new(name);
    t.add_member(member, TeamRole::Member);
    t.attributes = map(attributes);
    let res = instance.register_team(&mut t).await;
    assert!(res.success, "{res}");
}

async fn fixture() -> Fixture {
    let dir = tempfile::tempdir().unwrap();
    let instance = Arc::new(MemoryInstance::new("fixture"));
    instance.seed_user("admin", "pass", Some("admin"));
    for name in ["ed", "wren", "rita"] {
        instance.seed_user(name, "pw", None);
        instance.seed_user_attributes(name, map(json!({"role": "operator"})));
    }
    team(&instance, "Data", "ed", json!({})).await;
    team(
        &instance,
        "Wet",
        "wren",
        json!({"isolated": true, "plugins_write": true}),
    )
    .await;
    team(&instance, "Dry", "rita", json!({"isolated": true})).await;
    let actions = Arc::new(Actions::default());
    let state = AppState::builder(common::config(dir.path()), instance.clone())
        .secret(SECRET.to_vec())
        .actions(actions.clone())
        .build()
        .unwrap();
    Fixture {
        state,
        actions,
        _dir: dir,
    }
}

impl Fixture {
    async fn access(&self, user: &str) -> meerschaum_server::access::Access {
        meerschaum_server::access::access_of_fresh(
            &self.state,
            Principal::User(User::new(user, None)),
            &Scopes::List(vec!["*".into()]),
        )
        .await
        .unwrap()
    }

    async fn answer(&self, user: &str, facts: Option<HostFacts>) -> SystemCommands {
        *self.actions.facts.lock().unwrap() = facts;
        self.state.team_execution_probes.clear();
        let access = self.access(user).await;
        may_run_system_commands(&self.state, &access).await
    }
}

fn reason(answer: SystemCommands) -> String {
    match answer {
        SystemCommands::No { reason: Some(r) } => r,
        other => panic!("expected a refusal with a reason, got {other:?}"),
    }
}

#[tokio::test]
async fn selected_executor_requires_remote_readiness_and_never_uses_a_host_probe() {
    let f = fixture().await;
    let mut team = f.state.instance().get_team("Wet").await.unwrap().unwrap();
    team.attributes.insert(
        "execution".into(),
        Value::Map(map(json!({"mode": "container"}))),
    );
    assert!(f.state.instance().edit_team(&team).await.success);
    // Even a successful local proof cannot authorize the selected unavailable remote.
    assert!(reason(f.answer("wren", Some(good())).await).contains("remote unavailable"));
    *f.actions.remote.lock().unwrap() = Some(Ok(()));
    assert_eq!(
        f.answer("wren", None).await,
        SystemCommands::TeamOnly { team: "Wet".into() }
    );
    *f.actions.remote.lock().unwrap() = Some(Err("generation revoked".into()));
    assert!(reason(f.answer("wren", Some(good())).await).contains("generation revoked"));
}

#[tokio::test]
async fn the_one_answer_across_callers_and_conditions() {
    let f = fixture().await;
    // Administrators: anywhere, whatever the host says.
    assert_eq!(f.answer("admin", None).await, SystemCommands::Anywhere);
    // The open side: no, and no team execution to explain.
    assert_eq!(
        f.answer("ed", Some(good())).await,
        SystemCommands::No { reason: None }
    );
    // An isolated team whose members may not install plugins: no.
    assert_eq!(
        f.answer("rita", Some(good())).await,
        SystemCommands::No { reason: None }
    );

    // A team that runs code, each condition failing in turn.
    let off = HostFacts {
        mode: Mode::Off,
        ..good()
    };
    assert!(reason(f.answer("wren", Some(off)).await).contains("sandbox is off"));
    assert!(reason(f.answer("wren", None).await).contains("cannot start the team's processes"));
    let failed = HostFacts {
        probe: Err("the process exited".into()),
        ..good()
    };
    assert!(reason(f.answer("wren", Some(failed)).await).contains("could not be proven"));
    let unenforced = HostFacts {
        probe: Ok(Probe {
            sandbox: Report::not_enforced(Mode::Require, "seccomp refused Landlock"),
            python_error: None,
        }),
        ..good()
    };
    assert!(reason(f.answer("wren", Some(unenforced)).await).contains("seccomp"));
    let readable = HostFacts {
        probe: Ok(Probe {
            sandbox: Report {
                canary: Canary::Readable,
                ..good_report()
            },
            python_error: None,
        }),
        ..good()
    };
    assert!(reason(f.answer("wren", Some(readable)).await).contains("canary"));
    let no_sockets = HostFacts {
        probe: Ok(Probe {
            sandbox: Report {
                abi: 8,
                level: sandbox::LEVEL_FILESYSTEM.to_string(),
                features: sandbox::features_of(8).0,
                ..good_report()
            },
            python_error: None,
        }),
        ..good()
    };
    assert!(reason(f.answer("wren", Some(no_sockets)).await).contains(sandbox::LEVEL_FILESYSTEM));
    let old_python = HostFacts {
        probe: Ok(Probe {
            sandbox: good_report(),
            python_error: Some("meerschaum 5.0.2 does not apply team scoping".into()),
        }),
        ..good()
    };
    assert!(reason(f.answer("wren", Some(old_python)).await).contains("5.0.2"));

    // Every condition holding: team execution, where this kernel allows it at all.
    let support = sandbox::kernel_support();
    let answer = f.answer("wren", Some(good())).await;
    if support.abi() >= 9 {
        assert_eq!(answer, SystemCommands::TeamOnly { team: "Wet".into() });
    } else {
        let why = reason(answer);
        assert!(
            why.contains("Landlock") || why.contains(sandbox::LEVEL_FILESYSTEM),
            "{why}"
        );
    }
}

#[tokio::test]
async fn team_execution_never_reaches_the_servers_process() {
    let f = fixture().await;
    *f.actions.facts.lock().unwrap() = Some(good());
    let wren = f.access("wren").await;
    // The withheld scopes are kept in this form only; `instance:chain` never.
    assert!(wren.has("actions:execute") && wren.has("webterm:use"));
    assert!(!wren.has("instance:chain"));
    let rita = f.access("rita").await;
    assert!(!rita.has("actions:execute") && !rita.has("webterm:use"));
    let sh = vec!["sh".to_string(), "-c".into(), "true".into()];
    // In the server's process (`/actions`, MCP, the restricted terminal): refused for every
    // member of an isolated team.
    for access in [&wren, &rita] {
        assert_eq!(
            check_command(&f.state, access, &sh, Place::Server).await,
            Err(SERVER_PROCESS_REFUSAL.to_string())
        );
    }
    // As a job: team execution passes; the other team keeps the deny-by-default policy.
    let job = check_command(&f.state, &wren, &sh, Place::Job).await;
    if sandbox::kernel_support().abi() >= 9 {
        assert_eq!(job, Ok(()));
    } else {
        assert!(job.unwrap_err().contains("cannot run here"));
    }
    let refused = check_command(&f.state, &rita, &sh, Place::Job)
        .await
        .unwrap_err();
    assert!(refused.contains("non-administrators"), "{refused}");
    // The open side, as before.
    let ed = f.access("ed").await;
    assert!(
        check_command(&f.state, &ed, &sh, Place::Job)
            .await
            .unwrap_err()
            .contains("non-administrators")
    );
    assert_eq!(
        check_command(&f.state, &f.access("admin").await, &sh, Place::Server).await,
        Ok(())
    );
}

/// An access holding exactly `permissions` (an administrator's narrowed key holds only its
/// scopes).
fn access(
    is_admin: bool,
    permissions: &[&str],
    no_auth: bool,
) -> meerschaum_server::access::Access {
    let mut principal = meerschaum_core::access::Principal::new(
        "someone",
        is_admin,
        Default::default(),
        Vec::new(),
    );
    principal.permissions = permissions.iter().map(|p| p.to_string()).collect();
    meerschaum_server::access::Access {
        auth: None,
        credential: None,
        principal,
        primary_team: meerschaum_server::access::EVERYONE.into(),
        attributes: None,
        no_auth,
        team_records: Vec::new(),
        api_execution: meerschaum_server::backends::ApiExecution::Unavailable,
    }
}

/// Moved from `execute_policy`'s unit tests (they need a server state now).
#[tokio::test]
async fn one_question_says_who_may_run_system_commands() {
    let f = fixture().await;
    let all: Vec<&str> = meerschaum_core::access::ALL_PERMISSIONS.to_vec();
    let anywhere = |a: SystemCommands| a == SystemCommands::Anywhere;
    // Administrators (with every permission) and everyone under `--no-auth` may.
    assert!(anywhere(
        may_run_system_commands(&f.state, &access(true, &all, false)).await
    ));
    let everything = meerschaum_server::access::Access::everything();
    assert!(anywhere(
        may_run_system_commands(&f.state, &everything).await
    ));
    assert!(anywhere(
        may_run_system_commands(&f.state, &access(false, &[], true)).await
    ));
    // Nobody else: not an operator with `webterm:use` and `actions:execute`, nor an
    // administrator's narrowed key.
    let operator = access(
        false,
        &["webterm:use", "actions:execute", "jobs:execute"],
        false,
    );
    assert_eq!(
        may_run_system_commands(&f.state, &operator).await,
        SystemCommands::No { reason: None }
    );
    let narrowed = access(true, &["pipes:read"], false);
    assert_eq!(
        may_run_system_commands(&f.state, &narrowed).await,
        SystemCommands::No { reason: None }
    );
}

/// Moved from `execute_policy`'s unit tests.
#[tokio::test]
async fn check_command_refuses_the_shell_only_to_those_who_may_not_run_it() {
    let f = fixture().await;
    let s = |args: &[&str]| args.iter().map(|a| a.to_string()).collect::<Vec<_>>();
    let all: Vec<&str> = meerschaum_core::access::ALL_PERMISSIONS.to_vec();
    let admin = access(true, &all, false);
    let user = access(false, &["actions:execute", "webterm:use"], false);
    for words in [
        s(&["touch", "/x"]),
        s(&["!touch", "/x"]),
        s(&["sh", "id"]),
        s(&["python"]),
        s(&["show", "version", "+", "touch", "/x"]),
        s(&["show", "version", "++", "touch", "/x"]),
        s(&["touch", "/x", ":", "--loop"]),
        s(&["show", "version", "--shell"]),
    ] {
        for place in [Place::Job, Place::Server] {
            assert!(
                check_command(&f.state, &admin, &words, place).await.is_ok(),
                "{words:?}"
            );
            assert!(
                check_command(&f.state, &user, &words, place).await.is_err(),
                "{words:?}"
            );
        }
    }
    assert!(
        check_command(&f.state, &user, &s(&["show", "version"]), Place::Job)
            .await
            .is_ok()
    );
}
