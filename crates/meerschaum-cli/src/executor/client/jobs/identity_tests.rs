//! Canonical job identity across teams, names and delegated launches, and the controller fixture
//! the other suites share.

use super::*;
use meerschaum_connector::InstanceConnector;
use meerschaum_core::Map;
use meerschaum_core::access::{Principal, Role, Team, TeamRole};
use meerschaum_core::config::{Config, ConfigOptions};
use meerschaum_core::users::User;
use meerschaum_server::access::Access;
use meerschaum_server::auth;
use meerschaum_server::backends::JobsBackend;

pub(super) async fn fixture(
    root: &std::path::Path,
) -> (Arc<Controller>, Arc<dyn InstanceConnector>) {
    let mut options = ConfigOptions::with_env(vec![
        ("MRSM_ROOT_DIR".into(), root.display().to_string()),
        (
            "MRSM_PLUGINS_DIR".into(),
            root.join("plugins").display().to_string(),
        ),
    ]);
    options.home = root.into();
    options.allow_writes = false;
    let snapshot = Value::from_json_str(&serde_json::json!({"meerschaum":{"instance":"sql:main", "connectors":{"sql":{"main":{"flavor":"sqlite", "database":root.join("identity.db")}}}}}).to_string()).unwrap().to_map();
    let config = Config::new(options.clone()).unwrap();
    config.set_config(snapshot.clone());
    let session = crate::native::Session::with_runtime(tokio::runtime::Handle::current());
    session.set_python_worker(false);
    let instance =
        tokio::task::spawn_blocking(move || session.instance("sql:main", &config).unwrap().conn())
            .await
            .unwrap();
    (
        Arc::new(Controller::new(
            root.into(),
            instance.clone(),
            options,
            snapshot,
        )),
        instance,
    )
}

fn caller(user: &User, id: &Value, team: &Team, role: TeamRole) -> Access {
    let mut user = user.clone();
    user.user_id = Some(id.clone());
    let mut access = Access::everything();
    access.no_auth = false;
    access.principal = Principal::new(
        &user.username,
        false,
        Role::Operator.permissions(),
        vec![(team.name.clone(), role)],
    );
    access.primary_team = team.name.clone();
    access.team_records = vec![team.clone()];
    access.auth = Some(auth::Principal::User(user));
    access
}

fn record(name: &str, team: &Team, owner: &User, id: &Value) -> Record {
    let mut properties = Map::new();
    meerschaum_core::job_access::JobAccess::stamp(
        &mut properties,
        Some(&owner.username),
        Some(&team.name),
    );
    let mut record = Record::new(
        name,
        Uuid::parse_str(team.id.as_deref().unwrap()).unwrap(),
        vec!["show".into(), "version".into()],
        properties,
    );
    record.owner_binding = Some(callbacks::OwnerBinding::User {
        username: owner.username.clone(),
        user_id: id.clone(),
    });
    record
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn names_are_projected_by_team_id_and_deleted_names_never_adopt_history() {
    let root = tempfile::tempdir().unwrap();
    let (controller, instance) = fixture(root.path()).await;
    let mut alice = User::new("alice", Some("synthetic-pass"));
    let mut bob = User::new("bob", Some("synthetic-pass"));
    let mut charlie = User::new("charlie", Some("synthetic-pass"));
    for user in [&mut alice, &mut bob, &mut charlie] {
        assert!(instance.register_user(user).await.success);
    }
    let alice_id = instance.get_user_id(&alice).await.unwrap().unwrap();
    let bob_id = instance.get_user_id(&bob).await.unwrap().unwrap();
    let charlie_id = instance.get_user_id(&charlie).await.unwrap().unwrap();
    let mut harbor = Team::new("Harbor");
    harbor.members.insert("alice".into(), TeamRole::Member);
    harbor.members.insert("bob".into(), TeamRole::Member);
    assert!(instance.register_team(&mut harbor).await.success);
    let public = record("nightly", &harbor, &alice, &alice_id);
    let private = record("execute-private", &harbor, &alice, &alice_id);
    assert!(instance.rename_team("Harbor", "Quay").await.success);
    let quay = instance.get_team("Quay").await.unwrap().unwrap();
    assert_eq!(quay.id, harbor.id);
    let bob_access = Arc::new(caller(&bob, &bob_id, &quay, TeamRole::Member));
    let visible = context::scope(
        Some(bob_access),
        controller.job_views(vec![public.clone(), private.clone()]),
    )
    .await
    .unwrap();
    assert_eq!(visible.len(), 1);
    assert_eq!(
        visible[0].properties.get("team").and_then(Value::as_str),
        Some("Quay")
    );
    let manager = Arc::new(caller(&bob, &bob_id, &quay, TeamRole::Manager));
    assert_eq!(
        context::scope(Some(manager), controller.job_views(vec![private.clone()]))
            .await
            .unwrap()
            .len(),
        1
    );
    let mut replacement = Team::new("Harbor");
    replacement
        .members
        .insert("charlie".into(), TeamRole::Member);
    assert!(instance.register_team(&mut replacement).await.success);
    let new_member = Arc::new(caller(
        &charlie,
        &charlie_id,
        &replacement,
        TeamRole::Member,
    ));
    assert!(
        context::scope(
            Some(new_member),
            controller.job_views(vec![public.clone(), private.clone()])
        )
        .await
        .unwrap()
        .is_empty()
    );
    assert!(instance.delete_team("Quay").await.success);
    let mut new_quay = Team::new("Quay");
    new_quay.members.insert("charlie".into(), TeamRole::Manager);
    assert!(instance.register_team(&mut new_quay).await.success);
    assert_ne!(new_quay.id, quay.id);
    let new_manager = Arc::new(caller(&charlie, &charlie_id, &new_quay, TeamRole::Manager));
    assert!(
        context::scope(
            Some(new_manager),
            controller.job_views(vec![public.clone(), private.clone()])
        )
        .await
        .unwrap()
        .is_empty()
    );
    let owner = caller(&alice, &alice_id, &replacement, TeamRole::Member);
    assert_eq!(
        context::scope(
            Some(Arc::new(owner.clone())),
            controller.job_views(vec![private.clone()])
        )
        .await
        .unwrap()
        .len(),
        1
    );
    let mut bounded = owner;
    bounded.principal = bounded.principal.within_team(&replacement.name);
    assert!(
        context::scope(Some(Arc::new(bounded)), controller.job_views(vec![private]))
            .await
            .unwrap()
            .is_empty(),
        "A team-bound credential used historical ownership to escape its UUID boundary."
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn hidden_remote_placement_never_falls_back_to_a_local_job_of_the_same_name() {
    let root = tempfile::tempdir().unwrap();
    let (controller, instance) = fixture(root.path()).await;
    let mut user = User::new("viewer", Some("synthetic-pass"));
    assert!(instance.register_user(&mut user).await.success);
    let id = instance.get_user_id(&user).await.unwrap().unwrap();
    let mut current = Team::new("Reused");
    current
        .members
        .insert(user.username.clone(), TeamRole::Member);
    assert!(instance.register_team(&mut current).await.success);
    let mut retired = current.clone();
    retired.id = Some(Uuid::new_v4().to_string());
    let name = format!("tpr-shadow-{}", Uuid::new_v4().simple());
    let mut hidden = record(
        &name,
        &retired,
        &User::new("another-owner", None),
        &999.into(),
    );
    hidden.owner_binding = None;
    Catalog::new(root.path()).reserve(hidden).unwrap();
    let marker = root.path().join("must-not-start");
    let mut local = meerschaum_jobs::JobsContext::for_root(root.path())
        .with_launch_environment(vec![("PATH".into(), "/bin:/usr/bin".into())]);
    local.config.executor = Some("local".into());
    local.launcher.supervisor = vec![
        "/bin/sh".into(),
        "-c".into(),
        format!("touch '{}'", marker.display()),
    ];
    let mut dormant = meerschaum_jobs::Job::new(
        &name,
        Some(vec!["show".into(), "version".into()]),
        meerschaum_jobs::JobOptions {
            executor_keys: Some("local".into()),
            ..Default::default()
        },
        &local,
    )
    .unwrap();
    assert!(dormant.daemon().write_properties().success);
    let backend = crate::executor::jobs::DelegatingJobs::new(
        root.path(),
        Arc::new(crate::jobs_backend::LocalJobsBackend::new(local)),
        controller,
    );
    let actor = Arc::new(caller(&user, &id, &current, TeamRole::Member));
    context::scope(Some(actor), async {
        assert!(backend.try_get(&name).await.unwrap().is_none());
        assert!(!backend.exists(&name).await);
        assert!(!backend.start(&name).await.success);
        assert!(backend.logs(&name).await.is_none());
        assert!(backend.monitor_logs(&name).await.is_none());
        assert!(
            !backend
                .try_list_definitions()
                .await
                .unwrap()
                .contains_key(&name)
        );
    })
    .await;
    assert!(
        !marker.exists(),
        "A hidden delegated name started its local bystander."
    );
}
