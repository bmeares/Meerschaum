//! `register_first_user`: of any number of concurrent first registrations, from independent
//! connectors (separate pools, as separate processes have), at most one succeeds; a failed one
//! leaves the lock free; a lock row that cannot be seeded is an error, never a lock.
//!
//! The races run on SQLite files (always available); the server flavors run the "already has a
//! user" case when their URIs are set.

mod common;
use common::*;
use meerschaum_connector::InstanceConnector;
use meerschaum_connector_sql::SqlConnector;
use meerschaum_core::users::User;

fn unique(tag: &str) -> String {
    format!(
        "rs_{tag}_{}",
        &uuid::Uuid::new_v4().simple().to_string()[..10]
    )
}

fn admin(name: &str) -> User {
    let mut user = User::new(name, Some("test1234"));
    user.user_type = Some("admin".into());
    user
}

async fn sqlite_pair(tag: &str) -> (std::path::PathBuf, SqlConnector, SqlConnector) {
    let path = temp_sqlite_path(tag);
    let uri = format!("sqlite:///{}", path.display());
    let a = SqlConnector::from_uri("rs_first_a", &uri).await.unwrap();
    let b = SqlConnector::from_uri("rs_first_b", &uri).await.unwrap();
    (path, a, b)
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn sqlite_first_user_is_registered_once_across_connectors() {
    let (path, a, b) = sqlite_pair("first_race").await;
    let names: Vec<String> = (0..8).map(|i| unique(&format!("first{i}"))).collect();
    let outcomes = futures_util::future::join_all(names.iter().enumerate().map(|(i, name)| {
        let conn = if i % 2 == 0 { &a } else { &b };
        async move {
            let mut user = admin(name);
            InstanceConnector::register_first_user(conn, &mut user).await
        }
    }))
    .await;
    let won = outcomes.iter().filter(|o| matches!(o, Ok(true))).count();
    let lost = outcomes.iter().filter(|o| matches!(o, Ok(false))).count();
    assert_eq!(won, 1, "{outcomes:?}");
    assert_eq!(won + lost, 8, "no registration may fail open: {outcomes:?}");
    let users = InstanceConnector::get_users(&a).await.unwrap();
    assert_eq!(users.len(), 1, "{users:?}");
    let user_type = a.get_user_type(&User::new(&users[0], None)).await.unwrap();
    assert_eq!(user_type.as_deref(), Some("admin"));
    drop((a, b));
    let _ = std::fs::remove_file(&path);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn sqlite_a_failed_first_registration_leaves_the_lock_free() {
    let (path, a, b) = sqlite_pair("first_fail").await;
    // The instance tables, then a trigger that refuses one username's insert.
    assert!(a.ensure_instance_tables(true).await);
    let users = a.instance_table_name("mrsm_users");
    assert!(
        a.exec(&format!(
            "CREATE TRIGGER refuse_boom BEFORE INSERT ON {users} \
             WHEN NEW.username = 'boom' BEGIN SELECT RAISE(ABORT, 'refused'); END"
        ))
        .await
        .is_some()
    );
    let failed = InstanceConnector::register_first_user(&a, &mut admin("boom")).await;
    assert!(failed.is_err(), "{failed:?}");
    assert!(InstanceConnector::get_users(&a).await.unwrap().is_empty());
    // Rolled back: the other connector becomes the first user.
    let name = unique("after");
    let won = InstanceConnector::register_first_user(&b, &mut admin(&name)).await;
    assert!(matches!(won, Ok(true)), "{won:?}");
    assert_eq!(InstanceConnector::get_users(&a).await.unwrap(), vec![name]);
    drop((a, b));
    let _ = std::fs::remove_file(&path);
}

#[tokio::test]
async fn sqlite_a_lock_row_that_cannot_be_seeded_is_an_error() {
    let (path, a, _b) = sqlite_pair("first_nolock").await;
    assert!(a.ensure_instance_tables(true).await);
    let lock = a.instance_table_name("mrsm_bootstrap");
    assert!(
        a.exec(&format!(
            "CREATE TABLE {lock} (claim INTEGER NOT NULL PRIMARY KEY CHECK (claim <> 1))"
        ))
        .await
        .is_some()
    );
    let refused = InstanceConnector::register_first_user(&a, &mut admin(&unique("nolock"))).await;
    assert!(refused.is_err(), "{refused:?}");
    assert!(InstanceConnector::get_users(&a).await.unwrap().is_empty());
    drop(a);
    let _ = std::fs::remove_file(&path);
}

flavor_tests!(
    test_first_user_is_not_registered_when_a_user_exists,
    |c: Ctx| async move {
        let mut existing = User::new(&unique("existing"), Some("test1234"));
        ok(c.conn.register_user(&mut existing).await);
        let name = unique("late");
        let outcome = InstanceConnector::register_first_user(&c.conn, &mut admin(&name)).await;
        assert!(matches!(outcome, Ok(false)), "{outcome:?}");
        assert!(
            c.conn
                .get_user_id(&User::new(&name, None))
                .await
                .unwrap()
                .is_none()
        );
        ok(c.conn.delete_user(&existing).await);
    }
);
