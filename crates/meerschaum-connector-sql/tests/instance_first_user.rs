//! `register_first_user`: of any number of concurrent first registrations, from independent
//! connectors (separate pools, as separate processes have), at most one succeeds; a failed one
//! leaves the lock free; a lock row that cannot be seeded is an error, never a lock.
//!
//! The races run on SQLite files (always available) and, when their URIs are set, on a database
//! of their own on PostgreSQL (defaulting to REPEATABLE READ, where locking the claim row without
//! changing it promoted a second administrator), MySQL and MariaDB; every flavor runs the
//! "already has a user" case.

mod common;
use common::*;
use meerschaum_connector::InstanceConnector;
use meerschaum_connector_sql::SqlConnector;
use meerschaum_connector_sql::uri::ConnectionSpec;
use meerschaum_core::users::User;
use std::time::{Duration, Instant};

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

/// Password hashing leaves the async worker available to advance other registrations.
#[tokio::test(flavor = "current_thread")]
async fn hashing_a_first_users_password_never_stalls_the_runtime() {
    let (path, a, _b) = sqlite_pair("first_hash").await;
    let started = Instant::now();
    meerschaum_core::users::hash_password("calibration", None, None).unwrap();
    let one_hash = started.elapsed();
    let (longest, first) = longest_stall(InstanceConnector::register_first_user(
        &a,
        &mut admin(&unique("hash")),
    ))
    .await;
    assert!(matches!(first, Ok(true)), "{first:?}");
    assert!(
        longest < one_hash / 2,
        "a {one_hash:?} password hash stalled the runtime for {longest:?}"
    );
    drop(a);
    let _ = std::fs::remove_file(&path);
}

/// Measure ticker gaps from before `work` starts until after it finishes.
async fn longest_stall<F: std::future::Future>(work: F) -> (Duration, F::Output) {
    let ticks = std::sync::Arc::new(std::sync::Mutex::new(Vec::<Instant>::new()));
    let (ready_tx, ready_rx) = tokio::sync::oneshot::channel();
    let ticker = tokio::spawn({
        let ticks = ticks.clone();
        async move {
            let mut ready = Some(ready_tx);
            let mut interval = tokio::time::interval(Duration::from_millis(10));
            loop {
                interval.tick().await;
                ticks.lock().unwrap().push(Instant::now());
                if let Some(ready) = ready.take() {
                    let _ = ready.send(());
                }
            }
        }
    });
    ready_rx.await.unwrap();
    let output = work.await;
    let seen = ticks.lock().unwrap().len();
    let recorded = || ticks.lock().unwrap().len();
    tokio::time::timeout(Duration::from_secs(10), async {
        while recorded() <= seen {
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
    })
    .await
    .expect("the ticker stopped");
    ticker.abort();
    let ticks = ticks.lock().unwrap().clone();
    let longest = ticks.windows(2).map(|w| w[1] - w[0]).max().unwrap();
    (longest, output)
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

/// A database of its own on a server flavor, so a race starts with no users without touching
/// other tests' users. PostgreSQL's defaults to REPEATABLE READ.
struct Dedicated {
    flavor: &'static str,
    admin: SqlConnector,
    spec: ConnectionSpec,
    database: String,
}

impl Dedicated {
    async fn create(flavor: &'static str) -> Option<Self> {
        let env = FLAVOR_ENVS
            .iter()
            .find(|(f, _)| *f == flavor)
            .map(|(_, e)| *e)
            .expect("flavor");
        let Ok(uri) = std::env::var(env) else {
            eprintln!("Skipping {flavor}: set {env}.");
            return None;
        };
        let mut spec = ConnectionSpec::from_uri(&uri, Some(&format!("rs_first_{flavor}")), None)
            .expect("valid URI");
        let admin = SqlConnector::connect(&format!("rs_first_admin_{flavor}"), spec.clone())
            .await
            .expect("connects");
        let database = unique("first_db");
        admin
            .driver()
            .exec(&format!("CREATE DATABASE {database}"))
            .await
            .unwrap_or_else(|e| {
                panic!("The race needs a database of its own (CREATE DATABASE {database}): {e}")
            });
        if flavor == "postgresql" {
            admin
                .driver()
                .exec(&format!(
                    "ALTER DATABASE {database} SET default_transaction_isolation TO 'repeatable read'"
                ))
                .await
                .expect("isolation");
        }
        spec.database = Some(database.clone());
        Some(Self {
            flavor,
            admin,
            spec,
            database,
        })
    }

    async fn connect(&self, label: &str) -> SqlConnector {
        SqlConnector::connect(label, self.spec.clone())
            .await
            .expect("connects")
    }

    /// Wait until `n` sessions of this database wait on a lock in a statement starting with
    /// `statement` (`'INSERT INTO%mrsm_users'`, say).
    async fn wait_for_lock_waits(&self, statement: &str, n: i64) {
        let query = if self.flavor == "postgresql" {
            format!(
                "SELECT COUNT(*) AS n FROM pg_stat_activity WHERE datname = '{}' \
                 AND wait_event_type = 'Lock' AND query ILIKE '{statement}%'",
                self.database
            )
        } else {
            format!(
                "SELECT COUNT(*) AS n FROM information_schema.innodb_trx t \
                 JOIN information_schema.processlist p ON p.id = t.trx_mysql_thread_id \
                 WHERE p.db = '{}' AND t.trx_state = 'LOCK WAIT' AND t.trx_query LIKE '{statement}%'",
                self.database
            )
        };
        // MySQL and MariaDB refresh the `INFORMATION_SCHEMA.INNODB_TRX` cache only when more than
        // 0.1 s passed since its *last read*, so polling faster keeps reading a snapshot taken
        // before the wait began: https://dev.mysql.com/doc/refman/5.7/en/innodb-information-schema-internal-data.html
        let poll = if self.flavor == "postgresql" {
            Duration::from_millis(50)
        } else {
            Duration::from_millis(200)
        };
        let deadline = Instant::now() + Duration::from_secs(30);
        loop {
            let waiting = self
                .admin
                .driver()
                .value(&query)
                .await
                .expect("lock waits")
                .and_then(|v| v.as_i64())
                .unwrap_or(0);
            if waiting >= n {
                return;
            }
            assert!(
                Instant::now() < deadline,
                "no session waits in {statement} ({waiting} of {n})"
            );
            tokio::time::sleep(poll).await;
        }
    }

    async fn drop_database(self, connectors: Vec<SqlConnector>) {
        for conn in &connectors {
            conn.driver().close().await;
        }
        let drop = if self.flavor == "postgresql" {
            format!("DROP DATABASE {} WITH (FORCE)", self.database)
        } else {
            format!("DROP DATABASE {}", self.database)
        };
        self.admin
            .driver()
            .exec(&drop)
            .await
            .expect("drop database");
        self.admin.driver().close().await;
    }
}

/// The deterministic race: the first registration holds the claim (its insert waits on a
/// third session's table lock), the second demonstrably waits on the claim row, then the first
/// commits. The second is an ordinary registration, never a second administrator.
async fn a_waiting_first_registration_is_not_promoted(flavor: &'static str) {
    let Some(d) = Dedicated::create(flavor).await else {
        return;
    };
    let winner = d.connect("rs_first_w").await;
    let loser = d.connect("rs_first_l").await;
    let blocker = d.connect("rs_first_c").await;
    // The instance tables and the username key first: their DDL would wait on the blocker.
    let mut warm = User::new(&unique("warm"), Some("test1234"));
    ok(blocker.register_user(&mut warm).await);
    ok(blocker.delete_user(&warm).await);
    assert!(
        InstanceConnector::get_users(&blocker)
            .await
            .unwrap()
            .is_empty()
    );

    let users = blocker.instance_table_name("mrsm_users");
    let mut tx = blocker.driver().begin().await.unwrap();
    let hold = if flavor == "postgresql" {
        format!("LOCK TABLE {users} IN SHARE MODE")
    } else {
        // REPEATABLE READ: locks the empty table's supremum gap, so inserts wait.
        format!("SELECT user_id FROM {users} FOR UPDATE")
    };
    tx.exec(&hold).await.unwrap();

    let (first, second) = (unique("first"), unique("second"));
    let winning = tokio::spawn({
        let name = first.clone();
        async move {
            let outcome = InstanceConnector::register_first_user(&winner, &mut admin(&name)).await;
            (winner, outcome)
        }
    });
    d.wait_for_lock_waits("INSERT INTO%mrsm_users", 1).await;
    let losing = tokio::spawn({
        let name = second.clone();
        async move {
            let outcome = InstanceConnector::register_first_user(&loser, &mut admin(&name)).await;
            (loser, outcome)
        }
    });
    d.wait_for_lock_waits("UPDATE%mrsm_bootstrap", 1).await;
    tx.rollback().await.unwrap();

    let (winner, won) = winning.await.unwrap();
    let (loser, lost) = losing.await.unwrap();
    assert!(matches!(won, Ok(true)), "{won:?}");
    assert!(matches!(lost, Ok(false)), "{lost:?}");
    assert_eq!(
        InstanceConnector::get_users(&blocker).await.unwrap(),
        vec![first.clone()]
    );
    let user_type = blocker
        .get_user_type(&User::new(&first, None))
        .await
        .unwrap();
    assert_eq!(user_type.as_deref(), Some("admin"));
    d.drop_database(vec![winner, loser, blocker]).await;
}

/// Eight first registrations at once from two independent connectors: one administrator, and
/// every other one is refused cleanly (none fails, none is promoted).
async fn first_registrations_at_once(flavor: &'static str) {
    let Some(d) = Dedicated::create(flavor).await else {
        return;
    };
    let a = d.connect("rs_first_a").await;
    let b = d.connect("rs_first_b").await;
    let names: Vec<String> = (0..8).map(|i| unique(&format!("first{i}"))).collect();
    let outcomes = futures_util::future::join_all(names.iter().enumerate().map(|(i, name)| {
        let conn = if i % 2 == 0 { &a } else { &b };
        async move { InstanceConnector::register_first_user(conn, &mut admin(name)).await }
    }))
    .await;
    let won = outcomes.iter().filter(|o| matches!(o, Ok(true))).count();
    let lost = outcomes.iter().filter(|o| matches!(o, Ok(false))).count();
    assert_eq!(won, 1, "{outcomes:?}");
    assert_eq!(won + lost, 8, "no registration may fail: {outcomes:?}");
    let users = InstanceConnector::get_users(&a).await.unwrap();
    assert_eq!(users.len(), 1, "{users:?}");
    d.drop_database(vec![a, b]).await;
}

/// A registration never succeeds with a reservation whose commit was refused.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn sqlite_a_user_is_registered_only_with_a_stored_id_reservation() {
    use sqlx::ConnectOptions as _;
    let (path, a, b) = sqlite_pair("reserve_held").await;
    let first = InstanceConnector::register_first_user(&a, &mut admin(&unique("held0"))).await;
    assert!(matches!(first, Ok(true)), "{first:?}");
    let (users, ids) = (
        a.instance_table_name("mrsm_users"),
        a.instance_table_name("mrsm_user_ids"),
    );
    // Another connection holds a read transaction (a shared lock in the rollback journal) for
    // longer than the busy timeout, then lets go.
    let mut reader = sqlx::sqlite::SqliteConnectOptions::new()
        .filename(&path)
        .journal_mode(sqlx::sqlite::SqliteJournalMode::Delete)
        .connect()
        .await
        .unwrap();
    sqlx::query("BEGIN").execute(&mut reader).await.unwrap();
    sqlx::query(sqlx::AssertSqlSafe(format!("SELECT COUNT(*) FROM {users}")))
        .fetch_one(&mut reader)
        .await
        .unwrap();
    let name = unique("held1");
    let mut user = User::new(&name, Some("test1234"));
    user.password_hash =
        Some(meerschaum_core::users::hash_password("test1234", None, Some(1_000)).unwrap());
    let (registered, ()) = tokio::join!(a.register_user(&mut user), async {
        tokio::time::sleep(Duration::from_secs(7)).await;
        sqlx::query("ROLLBACK").execute(&mut reader).await.unwrap();
    });
    let id: Option<i64> = sqlx::query_scalar(sqlx::AssertSqlSafe(format!(
        "SELECT user_id FROM {users} WHERE username = ?"
    )))
    .bind(name.as_str())
    .fetch_optional(&mut reader)
    .await
    .unwrap();
    match id {
        Some(id) => {
            assert!(registered.success, "{registered:?}");
            let stored: i64 = sqlx::query_scalar(sqlx::AssertSqlSafe(format!(
                "SELECT COUNT(*) FROM {ids} WHERE user_id = ?"
            )))
            .bind(id)
            .fetch_one(&mut reader)
            .await
            .unwrap();
            assert_eq!(
                stored, 1,
                "user {id} was registered with a rolled-back reservation"
            );
        }
        None => assert!(!registered.success, "{registered:?}"),
    }
    drop((reader, a, b));
    let _ = std::fs::remove_file(&path);
}

macro_rules! server_races {
    ($($flavor:literal => $waiting:ident, $at_once:ident;)*) => {$(
        #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
        async fn $waiting() {
            a_waiting_first_registration_is_not_promoted($flavor).await;
        }

        #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
        async fn $at_once() {
            first_registrations_at_once($flavor).await;
        }
    )*};
}

server_races! {
    "postgresql" => postgresql_waiting_registration, postgresql_registrations_at_once;
    "mysql" => mysql_waiting_registration, mysql_registrations_at_once;
    "mariadb" => mariadb_waiting_registration, mariadb_registrations_at_once;
}
