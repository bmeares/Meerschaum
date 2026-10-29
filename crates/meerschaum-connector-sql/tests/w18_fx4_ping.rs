//! Wave 18 (FX4): a pooled connection used within the last second is handed out without sqlx's
//! liveness ping; one idle for longer is still pinged, and a connection the server closed is
//! discarded and replaced instead of failing the query.
//!
//! Runs against `MRSM_TEST_PG_URI` / `MRSM_TEST_MYSQL_URI` (it only terminates its own backend).

use std::time::Duration;

use meerschaum_connector_sql::SqlConnector;
use meerschaum_connector_sql::driver::SqlValue;

fn int(value: Option<SqlValue>) -> i64 {
    match value.expect("a value") {
        SqlValue::Int(i) => i,
        SqlValue::UInt(u) => u as i64,
        other => panic!("not an integer: {other:?}"),
    }
}

/// Connect, find the backend, close it from another connection, and query again.
async fn recovers_after_the_server_closes_the_connection(
    uri: &str,
    pid_sql: &str,
    kill_sql: impl Fn(i64) -> String,
) {
    let conn = SqlConnector::from_uri("w18fx4a", uri).await.unwrap();
    let admin = SqlConnector::from_uri("w18fx4b", uri).await.unwrap();
    // The pool may hold more than one connection (the first-connection gate keeps its own):
    // collect every backend that answers, and close them all.
    let mut pids = std::collections::BTreeSet::new();
    for _ in 0..6 {
        pids.insert(int(conn.value(pid_sql).await));
    }
    for pid in &pids {
        assert!(admin.exec(&kill_sql(*pid)).await.is_some());
    }
    // Idle for longer than the ping threshold: the dead connections are detected by the ping,
    // dropped, and a new one answers.
    tokio::time::sleep(Duration::from_millis(1300)).await;
    let after = int(conn.value(pid_sql).await);
    assert!(
        !pids.contains(&after),
        "a new backend answered: {after} in {pids:?}"
    );
    // And the pool keeps working.
    for _ in 0..3 {
        assert!(conn.value(pid_sql).await.is_some());
    }
}

#[tokio::test]
async fn postgresql_recovers_after_the_backend_is_terminated() {
    let Ok(uri) = std::env::var("MRSM_TEST_PG_URI") else {
        eprintln!("Skipping: set MRSM_TEST_PG_URI.");
        return;
    };
    recovers_after_the_server_closes_the_connection(&uri, "SELECT pg_backend_pid()", |pid| {
        format!("SELECT pg_terminate_backend({pid})")
    })
    .await;
}

#[tokio::test]
async fn mysql_recovers_after_the_connection_is_killed() {
    let Ok(uri) = std::env::var("MRSM_TEST_MYSQL_URI") else {
        eprintln!("Skipping: set MRSM_TEST_MYSQL_URI.");
        return;
    };
    recovers_after_the_server_closes_the_connection(&uri, "SELECT CONNECTION_ID()", |id| {
        format!("KILL {id}")
    })
    .await;
}

/// The ping is skipped for a connection returned within the last moments, so one the server
/// closed in that window is handed out dead: a read must retry once on a fresh connection
/// instead of failing (FX6, review RV1 M7).
async fn a_read_survives_a_connection_closed_inside_the_no_ping_window(
    uri: &str,
    pid_sql: &str,
    kill_sql: impl Fn(i64) -> String,
) {
    let conn = SqlConnector::from_uri("w18fx6a", uri).await.unwrap();
    let admin = SqlConnector::from_uri("w18fx6b", uri).await.unwrap();
    let mut pids = std::collections::BTreeSet::new();
    for _ in 0..6 {
        pids.insert(int(conn.value(pid_sql).await));
    }
    for pid in &pids {
        assert!(admin.exec(&kill_sql(*pid)).await.is_some());
    }
    // No sleep: the idle connections were used a moment ago, so they are not pinged.
    let after = int(conn.value(pid_sql).await);
    assert!(
        !pids.contains(&after),
        "a new backend answered: {after} in {pids:?}"
    );
    // The same for the other read paths, each after the server closes the connections again.
    for round in 0..3 {
        let mut pids = std::collections::BTreeSet::new();
        for _ in 0..4 {
            pids.insert(int(conn.value(pid_sql).await));
        }
        for pid in &pids {
            assert!(admin.exec(&kill_sql(*pid)).await.is_some());
        }
        let sql = format!("{pid_sql} AS pid, {round} AS round");
        match round {
            0 => {
                let frame = conn.read(&sql).await.unwrap();
                assert_eq!(frame.height(), 1);
            }
            1 => {
                let frame = conn.read_params(&sql, &[]).await.unwrap();
                assert_eq!(frame.height(), 1);
            }
            _ => {
                assert!(conn.value_params(pid_sql, &[]).await.unwrap().is_some());
            }
        }
    }
}

#[tokio::test]
async fn postgresql_reads_survive_a_connection_closed_inside_the_no_ping_window() {
    let Ok(uri) = std::env::var("MRSM_TEST_PG_URI") else {
        eprintln!("Skipping: set MRSM_TEST_PG_URI.");
        return;
    };
    a_read_survives_a_connection_closed_inside_the_no_ping_window(
        &uri,
        "SELECT pg_backend_pid()",
        |pid| format!("SELECT pg_terminate_backend({pid})"),
    )
    .await;
}

#[tokio::test]
async fn mysql_reads_survive_a_connection_closed_inside_the_no_ping_window() {
    let Ok(uri) = std::env::var("MRSM_TEST_MYSQL_URI") else {
        eprintln!("Skipping: set MRSM_TEST_MYSQL_URI.");
        return;
    };
    a_read_survives_a_connection_closed_inside_the_no_ping_window(
        &uri,
        "SELECT CONNECTION_ID()",
        |id| format!("KILL {id}"),
    )
    .await;
}
