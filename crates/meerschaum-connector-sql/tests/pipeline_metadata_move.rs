//! Atomic saved-Pipeline moves retain one live registration, never a stale identity.
mod common;

use common::*;
use meerschaum_connector::InstanceConnector;
use meerschaum_core::{Map, Pipe, Value};
use serde_json::json;

fn metadata(c: &Ctx, name: &str, identity: &str, revision: i64) -> Pipe {
    Pipe::builder("mrsm", "pipeline")
        .location(Some(name))
        .instance(&c.conn.keys())
        .parameters(params(
            &json!({
                "pipeline": {"name": name, "id": identity, "revision": revision},
                "access": {"owner": "alice", "visibility": "private"},
                "description": "Révision 'quoted' \\ text",
            })
            .to_string(),
        ))
        .build()
        .unwrap()
}

fn renamed(expected: &Map, name: &str) -> Map {
    let mut parameters = expected["parameters"].to_map();
    let mut definition = parameters["pipeline"].to_map();
    definition.insert("name".into(), Value::Str(name.into()));
    definition.insert("revision".into(), Value::Int(2));
    parameters.insert("pipeline".into(), Value::Map(definition));
    parameters
}

flavor_tests!(
    pipeline_move_is_one_live_conditional_update,
    |c: Ctx| async move {
        let name = |suffix: &str| format!("{}_{}", c.prefix, suffix);
        let source = metadata(&c, &name("source"), "original-identity", 1);
        assert!(c.conn.register_pipe(&source).await.success);
        let original = c.conn.get_pipe_attributes(&source).await.unwrap();
        // A deleted source cannot be resurrected, even on SQLite when its numeric ID is reused.
        assert!(c.conn.delete_pipe(&source).await.success);
        assert!(
            !InstanceConnector::move_pipeline_registration(
                &c.conn,
                &source,
                &name("missing"),
                &original,
                &renamed(&original, &name("missing")),
            )
            .await
            .unwrap()
        );
        assert!(
            c.conn
                .register_pipe(&metadata(&c, &name("source"), "replacement-identity", 1))
                .await
                .success
        );
        assert!(
            !InstanceConnector::move_pipeline_registration(
                &c.conn,
                &source,
                &name("stale"),
                &original,
                &renamed(&original, &name("stale")),
            )
            .await
            .unwrap()
        );
        let previous = c.conn.get_pipe_attributes(&source).await.unwrap();
        let mut concurrent_edit = source.clone();
        let mut updated = previous["parameters"].to_map();
        updated.insert(
            "description".into(),
            Value::Str("concurrent metadata edit".into()),
        );
        concurrent_edit.set_parameters(updated);
        assert!(c.conn.edit_pipe(&concurrent_edit, false).await.success);
        assert!(
            !InstanceConnector::move_pipeline_registration(
                &c.conn,
                &source,
                &name("changed"),
                &previous,
                &renamed(&previous, &name("changed")),
            )
            .await
            .unwrap()
        );
        let fresh = c.conn.get_pipe_attributes(&source).await.unwrap();
        let target = metadata(&c, &name("occupied"), "destination-identity", 1);
        assert!(c.conn.register_pipe(&target).await.success);
        assert!(
            !InstanceConnector::move_pipeline_registration(
                &c.conn,
                &source,
                &name("occupied"),
                &fresh,
                &renamed(&fresh, &name("occupied")),
            )
            .await
            .unwrap()
        );
        let mut forged = renamed(&fresh, &name("forged"));
        forged.insert(
            "access".into(),
            Value::Map(params(r#"{"owner":"bob","visibility":"everyone"}"#)),
        );
        assert!(
            !InstanceConnector::move_pipeline_registration(
                &c.conn,
                &source,
                &name("forged"),
                &fresh,
                &forged,
            )
            .await
            .unwrap()
        );
        assert_eq!(c.conn.get_pipe_attributes(&source).await.unwrap(), fresh);
        let mut forged_id = renamed(&fresh, &name("forged_id"));
        let mut definition = forged_id["pipeline"].to_map();
        definition.insert("id".into(), Value::Str("reintroduced-identity".into()));
        forged_id.insert("pipeline".into(), Value::Map(definition));
        assert!(
            !InstanceConnector::move_pipeline_registration(
                &c.conn,
                &source,
                &name("forged_id"),
                &fresh,
                &forged_id,
            )
            .await
            .unwrap()
        );
        // Two independent accepted requests for the same source can move it only once.
        let a = name("winner_a");
        let b = name("winner_b");
        let (pa, pb) = (renamed(&fresh, &a), renamed(&fresh, &b));
        let (first, second) = tokio::join!(
            InstanceConnector::move_pipeline_registration(&c.conn, &source, &a, &fresh, &pa),
            InstanceConnector::move_pipeline_registration(&c.conn, &source, &b, &fresh, &pb),
        );
        assert_eq!(
            usize::from(first.unwrap()) + usize::from(second.unwrap()),
            1
        );
        let (mut winner, mut loser) = (metadata(&c, &a, "", 0), metadata(&c, &b, "", 0));
        if c.conn.get_pipe_id(&winner).await.unwrap().is_none() {
            std::mem::swap(&mut winner, &mut loser);
        }
        let moved = c.conn.get_pipe_attributes(&winner).await.unwrap();
        assert_eq!(moved["pipe_id"], fresh["pipe_id"]);
        assert_eq!(
            moved["parameters"],
            Value::Map(renamed(&fresh, winner.location_key().unwrap()))
        );
        assert!(c.conn.get_pipe_id(&source).await.unwrap().is_none());
        assert!(c.conn.get_pipe_id(&loser).await.unwrap().is_none());
        assert!(
            c.conn
                .get_pipe_id(&metadata(&c, &name("missing"), "", 0))
                .await
                .unwrap()
                .is_none()
        );
        assert!(
            c.conn
                .get_pipe_id(&metadata(&c, &name("stale"), "", 0))
                .await
                .unwrap()
                .is_none()
        );
        assert!(c.conn.delete_pipe(&winner).await.success);
        assert!(c.conn.delete_pipe(&target).await.success);

        // Different live sources competing for one destination keep one complete winner.
        let left = metadata(&c, &name("left"), "left-identity", 1);
        let right = metadata(&c, &name("right"), "right-identity", 1);
        assert!(c.conn.register_pipe(&left).await.success);
        assert!(c.conn.register_pipe(&right).await.success);
        let le = c.conn.get_pipe_attributes(&left).await.unwrap();
        let re = c.conn.get_pipe_attributes(&right).await.unwrap();
        let shared = name("shared");
        let (lp, rp) = (renamed(&le, &shared), renamed(&re, &shared));
        let (l, r) = tokio::join!(
            InstanceConnector::move_pipeline_registration(&c.conn, &left, &shared, &le, &lp),
            InstanceConnector::move_pipeline_registration(&c.conn, &right, &shared, &re, &rp),
        );
        let (l, r) = (l.unwrap(), r.unwrap());
        assert_eq!(usize::from(l) + usize::from(r), 1);
        let destination = metadata(&c, &shared, "", 0);
        let moved = c.conn.get_pipe_attributes(&destination).await.unwrap();
        let expected_winner = if l { &le } else { &re };
        assert_eq!(moved["pipe_id"], expected_winner["pipe_id"]);
        assert_eq!(moved["parameters"], Value::Map(if l { lp } else { rp }));
        assert!(
            c.conn
                .get_pipe_id(if l { &left } else { &right })
                .await
                .unwrap()
                .is_none()
        );
        assert!(
            c.conn
                .get_pipe_id(if l { &right } else { &left })
                .await
                .unwrap()
                .is_some()
        );
        assert!(c.conn.delete_pipe(&destination).await.success);
        assert!(
            c.conn
                .delete_pipe(if l { &right } else { &left })
                .await
                .success
        );
    }
);
