//! rc15 on Valkey (in-memory backend): protected connectors refuse writes (Q20) and the
//! internal `mrsm` pipes are left out of `fetch_pipes_keys` unless named (B2). One test: the
//! protected allowance is process-wide.

use std::sync::Arc;

use meerschaum_connector::traits::{Connector, DataQuery, InstanceConnector, PipesKeysQuery};
use meerschaum_connector_valkey::{MemoryBackend, ValkeyConnector};
use meerschaum_core::{Map, Pipe, Value};
use meerschaum_kernel::docs::frame_from_docs;
use meerschaum_kernel::value::Value as KValue;

fn pipe(conn: &ValkeyConnector, ck: &str, mk: &str, lk: Option<&str>) -> Pipe {
    Pipe::builder(ck, mk)
        .location(lk)
        .instance(&conn.connector_keys())
        .build()
        .unwrap()
}

#[tokio::test]
async fn protected_and_hidden_internal_pipes() {
    assert!(std::env::var("MRSM_ALLOW_PROTECTED").is_err());
    let backend = Arc::new(MemoryBackend::new());
    let plain = ValkeyConnector::with_backend("f2", Map::new(), backend.clone());
    let mut attrs = Map::new();
    attrs.insert("protected".into(), Value::Bool(true));
    let prot = ValkeyConnector::with_backend("f2", attrs, backend.clone());
    assert!(!plain.is_protected());
    assert!(Connector::is_protected(&prot));
    // `?protected=true` on a URI (an `MRSM_VALKEY_<LABEL>` variable) marks it too; the client
    // library never sees the query string.
    let mut by_uri = Map::new();
    by_uri.insert(
        "uri".into(),
        Value::Str("redis://localhost:6399/13?protected=true".into()),
    );
    let by_uri = ValkeyConnector::with_backend("f2", by_uri, backend.clone());
    assert!(Connector::is_protected(&by_uri));
    assert_eq!(by_uri.connection_url(), "redis://localhost:6399/13");
    let mut unmarked = Map::new();
    unmarked.insert(
        "uri".into(),
        Value::Str("redis://localhost:6399/13?protected=false".into()),
    );
    assert!(!ValkeyConnector::with_backend("f2", unmarked, backend.clone()).is_protected());
    let refused = by_uri
        .clear_pipe(
            &pipe(&by_uri, "plugin:noaa", "weather", None),
            &DataQuery::default(),
        )
        .await;
    assert!(
        refused.message.starts_with("Refusing to clear "),
        "{}",
        refused.message
    );

    let user = pipe(&plain, "plugin:noaa", "weather", None);
    let internal = pipe(&plain, "mrsm", "pipeline", Some("daily"));
    assert!(plain.register_pipe(&user).await.success);
    assert!(plain.register_pipe(&internal).await.success);

    // B2: hidden unless named.
    let all = plain
        .fetch_pipes_keys(&PipesKeysQuery::default())
        .await
        .unwrap();
    assert!(all.iter().any(|e| e.connector_keys == "plugin:noaa"));
    assert!(!all.iter().any(|e| e.connector_keys == "mrsm"));
    let named = plain
        .fetch_pipes_keys(&PipesKeysQuery {
            connector_keys: vec!["mrsm".into()],
            ..Default::default()
        })
        .await
        .unwrap();
    assert_eq!(named.len(), 1);

    // Q20: every write through the protected connector is refused, reads are not.
    let docs: Vec<indexmap::IndexMap<String, KValue>> =
        vec![[("v".to_string(), KValue::Int(1))].into_iter().collect()];
    let df = frame_from_docs(&docs).unwrap();
    let user_on_prot = pipe(&prot, "plugin:noaa", "weather", None);
    for (op, res) in [
        (
            "sync",
            prot.sync_pipe(&user_on_prot, df.clone(), &Default::default())
                .await,
        ),
        (
            "clear",
            prot.clear_pipe(&user_on_prot, &DataQuery::default()).await,
        ),
        ("drop", prot.drop_pipe(&user_on_prot).await),
        ("delete", prot.delete_pipe(&user_on_prot).await),
        ("edit", prot.edit_pipe(&user_on_prot).await),
        (
            "register",
            prot.register_pipe(&pipe(&prot, "plugin:noaa", "other", None))
                .await,
        ),
    ] {
        assert!(!res.success, "{op}");
        assert!(
            res.message.starts_with(&format!("Refusing to {op} ")),
            "{}",
            res.message
        );
        assert!(
            res.message.contains("protected instance 'valkey:f2'"),
            "{}",
            res.message
        );
    }
    assert!(
        InstanceConnector::fetch_pipes_keys(&prot, &PipesKeysQuery::default())
            .await
            .is_ok()
    );
    // Internal pipes are bookkeeping: allowed.
    let internal_on_prot = pipe(&prot, "mrsm", "pipeline", Some("weekly"));
    assert!(prot.register_pipe(&internal_on_prot).await.success);
    {
        let _allowed = meerschaum_core::protected::allow(true);
        let res = prot.sync_pipe(&user_on_prot, df, &Default::default()).await;
        assert!(res.success, "{}", res.message);
    }
}
