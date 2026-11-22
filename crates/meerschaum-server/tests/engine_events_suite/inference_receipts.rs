//! Only a trusted successful native transition may advance an accepted operation member.
use std::collections::BTreeMap;

use meerschaum_connector::mutation::MetadataInference;
use meerschaum_core::{Map, Pipe, Value};
use meerschaum_server::executor::pipe_operation::{Snapshot, SnapshotMember};
use serde_json::json;

fn map(value: serde_json::Value) -> Map {
    Value::from_json_str(&value.to_string()).unwrap().to_map()
}

#[test]
fn inference_receipts_compare_exact_registration_and_old_definition_without_peer_adoption() {
    let pipe = Pipe::builder("sql:source", "selected").instance("sql:main")
        .parameters(map(json!({"target":"selected_table","fetch":{"definition":"SELECT id FROM source"},"access":{"owner":"alice","team":"team-a","visibility":"private"},"columns":{"id":"id"},"dtypes":{"id":"int64"}})))
        .build().unwrap();
    let before = pipe.raw_parameters().clone();
    let member = SnapshotMember {
        registration: Value::Int(7),
        parameters: before.clone(),
        writable: true,
        node_window: None,
    };
    let snapshot = Snapshot::new(BTreeMap::from([(pipe.pipe_ref(), member)])).unwrap();
    let mut change = MetadataInference {
        pipe: pipe.clone(),
        registration: Value::Int(7),
        before: before.clone(),
        after: before.clone(),
    };
    change
        .after
        .get_mut("dtypes")
        .unwrap()
        .as_map_mut()
        .unwrap()
        .insert("payload".into(), Value::Str("json".into()));
    let mut accepted = snapshot.clone();
    assert!(accepted.accept_inference(&change).unwrap());
    assert_ne!(accepted, snapshot);
    assert!(
        accepted.accept_inference(&change).is_err(),
        "A stale receipt overwrote a newer accepted definition"
    );
    for (key, value) in [
        ("target", Value::Str("other_destination".into())),
        (
            "fetch",
            Value::Map(map(json!({"definition":"SELECT secret FROM other_source"}))),
        ),
        (
            "access",
            Value::Map(map(json!({"owner":"mallory","visibility":"public"}))),
        ),
    ] {
        let mut forged = change.clone();
        forged.after.insert(key.into(), value);
        let mut actual = snapshot.clone();
        assert!(
            actual.accept_inference(&forged).is_err(),
            "Inference adopted {key}"
        );
        assert_eq!(
            actual, snapshot,
            "Refused {key} changed the accepted snapshot"
        );
    }
    let mut recreated = change.clone();
    recreated.registration = Value::Int(8);
    assert!(snapshot.clone().accept_inference(&recreated).is_err());
    let mut peer = change.clone();
    peer.before.insert(
        "tags".into(),
        Value::List(vec![Value::Str("peer-edit".into())]),
    );
    peer.after.insert(
        "tags".into(),
        Value::List(vec![Value::Str("peer-edit".into())]),
    );
    assert!(
        snapshot.clone().accept_inference(&peer).is_err(),
        "Receipt adopted unrelated peer parameters"
    );
    let dependency = Snapshot::new(BTreeMap::from([(
        pipe.pipe_ref(),
        SnapshotMember {
            registration: Value::Int(7),
            parameters: before,
            writable: false,
            node_window: None,
        },
    )]))
    .unwrap();
    assert!(
        dependency.clone().accept_inference(&change).is_err(),
        "A metadata dependency gained write authority"
    );
    let mut unrelated = change.clone();
    unrelated.pipe = Pipe::builder("sql:source", "unselected")
        .instance("sql:main")
        .build()
        .unwrap();
    let mut unchanged = snapshot.clone();
    assert!(!unchanged.accept_inference(&unrelated).unwrap());
    assert_eq!(unchanged, snapshot);
}

#[test]
fn graph_transport_labels_include_selected_sources_but_no_unselected_or_dependency_sources() {
    let selected = Pipe::builder("sql:second", "selected")
        .instance("sql:main")
        .build()
        .unwrap();
    let dependency = Pipe::builder("sql:dependency_source", "metadata_only")
        .instance("sql:dependency")
        .build()
        .unwrap();
    let snapshot = Snapshot::new(BTreeMap::from([
        (
            selected.pipe_ref(),
            SnapshotMember {
                registration: Value::Int(1),
                parameters: Map::new(),
                writable: true,
                node_window: None,
            },
        ),
        (
            dependency.pipe_ref(),
            SnapshotMember {
                registration: Value::Int(2),
                parameters: Map::new(),
                writable: false,
                node_window: None,
            },
        ),
    ]))
    .unwrap();
    assert_eq!(
        snapshot.transport_instances().unwrap(),
        std::collections::BTreeSet::from([
            "sql:main".into(),
            "sql:second".into(),
            "sql:dependency".into()
        ])
    );
}
