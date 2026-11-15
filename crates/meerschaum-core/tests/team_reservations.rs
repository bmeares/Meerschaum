//! A connector reserved for a team (`teams: [<name>]`) is never inherited by name (5.1): deleting
//! the team retires the reservation, renaming it moves the reservation, and both are written to
//! the configuration file as it is written (its references kept).

use meerschaum_core::access::{
    Principal, TeamRole, can_use_connector_keys, move_reservations, reserved_for,
    retired_reservation,
};
use meerschaum_core::config::{Config, ConfigOptions};
use meerschaum_core::value::{Map, Value};

fn connectors() -> Map {
    Value::from_json_str(
        r#"{"sql": {"main": {"flavor": "timescaledb"},
                    "wetlands": {"flavor": "timescaledb", "database": "wetlands",
                                 "teams": ["Wetlands"]},
                    "shared": {"flavor": "postgresql", "teams": "Wetlands, Forest"}}}"#,
    )
    .unwrap()
    .to_map()
}

fn member_of(team: &str) -> Principal {
    Principal::new(
        "ann",
        false,
        Default::default(),
        vec![
            ("Everyone".into(), TeamRole::Member),
            (team.into(), TeamRole::Member),
        ],
    )
}

fn teams_of(connectors: &Map, label: &str) -> Option<Vec<String>> {
    match connectors["sql"].get(label)?.get("teams")? {
        Value::List(items) => Some(items.iter().map(Value::py_str).collect()),
        other => Some(vec![other.py_str()]),
    }
}

#[test]
fn a_deleted_teams_connector_is_not_inherited_by_name() {
    let mut connectors = connectors();
    // The hole: a later team with the deleted team's name (any case) is let in by name.
    let newcomer = member_of("wetlands");
    let teams = teams_of(&connectors, "wetlands");
    assert!(can_use_connector_keys(
        &newcomer,
        "sql:wetlands",
        teams.as_deref()
    ));
    assert_eq!(
        reserved_for(&connectors, "WETLANDS"),
        ["sql:wetlands", "sql:shared"]
    );

    // Deleting the team retires its reservations; the connectors themselves stay.
    let tombstone = retired_reservation("Wetlands", Some("3f2a"));
    assert!(
        !meerschaum_core::access::valid_team_name(&tombstone).success,
        "no team can be named like a retired reservation"
    );
    let changed = move_reservations(&mut connectors, "Wetlands", &tombstone);
    assert_eq!(changed, ["sql:wetlands", "sql:shared"]);
    assert!(reserved_for(&connectors, "Wetlands").is_empty());
    let teams = teams_of(&connectors, "wetlands");
    assert!(!can_use_connector_keys(
        &newcomer,
        "sql:wetlands",
        teams.as_deref()
    ));
    assert_eq!(
        connectors["sql"]
            .get("wetlands")
            .unwrap()
            .get("database")
            .unwrap()
            .py_str(),
        "wetlands"
    );
    // Other teams keep theirs.
    assert_eq!(
        teams_of(&connectors, "shared").unwrap(),
        [tombstone.as_str(), "Forest"]
    );
    assert!(can_use_connector_keys(
        &member_of("Forest"),
        "sql:shared",
        teams_of(&connectors, "shared").as_deref()
    ));
}

#[test]
fn renaming_moves_the_reservation_to_the_new_name() {
    let mut connectors = connectors();
    move_reservations(&mut connectors, "Wetlands", "Marsh");
    let teams = teams_of(&connectors, "wetlands");
    assert!(can_use_connector_keys(
        &member_of("Marsh"),
        "sql:wetlands",
        teams.as_deref()
    ));
    assert!(!can_use_connector_keys(
        &member_of("Wetlands"),
        "sql:wetlands",
        teams.as_deref()
    ));
    assert!(move_reservations(&mut connectors, "Nobody", "X").is_empty());
}

#[test]
fn reservations_are_rewritten_in_the_configuration_file() {
    let root = tempfile::tempdir().unwrap();
    std::fs::create_dir_all(root.path().join("config")).unwrap();
    std::fs::write(
        root.path().join("config").join("meerschaum.yaml"),
        "connectors:\n  sql:\n    wetlands:\n      flavor: timescaledb\n      host: db\n      \
         username: mrsm_wetlands\n      password: MRSM{meerschaum:connectors:sql:main:password}\n      \
         teams:\n      - Wetlands\n",
    )
    .unwrap();
    let env = vec![(
        "MRSM_ROOT_DIR".into(),
        root.path().to_string_lossy().into_owned(),
    )];
    let mut opts = ConfigOptions::with_env(env);
    opts.allow_writes = true;
    let config = Config::new(opts).unwrap();
    let tombstone = retired_reservation("Wetlands", Some("1234"));
    let changed = config
        .move_team_reservations("wetlands", &tombstone)
        .unwrap();
    assert_eq!(changed, ["sql:wetlands"]);
    let text = std::fs::read_to_string(root.path().join("config").join("meerschaum.yaml")).unwrap();
    assert!(text.contains(&tombstone), "{text}");
    assert!(!text.contains("- Wetlands"), "{text}");
    // The reference is kept as written.
    assert!(
        text.contains("MRSM{meerschaum:connectors:sql:main:password}"),
        "{text}"
    );
    // In memory too.
    let live = config.get(&["meerschaum", "connectors"]).unwrap().to_map();
    assert!(reserved_for(&live, "Wetlands").is_empty());
    // Nothing to move: nothing written.
    assert!(
        config
            .move_team_reservations("Wetlands", "X")
            .unwrap()
            .is_empty()
    );
}
