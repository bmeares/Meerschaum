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

#[test]
fn a_team_connector_is_added_to_the_file_as_written() {
    let root = tempfile::tempdir().unwrap();
    let file = root.path().join("config").join("meerschaum.yaml");
    std::fs::create_dir_all(file.parent().unwrap()).unwrap();
    let written = "connectors:\n  sql:\n    shared:\n      flavor: sqlite\n      \
                   database: MRSM{meerschaum:connectors:sql:local:database}\n";
    std::fs::write(&file, written).unwrap();
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        std::fs::set_permissions(&file, std::fs::Permissions::from_mode(0o644)).unwrap();
    }
    let env = vec![
        (
            "MRSM_ROOT_DIR".into(),
            root.path().to_string_lossy().into_owned(),
        ),
        (
            "MRSM_SQL_MAIN".into(),
            "postgresql://mrsm:env-only-pw-4242@db.example.com:5432/meerschaum".into(),
        ),
        (
            "MRSM_CONFIG".into(),
            r#"{"meerschaum": {"connectors": {"api": {"up": {"password": "patch-only-31337"}}}}}"#
                .into(),
        ),
    ];
    let mut opts = ConfigOptions::with_env(env);
    opts.allow_writes = true;
    let config = Config::new(opts).unwrap();
    // The environment connector is in memory.
    assert!(
        config
            .get(&["meerschaum", "connectors", "sql", "main", "uri"])
            .unwrap()
            .py_str()
            .contains("env-only-pw-4242")
    );
    let attributes = Value::from_json_str(
        r#"{"flavor": "postgresql", "host": "db.example.com", "username": "mrsm_wetlands",
            "password": "generated-pw", "database": "wetlands", "teams": ["Wetlands"]}"#,
    )
    .unwrap()
    .to_map();
    config
        .add_connector("sql", "wetlands", &attributes)
        .unwrap();

    let text = std::fs::read_to_string(&file).unwrap();
    assert!(!text.contains("env-only-pw-4242"), "{text}");
    assert!(!text.contains("patch-only-31337"), "{text}");
    assert!(!text.contains("main:"), "{text}");
    assert!(
        text.contains("MRSM{meerschaum:connectors:sql:local:database}"),
        "{text}"
    );
    assert!(text.contains("generated-pw"), "{text}");
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        let mode = std::fs::metadata(&file).unwrap().permissions().mode() & 0o777;
        assert_eq!(mode, 0o600, "{mode:o}");
    }
    // In memory too, beside the environment connector.
    let live = config.get(&["meerschaum", "connectors", "sql"]).unwrap();
    assert_eq!(
        live.get("wetlands")
            .unwrap()
            .get("username")
            .unwrap()
            .py_str(),
        "mrsm_wetlands"
    );
    assert!(live.get("main").unwrap().get("uri").is_some());
    // A connector already in the file is refused, and the file is not touched.
    assert!(config.add_connector("sql", "shared", &attributes).is_err());
    assert_eq!(std::fs::read_to_string(&file).unwrap(), text);
}
