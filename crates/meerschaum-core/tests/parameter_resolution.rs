mod common;

use meerschaum_core::config::ConfigOptions;
use meerschaum_core::pipe::PipeDefaults;
use meerschaum_core::template::{InMemoryResolver, TemplateResolver};
use meerschaum_core::{Config, Map, Pipe, Value};

struct Resolver {
    pipes: InMemoryResolver,
    config: Config,
}
impl TemplateResolver for Resolver {
    fn load_pipe(&self, pipe: &Pipe) -> Pipe {
        self.pipes.load_pipe(pipe)
    }
    fn config(&self) -> Option<&Config> {
        Some(&self.config)
    }
    fn table_reference(&self, pipe: &Pipe) -> Result<String, String> {
        let target = pipe.target(None).map_err(|e| e.to_string())?;
        Ok(if pipe.instance_keys() == "sql:fixture_sqlite" {
            format!("\"{}\"", target.replace('"', "\"\""))
        } else {
            target
        })
    }
}

#[test]
fn parameters_and_targets_match_released_v4() {
    let fixture = common::load_fixture("parameter_resolution.json");
    let root = tempfile::tempdir().unwrap();
    let plugins = tempfile::tempdir().unwrap();
    let config = Config::new(ConfigOptions::with_env(vec![
        ("MRSM_ROOT_DIR".into(), root.path().display().to_string()),
        (
            "MRSM_PLUGINS_DIR".into(),
            plugins.path().display().to_string(),
        ),
    ]))
    .unwrap();
    config.set_config(fixture.get("config").unwrap().as_map().unwrap().clone());
    let defaults = PipeDefaults::default();
    let pipes = fixture
        .get("registered")
        .unwrap()
        .as_list()
        .unwrap()
        .iter()
        .map(|v| Pipe::from_state(v.as_map().unwrap(), &defaults).unwrap())
        .collect();
    let resolver = Resolver {
        pipes: InMemoryResolver {
            defaults: defaults.clone(),
            pipes,
        },
        config,
    };
    for case in fixture.get("cases").unwrap().as_list().unwrap() {
        let mut pipe =
            Pipe::from_state(case.get("state").unwrap().as_map().unwrap(), &defaults).unwrap();
        pipe.resolve_parameters_with(&resolver, None).unwrap();
        common::assert_same(
            &Value::Map(pipe.parameters().clone()),
            case.get("parameters").unwrap(),
            pipe.metric_key(),
        );
        assert_eq!(
            pipe.target(None).unwrap(),
            case.get("target").unwrap().as_str().unwrap(),
            "{}",
            pipe.metric_key()
        );
    }
}

#[test]
fn ordinary_consumers_resolve_and_mutations_invalidate() {
    let mut pipe = Pipe::builder("plugin:fixture", "events").parameters(
        Value::from_json_str(r#"{"target":"{{ self.metric }}","name":"ts","kind":"int","columns":{"datetime":"{{ self.parameters['name'] }}"},"dtypes":{"ts":"{{ self.parameters['kind'] }}"},"fetch":{"definition":"SELECT '{{ self.metric }}'"},"indices":{"time":["{{ self.parameters['name'] }}"]}}"#).unwrap().as_map().unwrap().clone()
    ).build().unwrap();
    assert_eq!(pipe.target(None).unwrap(), "events");
    assert_eq!(pipe.column("datetime").as_deref(), Some("ts"));
    assert_eq!(
        pipe.param("dtypes").unwrap().get("ts"),
        Some(&Value::from("int"))
    );
    assert_eq!(
        pipe.param("fetch").unwrap().get("definition"),
        Some(&Value::from("SELECT 'events'"))
    );
    assert_eq!(
        pipe.indices().get("time"),
        Some(&Value::List(vec![Value::from("ts")]))
    );
    assert_eq!(
        pipe.raw_parameters().get("target"),
        Some(&Value::from("{{ self.metric }}"))
    );
    pipe.parameters_mut()
        .insert("name".into(), Value::from("event_time"));
    assert_eq!(pipe.column("datetime").as_deref(), Some("event_time"));
    pipe.update_parameters(Map::from([(
        "target".into(),
        Value::from("{{ self.connector_keys }}"),
    )]));
    assert_eq!(pipe.target(None).unwrap(), "plugin:fixture");
    assert_eq!(
        pipe.to_state().get("parameters").unwrap().get("target"),
        pipe.raw_parameters().get("target")
    );
}

#[test]
fn engine_defaults_and_overrides() {
    let root = tempfile::tempdir().unwrap();
    let plugins = tempfile::tempdir().unwrap();
    let config = Config::new(ConfigOptions::with_env(vec![
        ("MRSM_ROOT_DIR".into(), root.path().display().to_string()),
        (
            "MRSM_PLUGINS_DIR".into(),
            plugins.path().display().to_string(),
        ),
        (
            "MRSM_CONFIG".into(),
            r#"{"system":{"engine":{"python_workers":7,"inclusive_cap":false}}}"#.into(),
        ),
    ]))
    .unwrap();
    assert_eq!(
        config.get(&["system", "engine", "python_workers"]),
        Some(Value::Int(7))
    );
    assert_eq!(
        config.get(&["system", "engine", "inclusive_cap"]),
        Some(Value::Bool(false))
    );
    assert_eq!(
        config.get(&["system", "engine", "flat_ledger"]),
        Some(Value::Bool(true))
    );
    for (cpus, workers) in [(1, 1), (2, 2), (64, 4)] {
        let defaults = meerschaum_core::config::default_config("/unused", cpus, false);
        let engine = defaults["system"].get("engine").unwrap();
        assert_eq!(engine.get("python_workers"), Some(&Value::Int(workers)));
        assert_eq!(engine.get("inclusive_cap"), Some(&Value::Bool(true)));
        assert_eq!(engine.get("flat_ledger"), Some(&Value::Bool(true)));
    }
}
