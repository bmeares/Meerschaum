"""
`Pipe('plugin:<native plugin>', ...)` through the Python façade: a native (Rust) plugin is not a
Python module, so `PluginConnector` must not try to import it (which printed a
"Plugin '...' cannot be found. Is it installed?" traceback) and runs its `register(pipe)` hook
through the core instead.

Connector types a native plugin declares (`#[make_connector] mod readings { ... }`) are known to
`mrsm.get_connector()` too: it returns a `NativeConnector` façade whose methods run in the core,
as a source (`readings:office`) and as an instance (`#[make_connector(instance)]`).

Each case runs in a subprocess with its own root and plugins directory (the native plugins are
not part of `tests/plugins`) and a SQLite instance of its own.
"""

import json
import os
import pathlib
import shutil
import subprocess
import sys
import textwrap

import pytest

REPO = pathlib.Path(__file__).resolve().parent.parent
CARGO = shutil.which('cargo') or os.path.expanduser('~/.cargo/bin/cargo')

SINGLE_FILE = textwrap.dedent(
    """
    fn fetch() -> Vec<Value> {
        vec![json!({"id": 1, "temperature": 21.5})]
    }

    fn register() -> Value {
        json!({"columns": {"id": "id"}})
    }
    """
)

FETCH_ONLY = textwrap.dedent(
    """
    fn fetch() -> Vec<Value> {
        vec![json!({"id": 1})]
    }
    """
)

SCRIPT = textwrap.dedent(
    """
    import json, sys
    import meerschaum as mrsm

    name = sys.argv[1]
    out = {}
    pipe = mrsm.Pipe('plugin:' + name, 'm', instance='sql:local')
    out['register'] = list(pipe.register())
    out['is_native'] = getattr(pipe.connector, 'is_native', None)
    out['parameters'] = mrsm.Pipe('plugin:' + name, 'm', instance='sql:local').parameters
    out['sync'] = list(pipe.sync())
    out['rowcount'] = pipe.get_rowcount()
    df = pipe.get_data()
    out['data'] = df.to_dict(orient='records') if df is not None else None
    print('RESULT ' + json.dumps(out))
    """
)


def _run(
    tmp_path: pathlib.Path,
    name: str,
    script: str = SCRIPT,
    extra_env: dict | None = None,
) -> tuple[dict, str]:
    root = tmp_path / 'root'
    root.mkdir(exist_ok=True)
    env = {
        key: value
        for key, value in os.environ.items()
        if not key.startswith('MRSM_') and key not in ('TMUX', 'TMUX_PANE')
    }
    env.update({
        'PYTHONPATH': str(REPO),
        'MRSM_ROOT_DIR': str(root),
        'MRSM_PLUGINS_DIR': str(tmp_path / 'plugins'),
        'MRSM_NO_UPDATE_CHECK': '1',
        'MRSM_NOASK': 'true',
        'MRSM_SQL_LOCAL': json.dumps({'flavor': 'sqlite', 'database': str(tmp_path / 'local.db')}),
        'PATH': os.path.dirname(CARGO) + os.pathsep + env.get('PATH', ''),
    })
    env.update(extra_env or {})
    proc = subprocess.run(
        [sys.executable, '-c', script, name],
        env=env,
        capture_output=True,
        text=True,
        timeout=600,
    )
    output = proc.stdout + proc.stderr
    lines = [line for line in proc.stdout.splitlines() if line.startswith('RESULT ')]
    assert lines, output
    return json.loads(lines[-1][len('RESULT '):]), output


def _assert_quiet(output: str):
    assert 'cannot be found' not in output, output
    assert 'Traceback' not in output, output
    assert 'Exception' not in output, output


@pytest.mark.skipif(not os.path.exists(CARGO), reason="single-file plugins need cargo")
def test_register_single_file_native_plugin(tmp_path):
    plugins = tmp_path / 'plugins'
    plugins.mkdir()
    (plugins / 'readings.rs').write_text(SINGLE_FILE)
    result, output = _run(tmp_path, 'readings')
    _assert_quiet(output)
    assert result['register'] == [
        True, "Successfully registered Pipe('plugin:readings', 'm', instance='sql:local')."
    ], result
    assert result['is_native'] is True, result
    # The plugin's `register()` supplied the parameters.
    assert result['parameters']['columns'] == {'id': 'id'}, result
    assert result['sync'][0] is True, result
    assert result['rowcount'] == 1, result
    # Typed columns, as for a Python plugin's documents (not `'1'` / `'21.5'` from `TEXT`).
    assert result['data'] == [{'id': 1, 'temperature': 21.5}], result


@pytest.mark.skipif(not os.path.exists(CARGO), reason="single-file plugins need cargo")
def test_register_native_plugin_without_register_hook(tmp_path):
    plugins = tmp_path / 'plugins'
    plugins.mkdir()
    (plugins / 'counts.rs').write_text(FETCH_ONLY)
    result, output = _run(tmp_path, 'counts')
    _assert_quiet(output)
    assert result['register'][0] is True, result
    assert result['is_native'] is True, result
    assert result['parameters']['columns'] == {'datetime': None, 'id': None}, result
    assert result['sync'][0] is True, result


@pytest.mark.skipif(not os.path.exists(CARGO), reason="the example plugin is built with cargo")
def test_register_native_directory_plugin(tmp_path):
    subprocess.run(
        [CARGO, 'build', '-p', 'meerschaum-plugin', '--example', 'native_demo'],
        cwd=REPO,
        check=True,
        capture_output=True,
    )
    suffix = {'darwin': 'dylib', 'win32': 'dll'}.get(sys.platform, 'so')
    prefix = '' if sys.platform == 'win32' else 'lib'
    library = REPO / 'target' / 'debug' / 'examples' / f'{prefix}native_demo.{suffix}'
    plugin_dir = tmp_path / 'plugins' / 'native_demo'
    plugin_dir.mkdir(parents=True)
    (plugin_dir / 'plugin.toml').write_text('name = "native_demo"\nversion = "1.0.0"\n')
    shutil.copy(library, plugin_dir / library.name)

    result, output = _run(tmp_path, 'native_demo')
    _assert_quiet(output)
    assert result['register'][0] is True, result
    assert result['is_native'] is True, result
    assert result['parameters']['columns'] == {'datetime': 'id'}, result
    assert result['parameters']['dtypes'] == {'id': 'int'}, result


CONNECTOR_TYPE = textwrap.dedent(
    """
    #[make_connector]
    mod readings {
        fn fetch(connector: &Connector) -> Vec<Value> {
            vec![json!({"id": 1, "source": connector.label()})]
        }

        fn register() -> Value {
            json!({"columns": {"id": "id"}})
        }
    }
    """
)

CONNECTOR_TYPE_SCRIPT = textwrap.dedent(
    """
    import json, sys
    import meerschaum as mrsm

    out = {}
    conn = mrsm.get_connector('readings:office')
    out['class'] = type(conn).__name__
    out['keys'] = str(conn)
    out['is_native'] = conn.is_native
    out['same'] = mrsm.get_connector('readings', 'office') is conn
    out['is_instance_type'] = 'readings' in mrsm.connectors.instance_types

    pipe = mrsm.Pipe('readings:office', 'm', instance='sql:local')
    out['pipe_connector_is_conn'] = pipe.connector is conn
    out['register_hook'] = conn.register(pipe)
    out['register'] = list(pipe.register())
    out['parameters'] = mrsm.Pipe('readings:office', 'm', instance='sql:local').parameters
    out['sync'] = list(pipe.sync())
    out['rowcount'] = pipe.get_rowcount()
    out['data'] = pipe.get_data().to_dict(orient='records')
    out['fetch'] = conn.fetch(pipe).to_dict(orient='records')

    ### Unknown types keep v4's warning and `None`.
    out['unknown'] = mrsm.get_connector('nosuchtype:x')
    print('RESULT ' + json.dumps(out))
    """
)


@pytest.mark.skipif(not os.path.exists(CARGO), reason="single-file plugins need cargo")
def test_native_connector_type_from_python(tmp_path):
    plugins = tmp_path / 'plugins'
    plugins.mkdir()
    (plugins / 'readings.rs').write_text(CONNECTOR_TYPE)
    result, output = _run(tmp_path, 'readings', CONNECTOR_TYPE_SCRIPT)
    assert 'Traceback' not in output, output
    assert result['class'] == 'ReadingsConnector', result
    assert result['keys'] == 'readings:office', result
    assert result['is_native'] is True, result
    assert result['same'] is True, result
    assert result['is_instance_type'] is False, result
    assert result['pipe_connector_is_conn'] is True, result
    assert result['register_hook'] == {'columns': {'id': 'id'}}, result
    assert result['register'] == [
        True, "Successfully registered Pipe('readings:office', 'm', instance='sql:local')."
    ], result
    # The connector's `register()` supplied the parameters.
    assert result['parameters']['columns'] == {'id': 'id'}, result
    assert result['sync'][0] is True, result
    assert result['rowcount'] == 1, result
    # The integer `id` is an integer column (not `TEXT`), as for a Python plugin's documents.
    assert result['data'] == [{'id': 1, 'source': 'office'}], result
    assert result['fetch'] == [{'id': 1, 'source': 'office'}], result
    assert result['unknown'] is None, result
    assert "Cannot create Connector of type 'nosuchtype'." in output, output


INSTANCE_TYPE = textwrap.dedent(
    """
    use std::path::PathBuf;

    fn store_path() -> PathBuf {
        PathBuf::from(std::env::var("NATIVE_STORE").expect("NATIVE_STORE"))
    }

    fn load() -> serde_json::Map<String, Value> {
        std::fs::read_to_string(store_path())
            .ok()
            .and_then(|text| serde_json::from_str(&text).ok())
            .unwrap_or_default()
    }

    fn save(store: &serde_json::Map<String, Value>) {
        std::fs::write(store_path(), Value::Object(store.clone()).to_string()).unwrap();
    }

    fn key(pipe: &Pipe) -> String {
        format!("{}|{}|{}", pipe.connector_keys(), pipe.metric_key(), pipe.location_key().unwrap_or(""))
    }

    #[make_connector(instance)]
    mod jsonstore {
        use super::*;

        fn register_pipe(pipe: &Pipe) -> SuccessTuple {
            let mut store = load();
            store.insert(key(pipe), json!({"keys": pipe.keys(), "parameters": pipe.parameters(), "rows": []}));
            save(&store);
            SuccessTuple::ok("Registered in the JSON store.")
        }

        fn pipe_exists(pipe: &Pipe) -> bool {
            load().contains_key(&key(pipe))
        }

        fn get_pipe_id(pipe: &Pipe) -> Option<i64> {
            load().keys().position(|k| *k == key(pipe)).map(|i| i as i64 + 1)
        }

        fn get_pipe_attributes(pipe: &Pipe) -> Value {
            match load().get(&key(pipe)) {
                Some(entry) => json!({"parameters": entry["parameters"]}),
                None => json!({}),
            }
        }

        fn fetch_pipes_keys() -> Value {
            Value::Array(load().values().map(|e| {
                json!([e["keys"]["connector_keys"], e["keys"]["metric_key"], e["keys"]["location_key"]])
            }).collect())
        }

        fn sync_pipe(pipe: &Pipe, data: Vec<Value>) -> (bool, String) {
            let mut store = load();
            let Some(entry) = store.get_mut(&key(pipe)) else {
                return (false, format!("{} is not registered.", key(pipe)));
            };
            let rows = entry["rows"].as_array_mut().unwrap();
            let mut inserted = 0;
            for row in data {
                if !rows.contains(&row) {
                    rows.push(row);
                    inserted += 1;
                }
            }
            save(&store);
            (true, format!("Inserted {inserted} rows into the JSON store."))
        }

        fn get_pipe_data(pipe: &Pipe) -> Vec<Value> {
            load().get(&key(pipe)).and_then(|e| e["rows"].as_array().cloned()).unwrap_or_default()
        }

        fn get_pipe_rowcount(pipe: &Pipe) -> i64 {
            get_pipe_data(pipe).len() as i64
        }

        fn get_pipe_columns_types(pipe: &Pipe) -> Value {
            if get_pipe_data(pipe).is_empty() { json!({}) } else { json!({"id": "BIGINT", "value": "BIGINT"}) }
        }

        fn drop_pipe(pipe: &Pipe) -> SuccessTuple {
            let mut store = load();
            if let Some(entry) = store.get_mut(&key(pipe)) {
                entry["rows"] = json!([]);
            }
            save(&store);
            SuccessTuple::success()
        }

        fn delete_pipe(pipe: &Pipe) -> SuccessTuple {
            let mut store = load();
            store.remove(&key(pipe));
            save(&store);
            SuccessTuple::success()
        }
    }
    """
)

INSTANCE_SCRIPT = textwrap.dedent(
    """
    import json, sys
    import meerschaum as mrsm

    out = {}
    instance = mrsm.get_connector('jsonstore:main')
    out['class'] = type(instance).__name__
    out['is_instance'] = instance.IS_INSTANCE
    out['is_instance_type'] = 'jsonstore' in mrsm.connectors.instance_types

    pipe = mrsm.Pipe('demo', 'readings', instance='jsonstore:main', columns={'id': 'id'})
    out['pipe_instance_is_instance'] = pipe.instance_connector is instance
    out['register'] = list(pipe.register())
    out['id'] = pipe.id
    out['exists'] = pipe.exists()
    out['parameters'] = mrsm.Pipe('demo', 'readings', instance='jsonstore:main').parameters
    out['sync'] = list(pipe.sync([{'id': 1, 'value': 10}, {'id': 2, 'value': 20}]))
    out['rowcount'] = pipe.get_rowcount()
    out['data'] = pipe.get_data().to_dict(orient='records')
    out['keys'] = [list(keys) for keys in instance.fetch_pipes_keys()]
    out['pipes'] = [str(p) for p in mrsm.get_pipes(instance='jsonstore:main', as_list=True)]
    out['delete'] = list(pipe.delete())
    out['exists_after'] = mrsm.Pipe('demo', 'readings', instance='jsonstore:main').exists()
    print('RESULT ' + json.dumps(out))
    """
)


@pytest.mark.skipif(not os.path.exists(CARGO), reason="single-file plugins need cargo")
def test_native_instance_connector_from_python(tmp_path):
    plugins = tmp_path / 'plugins'
    plugins.mkdir()
    (plugins / 'store.rs').write_text(INSTANCE_TYPE)
    result, output = _run(
        tmp_path,
        'store',
        INSTANCE_SCRIPT,
        {'NATIVE_STORE': str(tmp_path / 'store.json')},
    )
    assert 'Traceback' not in output, output
    assert result['class'] == 'JsonstoreConnector', result
    assert result['is_instance'] is True, result
    assert result['is_instance_type'] is True, result
    assert result['pipe_instance_is_instance'] is True, result
    assert result['register'] == [True, 'Registered in the JSON store.'], result
    assert result['id'] == 1, result
    assert result['exists'] is True, result
    assert result['parameters']['columns'] == {'id': 'id'}, result
    assert result['sync'][0] is True, result
    assert result['rowcount'] == 2, result
    assert result['data'] == [{'id': 1, 'value': 10}, {'id': 2, 'value': 20}], result
    assert result['keys'] == [['demo', 'readings', None]], result
    assert result['pipes'] == ["Pipe('demo', 'readings', instance='jsonstore:main')"], result
    assert result['delete'][0] is True, result
    assert result['exists_after'] is False, result
