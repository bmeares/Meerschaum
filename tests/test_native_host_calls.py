"""
Host calls of native (Rust) plugins driven from Python: `mrsm.Pipe('plugin:hosted', ...).sync()`
runs a single-file plugin whose `sync` reads another pipe (`host::pipe(...).get_data(...)`),
counts its rows with `host::read_sql(...)`, clears its own rows and rewrites them, with a factor
from its plugin config (patched into the live config through `MRSM_CONFIG`); a native connector
type's `fetch` called from Python reads a pipe through the host too; the plugin's `register`
hook makes a host call; and a `sync` that writes the plugin's config through the host is read
back by the next run, on disk and in Python's live config.

Each case runs in a subprocess with its own root, plugins directory and SQLite instance.
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

HOSTED = textwrap.dedent(
    """
    fn sync(pipe: &Pipe, begin: Option<Bound>, end: Option<Bound>) -> Result<SuccessTuple> {
        let config = host::plugin_config()?;
        let factor = config["factor"].as_i64().unwrap_or(10);
        if pipe.metric_key() == "counter" {
            // Each run counts itself in the plugin's config.
            let runs = config["runs"].as_i64().unwrap_or(0) + 1;
            let written = host::write_plugin_config(&json!({"factor": factor, "runs": runs}))?;
            return Ok(SuccessTuple {
                success: written.success,
                message: format!("runs {runs}: {}", written.message),
            });
        }
        if pipe.metric_key() == "parent" {
            return pipe.sync(vec![
                json!({"ts": "2024-01-01T00:00:00+00:00", "value": 1}),
                json!({"ts": "2024-01-01T01:00:00+00:00", "value": 2}),
            ]);
        }
        let parent = host::pipe(pipe.connector_keys(), "parent", None, None)?;
        let counted = host::read_sql(
            pipe.instance_keys(),
            &format!("SELECT COUNT(*) AS n FROM \\"{}\\"", parent.target()),
        )?;
        let rows: Vec<Value> = parent
            .get_data(begin.clone(), end.clone(), None)?
            .into_iter()
            .map(|mut row| {
                row["value"] = json!(row["value"].as_i64().unwrap_or(0) * factor);
                row
            })
            .collect();
        let cleared = pipe.clear(begin, end, None)?;
        if !cleared.success {
            return Ok(cleared);
        }
        let synced = pipe.sync(rows)?;
        Ok(SuccessTuple {
            success: synced.success,
            message: format!("parent rows {}", counted[0]["n"]),
        })
    }

    /// New pipes get a note from the plugin's config (a host call in `register`).
    fn register(pipe: &Pipe) -> Result<Value> {
        Ok(json!({"note": format!("{} factor={}", pipe.metric_key(), host::plugin_config()?["factor"])}))
    }

    #[make_connector]
    mod mirror {
        fn fetch(pipe: &Pipe) -> Result<Vec<Value>> {
            host::pipe("plugin:hosted", "parent", None, Some(pipe.instance_keys()))?
                .get_data(None, None, None)
        }
    }
    """
)

SCRIPT = textwrap.dedent(
    """
    import json
    import os
    import meerschaum as mrsm

    out = {}
    kwargs = dict(instance='sql:local', columns={'datetime': 'ts'}, dtypes={'value': 'int'})
    parent = mrsm.Pipe('plugin:hosted', 'parent', **kwargs)
    out['parent'] = list(parent.sync())
    child = mrsm.Pipe('plugin:hosted', 'child', **kwargs)
    out['child'] = list(child.sync())
    out['rerun'] = list(child.sync())
    out['values'] = [int(v) for v in child.get_data()['value']]
    out['fetch'] = [
        int(row['value'])
        for row in mrsm.get_connector('mirror:x').fetch(parent).to_dict(orient='records')
    ]
    out['note'] = mrsm.Pipe('plugin:hosted', 'child', instance='sql:local').parameters.get('note')
    counter = mrsm.Pipe('plugin:hosted', 'counter', **kwargs)
    out['counter'] = [list(counter.sync()), list(counter.sync())]
    with open(os.path.join(os.environ['MRSM_ROOT_DIR'], 'config', 'plugins.json')) as f:
        out['plugins_file'] = json.load(f)
    # The next native call takes the native write into the live config dictionary.
    parent.sync()
    out['runs'] = mrsm.get_config('plugins', 'hosted', 'runs')
    print('RESULT ' + json.dumps(out))
    """
)


@pytest.mark.skipif(not os.path.exists(CARGO), reason="single-file plugins need cargo")
def test_native_plugin_host_calls_from_python(tmp_path):
    plugins = tmp_path / 'plugins'
    plugins.mkdir()
    (plugins / 'hosted.rs').write_text(HOSTED)
    root = tmp_path / 'root'
    root.mkdir()
    env = {
        key: value
        for key, value in os.environ.items()
        if not key.startswith('MRSM_') and key not in ('TMUX', 'TMUX_PANE')
    }
    env.update({
        'PYTHONPATH': str(REPO),
        'MRSM_ROOT_DIR': str(root),
        'MRSM_PLUGINS_DIR': str(plugins),
        'MRSM_NO_UPDATE_CHECK': '1',
        'MRSM_NOASK': 'true',
        ### The checks read pandas frames (`meerschaum:dataframe`, rc15).
        'MRSM_DATAFRAME': 'pandas',
        'MRSM_SQL_LOCAL': json.dumps({'flavor': 'sqlite', 'database': str(tmp_path / 'local.db')}),
        'MRSM_CONFIG': json.dumps({'plugins': {'hosted': {'factor': 100}}}),
        'PATH': os.path.dirname(CARGO) + os.pathsep + env.get('PATH', ''),
    })
    proc = subprocess.run(
        [sys.executable, '-c', SCRIPT],
        env=env,
        capture_output=True,
        text=True,
        timeout=900,
    )
    output = proc.stdout + proc.stderr
    lines = [line for line in proc.stdout.splitlines() if line.startswith('RESULT ')]
    assert lines, output
    result = json.loads(lines[-1][len('RESULT '):])
    assert 'Traceback' not in output, output
    assert result['parent'][0] is True, result
    assert result['child'] == [True, 'parent rows 2'], result
    # The child's rows are cleared and rewritten, so a rerun changes nothing.
    assert result['rerun'] == [True, 'parent rows 2'], result
    assert result['values'] == [100, 200], result
    assert result['fetch'] == [1, 2], result
    # `register` made a host call (the factor patched in through `MRSM_CONFIG`).
    assert result['note'] == 'child factor=100', result
    # A native plugin writes its config through the façade, and the next run reads it back.
    assert result['counter'] == [
        [True, "runs 1: Wrote the config of plugin 'hosted'."],
        [True, "runs 2: Wrote the config of plugin 'hosted'."],
    ], result
    assert result['plugins_file'] == {'hosted': {'factor': 100, 'runs': 2}}, result
    # Python's live config sees the native write too.
    assert result['runs'] == 2, result
