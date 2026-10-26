"""
`Pipe('plugin:<native plugin>', ...)` through the Python façade: a native (Rust) plugin is not a
Python module, so `PluginConnector` must not try to import it (which printed a
"Plugin '...' cannot be found. Is it installed?" traceback) and runs its `register(pipe)` hook
through the core instead.

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
    print('RESULT ' + json.dumps(out))
    """
)


def _run(tmp_path: pathlib.Path, name: str) -> tuple[dict, str]:
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
    proc = subprocess.run(
        [sys.executable, '-c', SCRIPT, name],
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
