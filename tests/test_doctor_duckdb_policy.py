"""Doctor observes DuckDB host policy and metadata without importing or opening the driver."""
import json
import os
from pathlib import Path
import signal
import subprocess
import sys
import textwrap

import pytest


REPO = Path(__file__).resolve().parents[1]


def _run(tmp_path, program, version='99.0.0', plugin=False):
    root = tmp_path / 'root'
    root.mkdir()
    fake = tmp_path / 'fake'
    package = fake / 'duckdb'
    package.mkdir(parents=True)
    (package / '__init__.py').write_text("raise AssertionError('doctor imported optional DuckDB')\n")
    if version is not None:
        metadata = fake / f'duckdb-{version}.dist-info'
        metadata.mkdir()
        (metadata / 'METADATA').write_text(f'Name: duckdb\nVersion: {version}\n')
    database = root / 'existing.duckdb'
    database.write_bytes(b'not a database: diagnostics must not open this file')
    env = {key: value for key, value in os.environ.items()
           if not key.startswith(('MRSM_', 'PYTHON')) and key not in ('TMUX', 'TMUX_PANE')}
    env.update(
        PYTHONPATH=os.pathsep.join((str(fake), str(REPO))), PYTHONDONTWRITEBYTECODE='1',
        MRSM_ROOT_DIR=str(root), MRSM_PLUGINS_DIR=str(tmp_path / 'plugins'),
        MRSM_VENVS_DIR=str(tmp_path / 'venvs'), MRSM_NO_AUTO_INSTALL='1',
        MRSM_NO_UPDATE_CHECK='1', MRSM_PYTHON=sys.executable,
        MRSM_CONFIG=json.dumps({'meerschaum': {'instance': 'sql:main', 'connectors': {'sql': {
            'main': {'flavor': 'sqlite', 'database': str(root / 'fallback.sqlite')},
            'w17cduckdoc': {'flavor': 'duckdb', 'database': str(database)},
        }}}}),
    )
    (tmp_path / 'plugins').mkdir()
    if plugin:
        (tmp_path / 'plugins' / 'w17c_doctor_sentinel.py').write_text(
            "import os\nfrom pathlib import Path\n"
            "Path(os.environ['MRSM_ROOT_DIR'], 'plugin-imported').write_text('imported')\n"
        )
    process = subprocess.Popen(
        [sys.executable, '-B', '-s', '-c', textwrap.dedent(program), str(root), str(database)],
        cwd=tmp_path, env=env, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True,
        start_new_session=(os.name != 'nt'),
    )
    try:
        output, _ = process.communicate(timeout=30)
    except subprocess.TimeoutExpired:
        if os.name == 'nt':
            process.kill()
        else:
            os.killpg(process.pid, signal.SIGKILL)
        output, _ = process.communicate()
        pytest.fail('doctor scratch process timed out:\n' + output)
    assert process.returncode == 0, output


@pytest.mark.parametrize('version, expected', [('99.0.0', 'PASS'), ('0.1.0', 'FAIL')])
def test_doctor_python_duckdb_metadata_skips_database_without_imports(tmp_path, version, expected):
    _run(tmp_path, f'''
        import contextlib, io, json, sys
        from pathlib import Path
        import meerschaum as mrsm
        from meerschaum._core import _native
        root, database = map(Path, sys.argv[1:])
        before = database.read_bytes()
        helper = 'meerschaum.connectors.sql._duckdb_transport'
        assert helper not in sys.modules and 'duckdb' not in sys.modules
        fact = json.loads(_native._duckdb_diagnostic())
        assert fact['transport'] == 'python' and fact['pinned'] is False, fact
        output = io.StringIO()
        with contextlib.redirect_stdout(output):
            success, message = mrsm.entry(['doctor', '--nopretty', '-i', 'sql:w17cduckdoc'])
        report = json.loads(output.getvalue())
        driver = next(c for c in report['checks'] if c['group']=='drivers' and c['name']=='duckdb')
        assert driver['status'] == {expected!r}, driver
        assert 'metadata' in driver['detail'] and 'not probed' in driver['detail'], driver
        instance = next(c for c in report['checks'] if c['group']=='instance' and c['name']=='connection')
        assert instance['status']=='SKIP' and 'read-only' in instance['detail'], instance
        assert helper not in sys.modules and 'duckdb' not in sys.modules
        assert database.read_bytes()==before
        assert not (root/'drivers').exists()
    ''', version=version)


def test_duckdb_diagnostic_preserves_pinned_policy_and_refuses_mutations(tmp_path):
    _run(tmp_path, '''
        import json, os, sys, types
        import meerschaum
        from meerschaum._core import _native
        helper = types.ModuleType('meerschaum.connectors.sql._duckdb_transport')
        helper._PID=os.getpid()
        helper._POLICY='native'
        helper._OVERRIDE=''
        sys.modules[helper.__name__]=helper
        assert json.loads(_native._duckdb_diagnostic())['transport']=='native'
        assert helper._POLICY=='native'  # Late driver metadata never switches a pinned native host.
        helper._POLICY='python'
        fact=json.loads(_native._duckdb_diagnostic())
        assert fact['transport']=='python' and fact['pinned'] is True, fact
        assert helper._POLICY=='python' and 'duckdb' not in sys.modules
        os.environ['MRSM_DUCKDB_LIB']='changed-after-selection'
        fact=json.loads(_native._duckdb_diagnostic())
        assert fact['transport']=='refused' and 'changed' in fact['problem'], fact
        os.environ.pop('MRSM_DUCKDB_LIB')
        helper._PID += 1
        fact=json.loads(_native._duckdb_diagnostic())
        assert fact['transport']=='refused' and 'parent process' in fact['problem'], fact
    ''')


def test_duckdb_metadata_unknown_warns_without_pinning_or_import(tmp_path):
    _run(tmp_path, '''
        import contextlib, importlib.metadata, io, json, sys
        import meerschaum as mrsm
        from meerschaum._core import _native
        original = importlib.metadata.distribution
        def missing_driver(name):
            if name == 'duckdb':
                raise importlib.metadata.PackageNotFoundError(name)
            return original(name)
        importlib.metadata.distribution = missing_driver
        fact = json.loads(_native._duckdb_diagnostic())
        assert fact['transport']=='python' and fact['version'] is None and not fact['pinned'], fact
        output = io.StringIO()
        with contextlib.redirect_stdout(output):
            success, message = mrsm.entry(['doctor', '--nopretty', '-i', 'sql:w17cduckdoc'])
        checks=json.loads(output.getvalue())['checks']
        driver=next(c for c in checks if c['group']=='drivers' and c['name']=='duckdb')
        assert driver['status']=='WARN' and 'metadata unavailable' in driver['detail'], driver
        assert 'duckdb' not in sys.modules
        assert 'meerschaum.connectors.sql._duckdb_transport' not in sys.modules
    ''')


@pytest.mark.parametrize('call', ['entry', 'action', 'patch', 'malformed', 'overflow'])
def test_sdk_doctor_does_not_preload_plugins_or_write_manifests(tmp_path, call):
    _run(tmp_path, f'''
        import contextlib, io, json, os, sys
        from pathlib import Path
        import meerschaum as mrsm
        root = Path(sys.argv[1])
        if {call!r} == 'action':
            # Importing the public registry explicitly discovers plugins before any action.
            # Doctor owns the subsequent call, not that separate registry initialization.
            from meerschaum.actions import actions
            import meerschaum.plugins as plugins
            marker = root / 'plugin-imported'
            assert marker.read_text() == 'imported', 'registry setup did not exercise discovery'
            marker.unlink()
            # Make discovery stale: a cached import cannot hide an unintended worker rebuild.
            Path(os.environ['MRSM_PLUGINS_DIR'], 'w17c_doctor_late.py').write_text(
                "from pathlib import Path\\n"
                + "Path(" + repr(str(marker)) + ").write_text('late discovery')\\n"
            )
            def refuse_discovery(*args, **kwargs):
                raise AssertionError('doctor requested in-process plugin discovery')
            plugins.load_plugins = refuse_discovery
        cache = root / 'cache'
        def snapshot():
            return {{str(p.relative_to(cache)): p.read_bytes()
                    for p in cache.rglob('*') if p.is_file()}} if cache.exists() else {{}}
        before = snapshot()
        output = io.StringIO()
        with contextlib.redirect_stdout(output):
            if {call!r} == 'entry':
                result = mrsm.entry(['doctor', '--nopretty', '-i', 'sql:w17cduckdoc'])
            elif {call!r} == 'action':
                result = actions['doctor'](nopretty=True, mrsm_instance='sql:w17cduckdoc')
            elif {call!r} == 'patch':
                result = mrsm.entry(['show', 'version', '--nopretty', '-i', 'sql:w17cduckdoc'],
                                    _patch_args={{'action': ['doctor']}})
            elif {call!r} == 'malformed':
                result = mrsm.entry(['doctor', '--port', 'not-a-number'])
                assert result[0] is False, result
            else:
                result = mrsm.entry(['doctor', '--chunk-days', '9223372036854775807'])
                assert result[0] is False and 'OverflowError' in result[1], result
        if {call!r} in ('entry', 'action', 'patch'):
            assert result[0] is True, result
            checks = json.loads(output.getvalue())['checks']
            connection = next(c for c in checks if c['group']=='instance' and c['name']=='connection')
            assert connection['status']=='SKIP', connection
        assert not (root / 'plugin-imported').exists(), 'doctor imported a plugin'
        assert snapshot() == before, 'doctor changed plugin / entry-point caches'
    ''', plugin=True)


@pytest.mark.parametrize('call', ['chain', 'reverse-patch', 'show-first'])
def test_sdk_non_doctor_steps_retain_normal_plugin_loading(tmp_path, call):
    _run(tmp_path, f'''
        import contextlib, io, sys
        from pathlib import Path
        import meerschaum as mrsm
        root = Path(sys.argv[1])
        with contextlib.redirect_stdout(io.StringIO()):
            if {call!r} == 'chain':
                result = mrsm.entry(['doctor', '--nopretty', '-i', 'sql:w17cduckdoc',
                                     '+', 'show', 'version', '--nopretty'])
            elif {call!r} == 'reverse-patch':
                result = mrsm.entry(['doctor', '--nopretty'],
                                    _patch_args={{'action': ['show', 'version']}})
            else:
                result = mrsm.entry(['show', 'version', '--nopretty', '+',
                                     'doctor', '--nopretty', '-i', 'sql:w17cduckdoc'])
        assert result[0] is True, result
        assert (root / 'plugin-imported').read_text() == 'imported'
        assert (root / 'cache' / 'plugins_manifest.json').is_file()
    ''', plugin=True)
