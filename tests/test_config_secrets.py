#! /usr/bin/env python3
# -*- coding: utf-8 -*-
# vim:fenc=utf-8

"""
5.1 secret handling, from Python and from the CLI: `MRSM{file:...}` / `MRSM{env:...}` references,
`MRSM_CONFIG_FILE` / `MRSM_PATCH_FILE`, the writer that never persists what the environment or a
reference holds, and the masking of values a reference produced.

Each test runs in subprocesses with a fresh root and only the variables it sets, so the import-time
environment layers are those of the test. Every value is synthetic.
"""

import json
import os
import pathlib
import subprocess
import sys
import textwrap

import pytest

REPO = pathlib.Path(__file__).resolve().parent.parent
FILE_SECRET = 'py-file-s3cret-value'
ENV_SECRET = 'py-env-s3cret-value'
CONFIG_ONLY = 'py-config-only-s3cret'
URI_ONLY = 'py-uri-only-s3cret'


def _env(root: pathlib.Path, **extra) -> dict:
    env = {
        key: value
        for key, value in os.environ.items()
        if not key.startswith('MRSM_') and key not in ('TMUX', 'TMUX_PANE')
    }
    env.update({
        'MRSM_ROOT_DIR': str(root),
        'MRSM_PLUGINS_DIR': str(root / 'plugins'),
        'PYTHONPATH': str(REPO),
        'MRSM_NOASK': 'true',
        'PY_SECRET_VAR': ENV_SECRET,
    })
    env.update({key: str(value) for key, value in extra.items()})
    return env


def _root(tmp_path: pathlib.Path) -> pathlib.Path:
    root = tmp_path / 'root'
    (root / 'config').mkdir(parents=True)
    (root / 'plugins').mkdir()
    (root / 'pg-password').write_text(FILE_SECRET + '\n')
    config = root / 'config' / 'meerschaum.yaml'
    config.write_text(textwrap.dedent(f"""\
        instance: sql:local
        connectors:
          sql:
            written:
              flavor: postgresql
              host: db.example
              username: u
              database: d
              password: MRSM{{file:pg-password}}
            envy:
              flavor: postgresql
              host: db.example
              username: u
              database: d
              password: MRSM{{env:PY_SECRET_VAR}}
            linked:
              flavor: sqlite
              database: MRSM{{meerschaum:connectors:sql:local:database}}
    """))
    os.chmod(config, 0o600)
    return root


def _python(root: pathlib.Path, code: str, **extra) -> dict:
    proc = subprocess.run(
        [sys.executable, '-c', textwrap.dedent(code)],
        env=_env(root, **extra),
        capture_output=True,
        text=True,
        timeout=180,
        cwd=str(root),
    )
    assert proc.returncode == 0, proc.stdout + proc.stderr
    return json.loads(proc.stdout.strip().splitlines()[-1])


def _cli(root: pathlib.Path, *args, **extra) -> subprocess.CompletedProcess:
    return subprocess.run(
        [sys.executable, '-m', 'meerschaum', *args],
        env=_env(root, **extra),
        capture_output=True,
        text=True,
        timeout=180,
        cwd=str(root),
    )


def test_references_resolve_in_python_and_connectors(tmp_path):
    root = _root(tmp_path)
    result = _python(
        root,
        """
        import json
        import meerschaum as mrsm
        from meerschaum.config import get_config
        out = {
            'file': get_config('meerschaum', 'connectors', 'sql', 'written', 'password'),
            'env': get_config('meerschaum', 'connectors', 'sql', 'envy', 'password'),
            'patched': get_config('meerschaum', 'connectors', 'api', 'patched', 'password'),
            'envconn': get_config('meerschaum', 'connectors', 'sql', 'envconn', 'password'),
            'conn': mrsm.get_connector('sql:written').password,
            'known': sorted(mrsm._core.secret_values()),
        }
        print(json.dumps(out))
        """,
        MRSM_CONFIG=json.dumps({'meerschaum': {'connectors': {'api': {'patched': {
            'host': 'h', 'password': 'MRSM{env:PY_SECRET_VAR}',
        }}}}}),
        MRSM_SQL_ENVCONN=json.dumps({
            'flavor': 'postgresql', 'host': 'h', 'username': 'u', 'database': 'd',
            'password': 'MRSM{file:pg-password}',
        }),
    )
    assert result['file'] == FILE_SECRET
    assert result['env'] == ENV_SECRET
    assert result['patched'] == ENV_SECRET
    assert result['envconn'] == FILE_SECRET
    assert result['conn'] == FILE_SECRET
    assert FILE_SECRET in result['known'] and ENV_SECRET in result['known']


def test_a_rotated_secret_reaches_the_next_connector(tmp_path):
    root = _root(tmp_path)
    result = _python(
        root,
        """
        import json, os, pathlib
        import meerschaum as mrsm
        from meerschaum.config import get_config
        first = mrsm.get_connector('sql:written').password
        root = pathlib.Path(os.environ['MRSM_ROOT_DIR'])
        (root / 'next').write_text('rotated-py-s3cret\\n')
        os.replace(root / 'next', root / 'pg-password')
        second = get_config('meerschaum', 'connectors', 'sql', 'written', 'password')
        conn = mrsm.get_connector('sql', 'written', refresh=True)
        print(json.dumps({'first': first, 'second': second, 'conn': conn.password}))
        """,
    )
    assert result == {
        'first': FILE_SECRET,
        'second': 'rotated-py-s3cret',
        'conn': 'rotated-py-s3cret',
    }


def test_an_unresolved_reference_fails_only_its_connector(tmp_path):
    root = _root(tmp_path)
    (root / 'pg-password').unlink()
    result = _python(
        root,
        """
        import json
        import meerschaum as mrsm
        from meerschaum.config import get_config
        out = {'instance': get_config('meerschaum', 'instance')}
        out['envy'] = mrsm.get_connector('sql:envy').password
        import io, contextlib
        from meerschaum.connectors.sql import SQLConnector
        try:
            SQLConnector('written')
            out['error'] = None
        except Exception as e:
            out['error'] = str(e)
        ### `get_connector` reports the failure and returns `None`, as for any other.
        buffer = io.StringIO()
        with contextlib.redirect_stderr(buffer):
            out['conn'] = repr(mrsm.get_connector('sql:written'))
        out['warning'] = buffer.getvalue()
        print(json.dumps(out))
        """,
    )
    assert result['instance'] == 'sql:local'
    assert result['envy'] == ENV_SECRET
    assert 'MRSM{file:pg-password}' in result['error']
    assert 'meerschaum:connectors:sql:written:password' in result['error']
    assert 'the file does not exist' in result['error']
    assert ENV_SECRET not in result['error']
    assert result['conn'] == 'None'

    # The CLI starts, shows the config and fails only where the connector is built.
    shown = _cli(root, 'show', 'config', 'meerschaum', '--nopretty')
    assert shown.returncode == 0, shown.stdout + shown.stderr
    assert ENV_SECRET not in shown.stdout
    used = _cli(root, 'show', 'pipes', '-i', 'sql:written')
    assert 'MRSM{file:pg-password}' in used.stdout + used.stderr


def test_python_writers_never_persist_environment_secrets(tmp_path):
    root = _root(tmp_path)
    _python(
        root,
        """
        import json
        from meerschaum.config import get_config, write_config
        section = get_config('meerschaum')
        section['connectors']['sql']['written']['port'] = 6543
        assert write_config({'meerschaum': section})
        assert write_config()
        print(json.dumps({}))
        """,
        MRSM_CONFIG=json.dumps({'meerschaum': {'connectors': {'api': {'cfg': {
            'host': 'h', 'password': CONFIG_ONLY,
        }}}}}),
        MRSM_SQL_ENVCONN=f'postgresql://u:{URI_ONLY}@h:5432/d',
    )
    text = (root / 'config' / 'meerschaum.yaml').read_text()
    for secret in (FILE_SECRET, ENV_SECRET, CONFIG_ONLY, URI_ONLY):
        assert secret not in text, text
    assert 'MRSM{file:pg-password}' in text
    assert 'MRSM{env:PY_SECRET_VAR}' in text
    assert 'MRSM{meerschaum:connectors:sql:local:database}' in text
    assert 'port: 6543' in text


def test_the_console_masks_values_a_reference_produced(tmp_path):
    root = _root(tmp_path)
    result = _python(
        root,
        """
        import json
        from meerschaum.config import get_config
        from meerschaum.api.dash.connector_listing import scrub, mask_attributes
        attributes = dict(get_config('meerschaum', 'connectors', 'sql', 'envy'))
        attributes['host'] = attributes['password']
        print(json.dumps({
            'scrubbed': scrub('failed for ' + attributes['password'], []),
            'masked': mask_attributes(attributes, 'sql'),
        }))
        """,
    )
    assert ENV_SECRET not in json.dumps(result)


def test_config_file_variables_and_the_cli_agree(tmp_path):
    root = _root(tmp_path)
    config_file = tmp_path / 'mrsm-config.json'
    config_file.write_text(json.dumps({'meerschaum': {'connectors': {'sql': {'fromfile': {
        'flavor': 'sqlite', 'database': 'MRSM{file:db-path}',
    }}}}}))
    db_path = tmp_path / 'from-a-secret.db'
    (root / 'db-path').write_text(str(db_path) + '\n')
    patch_file = tmp_path / 'mrsm-patch.json'
    patch_file.write_text(json.dumps({'plugins': {'p': {'setting': 7}}}))
    extra = {'MRSM_CONFIG_FILE': config_file, 'MRSM_PATCH_FILE': patch_file}
    result = _python(
        root,
        """
        import json
        import meerschaum as mrsm
        from meerschaum.config import get_config
        print(json.dumps({
            'database': mrsm.get_connector('sql:fromfile').database,
            'setting': get_config('plugins', 'p', 'setting'),
            'connectors': sorted(get_config('meerschaum', 'connectors')),
        }))
        """,
        **extra,
    )
    assert result['database'] == str(db_path)
    assert result['setting'] == 7
    assert 'config' not in result['connectors'] and 'patch' not in result['connectors']

    # The CLI (and the processes it starts) read the same: the SQLite file is the secret's path.
    proc = _cli(root, 'show', 'pipes', '-i', 'sql:fromfile', **extra)
    assert db_path.exists(), proc.stdout + proc.stderr

    # Both forms of one variable stop both the CLI and Python, naming both.
    proc = _cli(root, 'show', 'version', MRSM_CONFIG='{}', **extra)
    assert proc.returncode != 0
    assert 'MRSM_CONFIG' in proc.stdout + proc.stderr
    assert 'MRSM_CONFIG_FILE' in proc.stdout + proc.stderr
    proc = subprocess.run(
        [sys.executable, '-c', 'import meerschaum.config'],
        env=_env(root, MRSM_CONFIG='{}', **extra),
        capture_output=True, text=True, timeout=120,
    )
    assert proc.returncode != 0
    assert 'MRSM_CONFIG_FILE' in proc.stderr

    # `MRSM_SQL_MAIN_FILE` is still the connector `sql:main_file`.
    result = _python(
        root,
        """
        import json
        from meerschaum.config import get_config
        print(json.dumps(get_config('meerschaum', 'connectors', 'sql', 'main_file')))
        """,
        MRSM_SQL_MAIN_FILE='sqlite:////tmp/x.db',
    )
    assert result == {'uri': 'sqlite:////tmp/x.db'}


def test_a_child_python_process_sees_the_same_configuration(tmp_path):
    """A process Meerschaum starts as itself (a worker, a job) inherits `MRSM_CONFIG_FILE`."""
    root = _root(tmp_path)
    config_file = tmp_path / 'mrsm-config.json'
    config_file.write_text(json.dumps({'plugins': {'child': {'token': 'MRSM{file:pg-password}'}}}))
    result = _python(
        root,
        """
        import json, subprocess, sys
        code = (
            "import json; from meerschaum.config import get_config; "
            "print(json.dumps(get_config('plugins', 'child', 'token')))"
        )
        child = subprocess.run(
            [sys.executable, '-c', code], capture_output=True, text=True, check=True,
        )
        print(json.dumps({'child': json.loads(child.stdout.strip().splitlines()[-1])}))
        """,
        MRSM_CONFIG_FILE=config_file,
    )
    assert result == {'child': FILE_SECRET}


def test_processes_that_are_not_meerschaum_inherit_no_secret(tmp_path):
    """
    5.1: `get_child_env()` (the one Python helper) drops `MRSM_CONFIG`, `MRSM_PATCH`, their
    `_FILE` forms and every `MRSM_<TYPE>_<LABEL>`; the editor, pip / uv and the availability
    checks start with it, and this process keeps its own environment.
    """
    root = _root(tmp_path)
    out = tmp_path / 'editor.env'
    editor = tmp_path / 'editor.sh'
    editor.write_text(f'#!/bin/sh\nenv > {out}\n')
    editor.chmod(0o755)
    result = _python(
        root,
        """
        import json, os
        from meerschaum.config.environment import get_child_env
        from meerschaum.utils.misc import edit_file
        from meerschaum.utils.packages import _get_pip_os_env
        child = get_child_env()
        edit_file(os.path.join(os.environ['MRSM_ROOT_DIR'], 'pg-password'))
        print(json.dumps({
            'child': sorted(k for k in child if k.startswith('MRSM_')),
            'pip': sorted(k for k in _get_pip_os_env() if k.startswith('MRSM_')),
            'own': sorted(k for k in os.environ if k.startswith('MRSM_')),
            'explicit': get_child_env({
                'MRSM_SQL_X': 'sqlite:////x.db', 'MRSM_CONFIG_FILE': 'f', 'A': '1',
                'MRSM_TEST_PATH': '/kept',
            }),
        }))
        """,
        MRSM_SQL_PYCHILD=f'sqlite:////tmp/x.db?password={URI_ONLY}',
        MRSM_PATCH=json.dumps({'py_child': {'password': CONFIG_ONLY}}),
        EDITOR=str(editor),
    )
    for kept in ('MRSM_ROOT_DIR', 'MRSM_PLUGINS_DIR', 'MRSM_NOASK'):
        assert kept in result['child'] and kept in result['pip'], result
    for secret in ('MRSM_SQL_PYCHILD', 'MRSM_PATCH'):
        assert secret not in result['child'] and secret not in result['pip'], result
        assert secret in result['own'], result
    assert result['explicit'] == {'A': '1', 'MRSM_TEST_PATH': '/kept'}
    text = out.read_text()
    assert URI_ONLY not in text and CONFIG_ONLY not in text, text
    assert 'MRSM_ROOT_DIR=' in text, text
