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
