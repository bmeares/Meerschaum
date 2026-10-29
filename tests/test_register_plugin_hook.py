#! /usr/bin/env python3
# -*- coding: utf-8 -*-
# vim:fenc=utf-8

"""
`register pipes` and `Pipe.register()` ask a `plugin:` connector's `register(pipe)` for the pipe's
parameters and patch the pipe's own parameters onto them (v4: `self.parameters =
apply_patch_to_config(params, self.parameters)`). The expected parameters were recorded from v4.1.4.
"""

import json
import subprocess
import sys

import pytest

from tests._plugin_cli import REPO, marked_line, run_mrsm

_REGISTER = """
def register(pipe, **kw):
    return {
        'columns': {'datetime': 'dt', 'id': 'id'},
        'dtypes': {'val': 'float'},
        'tags': ['from_plugin'],
        'fetch': {'x': 1},
    }


def fetch(pipe, **kw):
    return []
"""

### A plugin connector needs `fetch()` or `sync()` to be built at all.
_FETCH = "def fetch(pipe, **kw):\n    return []\n"

_PLUGIN_PARAMETERS = {
    'columns': {'datetime': 'dt', 'id': 'id'},
    'dtypes': {'val': 'float'},
    'fetch': {'x': 1},
    'tags': ['from_plugin'],
}
_DEFAULT_PARAMETERS = {'columns': {'datetime': None, 'id': None}}
_COMMAND = ['register', 'pipes', '-c', 'plugin:w17breg', '-m', 'demo']


def _registered_parameters(tmp_path):
    result = run_mrsm(
        tmp_path, {}, ['show', 'pipes', '-c', 'plugin:w17breg', '-m', 'demo', '--nopretty'],
    )
    return json.loads(marked_line(result, '{'))['parameters']


@pytest.mark.parametrize(
    'plugin, extra, expected, in_output',
    [
        (_REGISTER, [], _PLUGIN_PARAMETERS, 'Successfully registered 1 pipe'),
        (
            _REGISTER,
            ['--tags', 'cli1', '--params', '{"columns":{"datetime":"cli_dt"},"fetch":{"y":2}}'],
            {
                'columns': {'datetime': 'cli_dt', 'id': 'id'},
                'dtypes': {'val': 'float'},
                'fetch': {'x': 1, 'y': 2},
                'tags': ['cli1'],
            },
            'Successfully registered 1 pipe',
        ),
        (
            "def register(pipe, **kw):\n    raise ValueError('boom')\n\n\n" + _FETCH,
            [],
            _DEFAULT_PARAMETERS,
            'boom',
        ),
        (
            "def register(pipe, **kw):\n    return ['not', 'a', 'dict']\n\n\n" + _FETCH,
            [],
            _DEFAULT_PARAMETERS,
            'Invalid parameters returned from `register()` in connector plugin:w17breg',
        ),
        (
            "def register(pipe, **kw):\n    return None\n\n\n" + _FETCH,
            [],
            _DEFAULT_PARAMETERS,
            'registered',
        ),
        ("def fetch(pipe, **kw):\n    return []\n", [], _DEFAULT_PARAMETERS, 'registered'),
    ],
    ids=['plugin', 'cli-wins', 'raises', 'not-a-dict', 'none', 'no-register'],
)
def test_register_pipes_applies_the_plugins_parameters(
    tmp_path, plugin, extra, expected, in_output,
):
    result = run_mrsm(tmp_path, {'w17breg.py': plugin}, _COMMAND + extra)
    output = result.stdout + result.stderr
    assert 'Successfully registered 1 pipe' in output, output
    assert in_output in output, output
    assert _registered_parameters(tmp_path) == expected


def test_the_facade_registers_the_same_parameters(tmp_path):
    (tmp_path / 'plugins').mkdir()
    (tmp_path / 'root').mkdir()
    (tmp_path / 'plugins' / 'w17breg.py').write_text(_REGISTER, encoding='utf-8')
    code = (
        "import json\n"
        "import meerschaum as mrsm\n"
        "pipe = mrsm.Pipe('plugin:w17breg', 'demo', instance='sql:regtest')\n"
        "print('REGISTERED', pipe.register()[0])\n"
        "print('PARAMETERS', json.dumps(pipe.get_parameters(apply_symlinks=False), sort_keys=True))\n"
    )
    import os
    env = {
        **os.environ,
        'PYTHONPATH': str(REPO),
        'MRSM_ROOT_DIR': str(tmp_path / 'root'),
        'MRSM_PLUGINS_DIR': str(tmp_path / 'plugins'),
        'MRSM_PYTHON': sys.executable,
        'MRSM_SQL_REGTEST': f"sqlite:///{tmp_path / 'regtest.db'}",
    }
    result = subprocess.run(
        [sys.executable, '-c', code], capture_output=True, text=True, env=env, timeout=180,
    )
    output = result.stdout + result.stderr
    assert 'REGISTERED True' in output, output
    line = [l for l in output.splitlines() if l.startswith('PARAMETERS ')][0]
    assert json.loads(line[len('PARAMETERS '):]) == _PLUGIN_PARAMETERS


@pytest.mark.parametrize('call', ['sync()', 'sync(parents=True)'], ids=['plain', 'engine'])
def test_syncing_an_unregistered_plugin_pipe_registers_the_plugins_parameters(tmp_path, call):
    """
    `Pipe.sync()` registers a pipe it finds unregistered through `Pipe.register()` (the plugin's
    `register()` hook included): a bare sync and the engine (`parents=True`) alike.
    """
    import os
    (tmp_path / 'plugins').mkdir()
    (tmp_path / 'root').mkdir()
    source = _REGISTER.replace(
        "return []", "return [{'dt': '2024-01-01', 'id': 1, 'val': 1.5}]",
    )
    (tmp_path / 'plugins' / 'w17breg.py').write_text(source, encoding='utf-8')
    code = (
        "import json\n"
        "import meerschaum as mrsm\n"
        "pipe = mrsm.Pipe('plugin:w17breg', 'demo', instance='sql:regtest')\n"
        f"print('SYNCED', pipe.{call}[0])\n"
        "pipe = mrsm.Pipe('plugin:w17breg', 'demo', instance='sql:regtest')\n"
        "print('PARAMETERS', json.dumps(pipe.get_parameters(apply_symlinks=False), sort_keys=True))\n"
    )
    env = {
        **os.environ,
        'PYTHONPATH': str(REPO),
        'MRSM_ROOT_DIR': str(tmp_path / 'root'),
        'MRSM_PLUGINS_DIR': str(tmp_path / 'plugins'),
        'MRSM_PYTHON': sys.executable,
        'MRSM_SQL_REGTEST': f"sqlite:///{tmp_path / 'regtest.db'}",
    }
    result = subprocess.run(
        [sys.executable, '-c', code], capture_output=True, text=True, env=env, timeout=180,
    )
    output = result.stdout + result.stderr
    assert 'SYNCED True' in output, output
    line = [l for l in output.splitlines() if l.startswith('PARAMETERS ')][0]
    assert json.loads(line[len('PARAMETERS '):]) == _PLUGIN_PARAMETERS
