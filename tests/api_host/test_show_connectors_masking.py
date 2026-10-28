#! /usr/bin/env python3
# -*- coding: utf-8 -*-
# vim:fenc=utf-8

"""
`mrsm show connectors` masks connector attributes by the rules of the web console's Connectors
page (`meerschaum.api.dash.connector_listing`): the CLI's Rust port
(`crates/meerschaum-cli/src/native/connector_mask.rs`) and the console's helper must agree. The
CLI draws its mask in ASCII (`******`), the console as `••••••`.

Both run in subprocesses with a temporary root; the connectors are synthetic (an `MRSM_CONFIG`
patch).
"""

import json
import os
import subprocess
import sys
import textwrap

import pytest

from tests.api_host._helpers import REPO

#: Connector attributes by label, for type `case` (its URI paths are masked).
CASES = {
    'pg': {
        'flavor': 'postgresql', 'host': 'db.example.com', 'port': 5432, 'username': 'mrsm',
        'password': 'hunter2pw', 'database': 'meerschaum',
    },
    'uri': {
        'uri': 'postgresql://mrsm:hunter2pw@db.example.com:5432/meerschaum?sslmode=require&password=x',
    },
    'at_in_password': {'uri': 'mysql://user:p@ss@w0rd@db.example.com:3306/data'},
    'odbc': {'uri': 'Driver={ODBC Driver 18};Server=h;Uid=u;Pwd=p@ssw0rd!;Encrypt=yes;'},
    'libpq': {'dsn': "host=h user=u password='a secret' dbname=d sslmode=require"},
    'token_userinfo': {'uri': 'https://tok3nvalue@hooks.example.com/services/T0/B1'},
    'names': {
        'api_key': 'k3y-abc-123', 'client_secret': 'cl1ent', 'pass': 'p4ss', 'pw': 'pw-value',
        'ssh_passphrase': 'open sesame', 'cookie': 'c00kie', 'stations': ['a', 'b'],
        'region': 'us-east-1', 'timeout': 30, 'verify': False, 'empty': '', 'nothing': None,
    },
    'nested': {
        'options': {'sslmode': 'require', 'sslkey': '/keys/client.key', 'application_name': 'mrsm'},
        'credentials': {'user': 'u', 'secret': 'zzz999'},
        '_hidden': 'never',
    },
    'scrubbed': {
        'password': 's3cretpw', 'host': 's3cretpw.example.com',
        'uri': 'postgresql://u:s3cretpw@h/db',
    },
    'text_secrets': {
        'host': 'h', 'driver': 'token=abc123 and Bearer abcdef123456',
        'schema': 'x password=hunter2pw y',
    },
    'percent': {'uri': 'postgresql://u:p%40ss%21word@h:5432/db'},
    'opaque': {'uri': 'just-some-text', 'connection_string': 'a=1&key=2&sslmode=disable'},
    'bad_port': {'uri': 'postgresql://u@h:notaport/db'},
}

#: Type `sql` (its URI path is the database).
SQL_CASES = {
    'p2mask_uri': {'uri': 'postgresql://mrsm:hunter2pw@db.example.com:5432/meerschaum'},
    'p2mask_sqlite': {'uri': 'sqlite:////var/lib/data/x.db'},
    'p2mask_attrs': {
        'flavor': 'mssql', 'host': 'h', 'username': 'sa', 'password': 'Sup3r!', 'database': 'db',
        'options': {'driver': 'ODBC Driver 18', 'TrustServerCertificate': 'yes', 'apikey': 'k'},
    },
}

SECRETS = (
    'hunter2pw', 'p@ss@w0rd', 'p@ssw0rd!', 'a secret', 'tok3nvalue', 'k3y-abc-123', 'cl1ent',
    'p4ss', 'pw-value', 'open sesame', 'c00kie', 'zzz999', 's3cretpw', 'abc123', 'abcdef123456',
    'Sup3r!', '/keys/client.key',
)


@pytest.fixture
def env(tmp_path):
    root = tmp_path / 'root'
    plugins = tmp_path / 'plugins'
    root.mkdir()
    plugins.mkdir()
    out = {
        key: value
        for key, value in os.environ.items()
        if not key.startswith('MRSM_') and key not in ('TMUX', 'TMUX_PANE', 'PYTHONPATH')
    }
    out.update({
        'PYTHONPATH': str(REPO),
        'MRSM_ROOT_DIR': str(root),
        'MRSM_PLUGINS_DIR': str(plugins),
        'MRSM_NO_AUTO_INSTALL': '1',
        'MRSM_CONFIG': json.dumps({'meerschaum': {'connectors': {'case': CASES, 'sql': SQL_CASES}}}),
        'NO_COLOR': '1',
    })
    return out


def _mrsm(env, *args):
    proc = subprocess.run(
        [sys.executable, '-m', 'meerschaum', 'show', 'connectors', *args],
        env=env,
        capture_output=True,
        text=True,
        timeout=180,
    )
    assert proc.returncode == 0, proc.stdout + proc.stderr
    return proc.stdout


def _json_lines(text):
    return [json.loads(line) for line in text.splitlines() if line.strip().startswith('{')]


def _console(env, cases_by_type):
    """The console helper's masks of `{type: {label: attributes}}`, with the CLI's mask."""
    code = textwrap.dedent('''
        import json, sys
        from meerschaum.api.dash import connector_listing as cl
        cases = json.loads(sys.argv[1])
        masked = {
            typ: {label: cl.mask_attributes(attrs, typ) for label, attrs in labels.items()}
            for typ, labels in cases.items()
        }
        print('JSON:' + json.dumps(masked, ensure_ascii=False).replace(cl.MASK, '******'))
    ''')
    proc = subprocess.run(
        [sys.executable, '-c', code, json.dumps(cases_by_type)],
        env=env,
        capture_output=True,
        text=True,
        timeout=180,
    )
    assert proc.returncode == 0, proc.stdout + proc.stderr
    line = [line for line in proc.stdout.splitlines() if line.startswith('JSON:')][-1]
    return json.loads(line[len('JSON:'):])


def test_show_connectors_masks_like_the_console(env):
    console = _console(env, {'case': CASES, 'sql': SQL_CASES})
    for typ, cases in (('case', CASES), ('sql', SQL_CASES)):
        shown = _json_lines(_mrsm(env, typ, '--nopretty'))[0]
        for label in cases:
            assert shown[label] == console[typ][label], (typ, label)
    # A connector's own attributes (the last section), for a few of them.
    for label in ('pg', 'odbc', 'names', 'scrubbed'):
        shown = _json_lines(_mrsm(env, f'case:{label}', '--nopretty'))[-1]
        assert shown == console['case'][label], label


def test_no_secret_reaches_the_output(env):
    text = _mrsm(env) + _mrsm(env, '--nopretty')
    for secret in SECRETS:
        assert secret not in text, secret
