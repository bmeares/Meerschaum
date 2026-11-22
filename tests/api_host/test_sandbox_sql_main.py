#! /usr/bin/env python3
# -*- coding: utf-8 -*-
# vim:fenc=utf-8

"""
Inside the API host sandbox, `sql:main` is a SQLite file under the sandbox root (never the
default database on localhost:5432, nor any network database), and it works: code that registers
users or pipes on `sql:main` writes there.
"""

import json

from tests.api_host._helpers import run_python, sandbox_main_db


def test_sql_main_is_the_sandbox_sqlite_file(sandbox):
    root, plugins = sandbox
    for host_mode in (True, False):
        result = run_python(root, plugins, '''
import json
import meerschaum as mrsm
from meerschaum.config import get_config
from meerschaum.core import User
conn = mrsm.get_connector('sql:main')
attributes = get_config('meerschaum', 'connectors', 'sql', 'main')
registered = conn.register_user(User('sandbox-guard-%s', 'guard-password-1', instance=conn))
print(json.dumps({
    'flavor': conn.flavor,
    'database': str(conn.database),
    'attributes': {k: str(v) for k, v in attributes.items()},
    'registered': list(registered),
}))
''' % int(host_mode), host_mode=host_mode)
        assert result.returncode == 0, result.stderr[-3000:]
        out = json.loads(result.stdout.strip().splitlines()[-1])
        assert out['flavor'] == 'sqlite', out
        assert out['database'] == str(sandbox_main_db(root)), out
        assert out['attributes']['database'] == str(sandbox_main_db(root)), out
        assert out['registered'][0] is True, out
    assert sandbox_main_db(root).is_file()
