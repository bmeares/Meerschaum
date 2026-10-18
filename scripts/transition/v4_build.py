"""Build v4.1.4 state. Run with the reference interpreter: `.venv-ref/bin/python -s -E v4_build.py <spec.json>`."""
import json
import os
import pathlib
import sys

HERE = pathlib.Path(__file__).resolve().parent
sys.path.insert(0, str(HERE))
import meerschaum as mrsm  # noqa: E402
assert mrsm.__version__ == '4.1.4', mrsm.__version__
assert 'site-packages' in mrsm.__file__, mrsm.__file__
from meerschaum.config._edit import write_config  # noqa: E402
from meerschaum.core import User, Plugin, Token  # noqa: E402
import data  # noqa: E402
from flavors import flavor_attrs  # noqa: E402

spec = json.loads(pathlib.Path(sys.argv[1]).read_text())
flavors = spec['flavors']
db_dir = spec['db_dir']
out = {'pipes': [], 'tokens': {}, 'users': {}, 'notes': []}


def ok(label, tup):
    success, msg = tup
    print(f"[v4] {label}: {success} {str(msg).splitlines()[0] if msg else ''}", flush=True)
    if not success:
        out['notes'].append(f"v4 {label} FAILED: {msg}")
    return success


# ---- plugin registered to the instances ----------------------------------------------------------
def build_instance(flavor):
    import subprocess
    r = subprocess.run([sys.executable, '-s', '-E', str(HERE / 'v4_admin.py'), 'create', flavor], capture_output=True, text=True)
    print(r.stdout.strip(), r.stderr.strip()[-300:])
    inst = f"sql:trn_{flavor}"
    conn = mrsm.get_connector(inst)
    geometry = flavor == 'postgis'
    P = lambda ck, mk, lk=None, **kw: mrsm.Pipe(ck, mk, lk, instance=inst, **kw)
    # kitchen sink
    dtypes = dict(data.KITCHEN_DTYPES)
    if geometry:
        dtypes['geom'] = 'geometry[Point, 4326]'
    kitchen = P(inst, 'kitchen', parameters={
        'columns': {'datetime': 'dt', 'id': 'id'},
        'dtypes': dtypes,
        'indices': {'bycat': ['s']},
        'upsert': True,
        'tags': ['trn', 'kitchen'],
    })
    ok(f'{flavor} register kitchen', kitchen.register())
    ok(f'{flavor} sync kitchen 1', kitchen.sync(data.kitchen_df(range(0, 10), geometry=geometry)))
    ok(f'{flavor} sync kitchen 2 (updates+inserts)', kitchen.sync(data.kitchen_df(range(5, 15), shift=1, geometry=geometry)))
    # autoincrement
    auto = P(inst, 'auto', parameters={
        'columns': {'primary': 'pk'}, 'autoincrement': True, 'tags': ['trn'],
    })
    ok(f'{flavor} sync auto 1', auto.sync(data.auto_docs(4, 'a')))
    ok(f'{flavor} sync auto 2', auto.sync(data.auto_docs(3, 'b')))
    # null indices
    nul = P(inst, 'nullidx', parameters={
        'columns': {'datetime': 'dt', 'id': 'id'}, 'null_indices': True, 'tags': ['trn'],
    })
    ok(f'{flavor} sync nullidx', nul.sync(data.nullidx_df(0, 9)))
    # static
    stat = P(inst, 'static', parameters={
        'columns': {'datetime': 'dt'}, 'static': True, 'tags': ['trn', 'static'],
        'dtypes': {'dt': 'datetime', 'v': 'float', 'name': 'string'},
    })
    ok(f'{flavor} sync static', stat.sync(data.static_df(6)))
    # SQL definition reading another pipe's table via a template
    defn = P(inst, 'defn', parameters={
        'columns': {'datetime': 'dt', 'id': 'id'},
        'fetch': {'definition': f"SELECT id, dt, i, f FROM {{{{ Pipe('{inst}', 'kitchen', instance='{inst}') }}}}"},
        'tags': ['trn'],
    })
    ok(f'{flavor} register defn', defn.register())
    defn_src = defn
    ok(f'{flavor} sync defn', defn_src.sync())
    # plugin fetch pipe
    pf = mrsm.Pipe('plugin:trn_plug', 'readings', instance=inst, parameters={
        'columns': {'datetime': 'ts', 'id': 'sensor'}, 'tags': ['trn'],
    })
    ok(f'{flavor} sync plugfetch', pf.sync())
    plain = mrsm.Pipe('plugin:trn_plain', 'plain', instance=inst, parameters={'columns': {'datetime': 'ts', 'id': 'k'}, 'tags': ['trn']})
    ok(f'{flavor} sync plain-import plugin', plain.sync())
    pc = P(inst, 'plugchild', parameters={
        'columns': {'datetime': 'ts', 'id': 'sensor'},
        'fetch': {'definition': f"SELECT ts, sensor, reading * 2 AS doubled FROM {{{{ Pipe('plugin:trn_plug', 'readings', instance='{inst}') }}}}"},
        'tags': ['trn'],
    })
    ok(f'{flavor} register plugchild', pc.register())
    ok(f'{flavor} sync plugchild', pc.sync())
    for p in (kitchen, auto, nul, stat, defn_src, pf, plain, pc):
        out['pipes'].append({'instance': inst, 'ck': p.connector_keys, 'mk': p.metric_key, 'lk': p.location_key})
    # plugin + users + tokens
    admin = User('trn_admin', 'AdminPass-12345', type='admin')
    bob = User('trn_bob', 'BobPass-12345')
    ok(f'{flavor} register admin', conn.register_user(admin))
    ok(f'{flavor} register bob', conn.register_user(bob))
    admin_id = conn.get_user_id(admin)
    bob_id = conn.get_user_id(bob)
    plug = Plugin('trn_plug', version='1.0.0', user_id=admin_id)
    ok(f'{flavor} register plugin', conn.register_plugin(plug))
    toks = {}
    for label, user, scopes in (
        ('trn-admin-all', User('trn_admin', instance=inst), ['*']),
        ('trn-bob-ro', User('trn_bob', instance=inst), ['pipes:read', 'instance:read']),
    ):
        user._instance_connector = conn
        t = Token(label=label, user=user, scopes=scopes, instance=inst)
        t.user  # touch
        t.generate_credentials()
        ok(f'{flavor} register token {label}', t.register())
        toks[label] = {'id': str(t.id), 'secret': t.secret, 'scopes': scopes, 'api_key': t.get_api_key()}
    out['tokens'][flavor] = toks
    out['users'][flavor] = {
        'admin': {'username': 'trn_admin', 'password': 'AdminPass-12345'},
        'bob': {'username': 'trn_bob', 'password': 'BobPass-12345'},
    }


for f in flavors:
    build_instance(f)

# env-var connector instance: a small pipe
envp = mrsm.Pipe('plugin:trn_plug', 'envreadings', instance='sql:trnenv', parameters={
    'columns': {'datetime': 'ts', 'id': 'sensor'}, 'tags': ['trn'],
})
ok('sync env pipe', envp.sync())
out['pipes'].append({'instance': 'sql:trnenv', 'ck': envp.connector_keys, 'mk': envp.metric_key, 'lk': envp.location_key})
second = mrsm.Pipe('plugin:trn_plug', 'second', instance='sql:trn_second', parameters={
    'columns': {'datetime': 'ts', 'id': 'sensor'}, 'tags': ['trn'],
})
ok('sync second pipe', second.sync())
out['pipes'].append({'instance': 'sql:trn_second', 'ck': second.connector_keys, 'mk': second.metric_key, 'lk': second.location_key})
pathlib.Path(spec['out']).write_text(json.dumps(out, indent=2))
print('[v4] build done')
