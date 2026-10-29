"""A script written against the v4 public API; run unchanged under v4 and v5. Prints one JSON document."""
import json, os, sys
import meerschaum as mrsm
from meerschaum.utils.packages import attempt_import
pd = attempt_import('pandas')
flavor = os.environ.get('TRN_FLAVOR', 'sqlite')
inst = 'sql:trn_' + flavor
out = {}
def step(name, fn):
    try:
        out[name] = fn()
    except Exception as e:  # recorded, not fatal
        out[name] = {'EXC': type(e).__name__, 'msg': str(e)[:200]}
conn = mrsm.get_connector(inst)
step('conn', lambda: [conn.type, conn.label, conn.flavor, str(conn)])
step('pipes', lambda: sorted(str(p) for p in mrsm.get_pipes(instance=inst, as_list=True)))
step('pipes_tags', lambda: sorted(str(p) for p in mrsm.get_pipes(instance=inst, tags=['kitchen'], as_list=True)))
k = mrsm.Pipe(inst, 'kitchen', instance=inst)
step('pipe_keys', lambda: [k.connector_keys, k.metric_key, k.location_key, k.instance_keys, k.target, str(k), k.columns])
step('params_filter', lambda: k.get_data(params={'id': [1, 2, 3]}, order='asc')[['id', 's']].to_dict('records'))
step('rowcount', lambda: k.get_rowcount())
step('sync_time', lambda: str(k.get_sync_time()))
step('registered', lambda: k.id is not None)
step('dtypes_param', lambda: k.dtypes if hasattr(k, 'dtypes') else None)
step('columns_types', lambda: dict(k.get_columns_types()))
step('as_docs', lambda: len(k.get_data(as_docs=True)) if 'as_docs' in k.get_data.__code__.co_varnames else None)
from meerschaum.utils.sql import sql_item_name
_q = sql_item_name(k.target, conn.flavor, getattr(conn, 'schema', None))
step('read', lambda: int(conn.read("SELECT COUNT(*) AS n FROM " + _q).iloc[0, 0]))
step('value', lambda: int(conn.value("SELECT MAX(" + sql_item_name('i', conn.flavor, None) + ") FROM " + _q)))
def _to_sql():
    sq = sql_item_name('trn_scratch', conn.flavor, getattr(conn, 'schema', None))
    conn.exec("DROP TABLE IF EXISTS " + sq)
    ok = conn.to_sql(pd.DataFrame({'a': [1, 2], 'b': ['x', 'y']}), name='trn_scratch', if_exists='replace')
    rows = conn.read("SELECT a, b FROM " + sq + " ORDER BY a").to_dict('records')
    conn.exec("DROP TABLE " + sq)
    return [bool(ok), rows]
step('to_sql', _to_sql)
step('engine', lambda: conn.engine.url.drivername)
def _tables():
    from meerschaum.connectors.sql.tables import get_tables
    t = get_tables(mrsm_instance=conn)
    return sorted(k for k in t if k in ('users', 'plugins', 'pipes', 'tokens'))
step('tables', _tables)
step('config', lambda: [mrsm.get_config('meerschaum', 'connectors', 'sql', 'trn_sqlite', 'flavor'), mrsm.get_config('system', 'trn_marker', warn=False)])
def _entry():
    r = mrsm.entry(['show', 'rowcounts', '-i', inst, '-m', 'kitchen'])
    return [bool(r[0]), isinstance(r, tuple)]
step('entry', _entry)
def _sync_unpack():
    df = pd.DataFrame([{'dt': '2030-01-01', 'v': 1}])
    p = mrsm.Pipe(inst, 'pyapi_tmp', instance=inst, columns={'datetime': 'dt'})
    success, msg = p.sync(df)
    ok2 = p.get_rowcount()
    p.delete()
    return [bool(success), isinstance(msg, str), ok2]
step('sync_unpack', _sync_unpack)
step('user', lambda: [str(conn.get_user_id(mrsm.core.User('trn_bob', instance=inst))) is not None, conn.get_username(conn.get_user_id(mrsm.core.User('trn_bob', instance=inst)))])
step('plugin', lambda: conn.get_plugin_user_id(mrsm.Plugin('trn_plug')) is not None)
print(json.dumps(out, default=str, sort_keys=True, indent=1))
