"""APIConnector client (v4 or v5 library) against a server: `api_client.py <host> <port> <user> <password>`; prints JSON."""
import json, os, sys
import meerschaum as mrsm
from meerschaum.utils.packages import attempt_import
pd = attempt_import('pandas')
host, port, user, pw = sys.argv[1], int(sys.argv[2]), sys.argv[3], sys.argv[4]
out = {}
def step(name, fn):
    try:
        out[name] = fn()
    except Exception as e:
        out[name] = {'EXC': type(e).__name__, 'msg': str(e)[:300]}
conn = mrsm.get_connector('api', 'trnclient', host=host, port=port, protocol='http', username=user, password=pw)
step('login', lambda: bool(conn.token))
p = mrsm.Pipe('sql:trn_' + os.environ.get('TRN_FLAVOR', 'sqlite'), 'kitchen', instance=conn)
step('registered', lambda: p.get_id() is not None if hasattr(p, 'get_id') else p.id is not None)
step('rowcount', lambda: p.get_rowcount())
step('columns', lambda: dict(p.columns))
step('sync_time', lambda: str(p.get_sync_time()))
step('data', lambda: p.get_data(params={'id': [1, 2]}, order='asc')[['id', 'i']].to_dict('records'))
q = mrsm.Pipe('plugin:trn_plug', 'apiclient', instance=conn, columns={'datetime': 'ts', 'id': 'k'})
def _sync():
    ok, msg = q.sync(pd.DataFrame([{'ts': '2031-01-01', 'k': 'a', 'v': 1.5}, {'ts': '2031-01-02', 'k': 'b', 'v': 2.5}]))
    return [bool(ok), q.get_rowcount()]
step('sync_through_api', _sync)
step('delete', lambda: bool(q.delete()[0]))
step('users', lambda: conn.get_users() is not None)
print(json.dumps(out, default=str, sort_keys=True, indent=1))
