"""Version-agnostic snapshot of every harness pipe (runs under v4 or v5): `snapshot.py <spec.json> <out.pkl>`."""
import json, pathlib, pickle, sys, traceback
import meerschaum as mrsm

spec = json.loads(pathlib.Path(sys.argv[1]).read_text())
res = {'version': mrsm.__version__, 'file': mrsm.__file__, 'pipes': {}}
for p in spec['pipes']:
    key = f"{p['instance']}|{p['ck']}|{p['mk']}|{p['lk']}"
    d = {}
    try:
        pipe = mrsm.Pipe(p['ck'], p['mk'], p['lk'], instance=p['instance'])
        df = pipe.get_data()
        d['df'] = df
        d['dtypes'] = {c: str(t) for c, t in df.dtypes.items()} if df is not None else None
        d['rowcount'] = pipe.get_rowcount()
        d['sync_time'] = pipe.get_sync_time()
        d['parameters'] = json.loads(json.dumps(pipe.parameters, default=str, sort_keys=True))
        d['columns'] = dict(pipe.columns)
        d['target'] = pipe.target
        d['columns_types'] = dict(pipe.get_columns_types())
        d['exists'] = pipe.exists()
        d['id'] = str(pipe.id)
    except Exception:
        d['error'] = traceback.format_exc()
    res['pipes'][key] = d
pathlib.Path(sys.argv[2]).write_bytes(pickle.dumps(res))
print('snapshot ok', len(res['pipes']), res['version'])
