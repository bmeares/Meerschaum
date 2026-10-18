"""Continue v4 state with v5 (run under the v5 interpreter). Prints PASS/FAIL lines as JSON-ish records."""
import json, sys, pathlib, traceback
HERE = pathlib.Path(__file__).resolve().parent
sys.path.insert(0, str(HERE))
import meerschaum as mrsm
import data
spec = json.loads(pathlib.Path(sys.argv[1]).read_text())
flavor = spec['flavor']
inst = f"sql:trn_{flavor}"
geometry = flavor == 'postgis'
results = []


def check(name, cond, detail=''):
    results.append({'name': name, 'ok': bool(cond), 'detail': str(detail)[:500]})
    print(('PASS ' if cond else 'FAIL ') + name + ('' if cond else f' :: {str(detail)[:500]}'), flush=True)


def guarded(name):
    def deco(fn):
        try:
            fn()
        except Exception:
            check(name + ' (raised)', False, traceback.format_exc()[-800:])
        return fn
    return deco


assert mrsm.__version__.startswith('5'), mrsm.__version__
P = lambda ck, mk, lk=None: mrsm.Pipe(ck, mk, lk, instance=inst)

@guarded('kitchen')
def _():
    k = P(inst, 'kitchen')
    before_st = k.get_sync_time()
    check('kitchen: parameters unchanged', k.columns == {'datetime': 'dt', 'id': 'id'} and k.parameters['upsert'] is True
          and k.parameters['tags'] == ['trn', 'kitchen'], k.parameters)
    check('kitchen: rowcount 15 from v4', k.get_rowcount() == 15, k.get_rowcount())
    ok, msg = k.sync(data.kitchen_df(range(12, 20), shift=2, geometry=geometry))
    check('kitchen: v5 sync (3 updates + 5 inserts)', ok, msg)
    check('kitchen: rowcount 20, no duplicates', k.get_rowcount() == 20, k.get_rowcount())
    df = k.get_data(params={'id': [12, 13, 14, 19]}, order='asc')
    check('kitchen: updated rows carry the new values', list(df['i']) == [122, 132, 142, 192], list(df['i']))
    check('kitchen: params filter', len(k.get_data(params={'id': 3})) == 1)
    check('kitchen: params negation', len(k.get_data(params={'id': '_3'})) == 19)
    check('kitchen: sync time advanced', k.get_sync_time() > before_st, (before_st, k.get_sync_time()))
    d = k.get_data(begin=data.BASE + __import__('datetime').timedelta(hours=10), end=data.BASE + __import__('datetime').timedelta(hours=13))
    check('kitchen: begin/end window', sorted(d['id'].tolist()) == [10, 11, 12], d['id'].tolist())
    # parameters round-trip
    k.parameters['tags'] = ['trn', 'kitchen', 'edited']
    ok, msg = k.edit()
    check('kitchen: edit()', ok, msg)
    k2 = P(inst, 'kitchen')
    check('kitchen: edit persisted', k2.parameters['tags'] == ['trn', 'kitchen', 'edited'], k2.parameters.get('tags'))
    ok, msg = k2.update_parameters({'trn_extra': {'a': 1}}, persist=True)
    check('kitchen: update_parameters(persist=True)', ok, msg)
    check('kitchen: update persisted', P(inst, 'kitchen').parameters.get('trn_extra') == {'a': 1})

@guarded('auto')
def _():
    a = P(inst, 'auto')
    check('auto: 7 rows from v4', a.get_rowcount() == 7, a.get_rowcount())
    ok, msg = a.sync(data.auto_docs(2, 'c'))
    check('auto: sync', ok, msg)
    df = a.get_data(order='asc')
    pks = sorted(df['pk'].tolist())
    check('auto: autoincrement continues without reuse', pks == list(range(1, 10)), pks)

@guarded('nullidx')
def _():
    n = P(inst, 'nullidx')
    check('nullidx: 9 rows from v4', n.get_rowcount() == 9, n.get_rowcount())
    ok, msg = n.sync(data.nullidx_df(6, 6))
    check('nullidx: sync overlapping rows', ok, msg)
    check('nullidx: 12 unique rows (null ids not duplicated)', n.get_rowcount() == 12, n.get_rowcount())

@guarded('static')
def _():
    s = P(inst, 'static')
    check('static: rows from v4', s.get_rowcount() == 6, s.get_rowcount())
    ok, msg = s.sync(data.static_df(8))
    check('static: sync with 2 new rows', ok, msg)
    check('static: 8 rows', s.get_rowcount() == 8, s.get_rowcount())

@guarded('defn')
def _():
    d = P(inst, 'defn')
    check('defn: parameters unchanged (template kept raw)', '{{ Pipe(' in d.attributes['parameters']['fetch']['definition'] and 'sql_trn' not in d.attributes['parameters']['fetch']['definition'], d.attributes.get('parameters'))
    ok, msg = d.sync()
    check('defn: sync (sql definition, inplace) picks up v5 kitchen rows', ok, msg)
    check('defn: 20 rows', d.get_rowcount() == 20, d.get_rowcount())

@guarded('plugfetch')
def _():
    pf = P('plugin:trn_plug', 'readings')
    check('plugfetch: 5 rows from v4', pf.get_rowcount() == 5, pf.get_rowcount())
    ok, msg = pf.sync()
    check('plugfetch: sync through the v4 plugin (fetch n=10)', ok, msg)
    check('plugfetch: 10 unique rows (no duplicates, nothing lost)', pf.get_rowcount() == 10, pf.get_rowcount())

@guarded('env')
def _():
    e = mrsm.Pipe('plugin:trn_plug', 'envreadings', instance='sql:trnenv')
    check('env connector: rows from v4', e.get_rowcount() == 5, e.get_rowcount())
    ok, msg = e.sync()
    check('env connector: sync', ok and e.get_rowcount() == 10, (msg, e.get_rowcount()))

json.dump(results, open(sys.argv[2], 'w'), indent=1)
print('SUMMARY', sum(r['ok'] for r in results), '/', len(results))
