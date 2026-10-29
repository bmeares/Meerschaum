"""Compare two snapshots (pickles from snapshot.py): `compare.py A.pkl B.pkl [--ignore-keys k,...]`.
Run with the v5 interpreter (pandas comes from the shared mrsm venv)."""
import pickle, sys, json
from meerschaum.utils.packages import attempt_import
pd = attempt_import('pandas', lazy=False)


EXPECTED_DTYPES_REF = 'PARITY W17D2-006'


def _arrow_variant(a, b):
    """v4.1.4 returns NumPy-backed dtypes when polars is absent; v5 always returns the polars-path dtypes
    (Arrow-backed, nanosecond-capable): `float64` -> `double[pyarrow]`, `string` -> `large_string[pyarrow]`,
    `datetime64[us, UTC]` -> `timestamp[us, tz=UTC][pyarrow]` or `datetime64[ns, UTC]`. Decided for rc1."""
    if b.endswith('[pyarrow]') and not a.endswith('[pyarrow]'):
        return True
    return a.startswith('datetime64[us,') and b == a.replace('[us,', '[ns,')


def split_dtype_diff(da, db):
    """(expected column differences, other column differences) between two dtype maps."""
    exp, other = {}, {}
    for c in sorted(set(da) | set(db)):
        x, y = da.get(c), db.get(c)
        if x == y:
            continue
        (exp if (x and y and _arrow_variant(x, y)) else other)[c] = (x, y)
    return exp, other


def cmp_snaps(a, b, ignore=()):
    diffs = []
    for key in sorted(set(a['pipes']) | set(b['pipes'])):
        pa, pb = a['pipes'].get(key), b['pipes'].get(key)
        if pa is None or pb is None:
            diffs.append((key, 'missing', pa is None, pb is None)); continue
        if 'error' in pa or 'error' in pb:
            if pa.get('error') != pb.get('error'):
                diffs.append((key, 'error', (pa.get('error') or '')[-300:], (pb.get('error') or '')[-300:]))
            continue
        for f in ('rowcount', 'sync_time', 'columns', 'target', 'exists', 'columns_types', 'dtypes', 'parameters'):
            if f in ignore:
                continue
            if pa[f] != pb[f]:
                if f == 'dtypes' and pa[f] and pb[f]:
                    exp, other = split_dtype_diff(pa[f], pb[f])
                    if exp:
                        diffs.append((key, 'EXPECTED-dtypes ' + EXPECTED_DTYPES_REF, {k: v[0] for k, v in exp.items()}, {k: v[1] for k, v in exp.items()}))
                    if other:
                        diffs.append((key, f, {k: v[0] for k, v in other.items()}, {k: v[1] for k, v in other.items()}))
                    continue
                diffs.append((key, f, pa[f], pb[f]))
        da, db = pa['df'], pb['df']
        if da is None or db is None:
            if (da is None) != (db is None):
                diffs.append((key, 'df-none', da is None, db is None))
            continue
        if list(da.columns) != list(db.columns):
            diffs.append((key, 'columns-order', list(da.columns), list(db.columns))); continue
        try:
            sa = da.sort_values(list(da.columns[:2]), kind='stable').reset_index(drop=True) if len(da) else da
            sb = db.sort_values(list(db.columns[:2]), kind='stable').reset_index(drop=True) if len(db) else db
            if not sa.astype(str).equals(sb.astype(str)):
                diffs.append((key, 'values(as str)', sa.head(3).to_dict('records'), sb.head(3).to_dict('records')))
        except Exception as e:
            diffs.append((key, 'cmp-error', repr(e), ''))
    return diffs


if __name__ == '__main__':
    a = pickle.load(open(sys.argv[1], 'rb')); b = pickle.load(open(sys.argv[2], 'rb'))
    ign = tuple(sys.argv[4].split(',')) if len(sys.argv) > 4 and sys.argv[3] == '--ignore' else ()
    print('A:', a['version'], a['file']); print('B:', b['version'], b['file'])
    ds = cmp_snaps(a, b, ign)
    for d in ds:
        print('EXPECTED' if d[1].startswith('EXPECTED') else 'DIFF', d[0], d[1]); print('   A:', str(d[2])[:600]); print('   B:', str(d[3])[:600])
    print('diffs:', len([d for d in ds if not d[1].startswith('EXPECTED')]), 'expected:', len([d for d in ds if d[1].startswith('EXPECTED')]))
