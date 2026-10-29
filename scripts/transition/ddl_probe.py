"""Print `yes` / `no` for whether a relation exists (v4 interpreter): `ddl_probe.py <flavor> <quoted relation>`."""
import sys, meerschaum as mrsm
flavor, rel = sys.argv[1], sys.argv[2]
conn = mrsm.get_connector(f'sql:trn_{flavor}')
try:
    df = conn.read(f'SELECT COUNT(*) AS n FROM {rel}', silent=True)
    print('yes' if df is not None else 'no')
except Exception:
    print('no')
