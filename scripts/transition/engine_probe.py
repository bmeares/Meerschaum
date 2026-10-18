"""Where do the `mrsm_engine_*` tables live? (v4 interpreter) `engine_probe.py <flavor>`"""
import sys, meerschaum as mrsm
flavor = sys.argv[1]
conn = mrsm.get_connector(f'sql:trn_{flavor}')
if flavor == 'sqlite':
    df = conn.read("SELECT name FROM sqlite_master WHERE name LIKE 'mrsm_engine%'")
    print('sqlite file tables:', sorted(df['name'].tolist()) if df is not None else None)
elif flavor == 'mssql':
    df = conn.read("SELECT TABLE_SCHEMA AS s, TABLE_NAME AS t FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_NAME LIKE 'mrsm_engine%'")
    print('engine tables in schemas:', sorted(set(df['s'])) if df is not None else None)
else:
    # PostgreSQL schemas / MySQL databases: the server-wide view
    q = "SELECT table_schema AS s FROM information_schema.tables WHERE table_name LIKE 'mrsm_engine%'"
    df = conn.read(q)
    print('engine tables in schemas:', sorted(set(df['s'])) if df is not None else None)
