"""The `mrsm_*` tables in schema `public` of the postgresql (5559) test container, sorted (v4 interpreter)."""
import meerschaum as mrsm
conn = mrsm.get_connector('sql', 'trn_probe', flavor='postgresql', username='test', password='test1234', database='testdb',
                          host='localhost', port=5559)
df = conn.read("SELECT table_name FROM information_schema.tables WHERE table_schema = 'public' AND table_name LIKE 'mrsm%' ORDER BY 1")
print(','.join(df['table_name'].tolist()) if df is not None else 'ERR')
