import meerschaum as mrsm
conn = mrsm.get_connector('sql', 'trnpg2')
v = conn.value('SELECT 1')
p = mrsm.Pipe('sql:trnpg2', 'nothing', instance='sql:trnpg2')
n = p.get_rowcount()
print('OK', v, n)
