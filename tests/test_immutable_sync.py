"""Public SDK sync repairs sparse immutable history through a SQL source."""

import pytest


@pytest.mark.parametrize('strategy', ['unbounded-simple', 'unbounded-daily-row-count', 'unbounded-binary-search'])
def test_immutable_sql_source_repairs_late_rows(strategy, tmp_path):
    import uuid
    import meerschaum as mrsm
    conn = mrsm.get_connector('sql', 'late_' + uuid.uuid4().hex[:8], flavor='sqlite',
                              database=str(tmp_path / 'readings.db'))
    assert conn.exec('CREATE TABLE readings (ts TIMESTAMP, id INTEGER, value INTEGER)') is not None
    assert conn.exec("INSERT INTO readings VALUES ('2024-01-10 00:00:00', 1, 1)") is not None
    pipe = mrsm.Pipe(conn, 'history', instance=conn, target='history',
                     columns={'datetime': 'ts', 'id': 'id'},
                     parameters={'sql': 'SELECT * FROM readings',
                                 'sync': {'immutable': True, 'strategy': strategy}})
    assert pipe.sync()[0]
    assert conn.exec("INSERT INTO readings VALUES ('1969-12-31 00:00:00', 2, 2), "
                     "('2024-05-29 00:00:00', 3, 3)") is not None
    for _ in range(2):
        success, message = pipe.sync()
        assert success, message
        assert conn.value('SELECT COUNT(*) FROM history') == 3
        assert conn.value('SELECT SUM(value) FROM history') == 6
