"""A SQLite scalar write returns a value only after its commit succeeds."""

import sqlite3
import subprocess
import sys
import uuid

import meerschaum as mrsm

READER = '''
import sqlite3, sys, time
reader = sqlite3.connect(sys.argv[1], isolation_level=None)
reader.execute('BEGIN')
reader.execute('SELECT COUNT(*) FROM ids').fetchall()
print('held', flush=True)
time.sleep(float(sys.argv[2]))
reader.execute('ROLLBACK')
'''

INSERT = 'INSERT INTO ids (id) SELECT COALESCE(MAX(id), 0) + 1 FROM ids RETURNING id'


def _stored(database):
    with sqlite3.connect(database) as check:
        return [row[0] for row in check.execute('SELECT id FROM ids ORDER BY id')]


def test_value_of_a_sqlite_write_with_returning_is_stored_or_none(tmp_path):
    database = tmp_path / 'returning.db'
    label = 'returning_' + uuid.uuid4().hex[:8]
    conn = mrsm.get_connector('sql', label, flavor='sqlite', database=str(database))
    conn.exec('CREATE TABLE ids (id INTEGER PRIMARY KEY)')

    ### Another process holds a shared lock for longer than the busy timeout.
    reader = subprocess.Popen(
        [sys.executable, '-c', READER, str(database), '7'],
        stdout=subprocess.PIPE, text=True,
    )
    try:
        assert reader.stdout.readline().strip() == 'held'
        refused = conn.value(INSERT, silent=True)
    finally:
        assert reader.wait(timeout=60) == 0
    assert ([] if refused is None else [refused]) == _stored(database)

    ### Without contention the write commits and its value is returned.
    before = _stored(database)
    inserted = conn.value(INSERT)
    assert _stored(database) == before + [inserted]
    assert conn.value('SELECT COUNT(*) FROM ids') == len(before) + 1


def test_sqlite_returning_scalar_finishes_all_rows_and_preserves_rollback(tmp_path):
    conn = mrsm.get_connector(
        'sql', 'returning_' + uuid.uuid4().hex[:8],
        flavor='sqlite', database=str(tmp_path / 'many.db'),
    )
    conn.exec('CREATE TABLE items (id INTEGER PRIMARY KEY, score INTEGER)')
    conn.exec('INSERT INTO items VALUES (1, 0), (2, 0), (3, 0)')
    first = conn.value(
        'UPDATE items SET score = score + :step RETURNING id',
        params={'step': 2},
    )
    assert first in (1, 2, 3)
    assert conn.value('SELECT COUNT(*) FROM items WHERE score = 2') == 3
    rolled_back = conn.value('DELETE FROM items RETURNING id', commit=False)
    assert rolled_back in (1, 2, 3)
    assert conn.value('SELECT COUNT(*) FROM items') == 3
