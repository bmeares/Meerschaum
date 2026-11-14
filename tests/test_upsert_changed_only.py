"""Guarded upserts retain Python row counts, zero-write success and nullable keys."""

from datetime import datetime, timezone
import uuid

import pytest
import meerschaum as mrsm

from tests.connectors import conns, get_flavors


@pytest.mark.parametrize('flavor', [flavor for flavor in get_flavors() if flavor in conns])
@pytest.mark.parametrize('key_only', [False, True])
def test_guarded_upsert_counts_physical_writes(flavor, key_only):
    if flavor not in ('postgresql', 'postgis', 'timescaledb', 'timescaledb_ha'):
        pytest.skip('PostgreSQL-family upsert guard')
    pipe = mrsm.Pipe(
        'test', 'guarded_upsert', uuid.uuid4().hex[:12], instance=conns[flavor],
        columns={'datetime': 'ts', 'id': 'id'},
        parameters={'upsert': True, 'upsert_changed_only': True, 'hypertable': False},
    )
    timestamp = datetime(2026, 1, 1, tzinfo=timezone.utc)
    rows = [{'ts': timestamp, 'id': identifier} for identifier in (1, None)]
    if not key_only:
        for row in rows:
            row['value'] = 5
    try:
        first = pipe.sync(rows, check_existing=False)
        assert first[0], first[1]
        assert first.upserted == 2
        repeated = pipe.sync(rows, check_existing=False)
        assert repeated[0], repeated[1]
        assert repeated.upserted == 0
        assert repeated[1] == 'Upserted 0 rows.'
        if not key_only:
            rows[1]['value'] = 6
            changed = pipe.sync(rows, check_existing=False)
            assert changed[0], changed[1]
            assert changed.upserted == 1
        assert pipe.get_rowcount() == 2
    finally:
        pipe.delete()
