"""
In-place syncs on the PostgreSQL family through `Pipe.sync()`: enum and other extension-typed
columns without PostGIS, columns v4's NULL stand-in (`-987654321`) cannot fill (an enum, an
interval, a range), and a `primary`-keyed upsert into a partitioned target (a TimescaleDB
hypertable, a natively partitioned table), whose key also holds the datetime column.
"""

from __future__ import annotations

import uuid

import pytest

from meerschaum import Pipe
from tests.connectors import conns, get_flavors

@pytest.fixture
def pg_conn(flavor):
    """A PostgreSQL-family instance and a source table prefix of this test's own."""
    if flavor not in ('timescaledb', 'timescaledb-ha', 'timescaledb_ha', 'postgis', 'postgresql'):
        pytest.skip('PostgreSQL-family in-place sync')
    conn = conns[flavor]
    prefix = f"_mrsm_ipt_{uuid.uuid4().hex[:8]}"
    yield conn, prefix
    conn.exec(f'DROP TABLE IF EXISTS "{prefix}_src"', silent=True)
    conn.exec(f'DROP TYPE IF EXISTS "{prefix}_mood" CASCADE', silent=True)


def _source(conn, prefix: str) -> str:
    source = f'"{prefix}_src"'
    assert conn.exec(f'CREATE TYPE "{prefix}_mood" AS ENUM (\'happy\', \'sad\')')
    assert conn.exec(
        f"CREATE TABLE {source} (ts TIMESTAMPTZ NOT NULL, id BIGINT NOT NULL, "
        f"m \"{prefix}_mood\", iv INTERVAL, r INT4RANGE, v INTEGER)"
    )
    assert conn.exec(
        f"INSERT INTO {source} SELECT '2024-01-01'::timestamptz + (i || ' days')::interval, i, "
        f"'happy', interval '1 day', int4range(i, 10), i FROM generate_series(1, 3) AS i"
    )
    return source


def _pipe(conn, prefix: str, name: str, source: str, **parameters) -> Pipe:
    location = prefix.lstrip('_')
    pipe = Pipe(str(conn), name, location, instance=conn)
    pipe.delete()
    return Pipe(
        str(conn), name, location,
        instance=conn,
        parameters={
            'fetch': {'definition': f"SELECT ts, id, m, iv, r, v FROM {source}"},
            'verify': {'chunk_minutes': 1440},
            **parameters,
        },
    )


@pytest.mark.parametrize('flavor', [flavor for flavor in get_flavors() if flavor in conns])
@pytest.mark.parametrize('columns, upsert', [
    ({'datetime': 'ts', 'id': 'id'}, False),
    ({'datetime': 'ts', 'id': 'id'}, True),
    ({'datetime': 'ts', 'primary': 'id'}, True),
    ({'datetime': 'ts', 'primary': 'id'}, False),
])
def test_inplace_syncs_of_special_types_and_partitioned_targets(pg_conn, columns, upsert):
    conn, prefix = pg_conn
    source = _source(conn, prefix)
    pipe = _pipe(conn, prefix, f"ipt_{'_'.join(columns)}_{int(upsert)}", source, columns=columns, upsert=upsert)
    try:
        for _ in range(2):
            success, message = pipe.sync()
            assert success, message
        conn.exec(f"UPDATE {source} SET m = 'sad', iv = NULL, r = NULL WHERE id = 2")
        conn.exec(
            f"INSERT INTO {source} VALUES ('2024-03-01', 4, NULL, NULL, NULL, 4)"
        )
        success, message = pipe.sync()
        assert success, message
        rows = conn.read(
            f"SELECT id, m::text AS m, iv::text AS iv, r::text AS r FROM \"{pipe.target}\" ORDER BY id"
        )
        assert rows['id'].tolist() == [1, 2, 3, 4]
        assert rows['m'].tolist()[:2] == ['happy', 'sad']
        assert rows['iv'].isna().tolist() == [False, True, False, True]
        assert rows['r'].isna().tolist() == [False, True, False, True]
    finally:
        pipe.delete()
