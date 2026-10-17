"""
PostgreSQL-family upsert pipes keep one `UQ_` unique constraint (never an extra `IX_` unique
index): `SQLConnector.get_create_index_queries` and `get_drop_index_queries` (built in Rust)
against a real table.
"""

from datetime import datetime, timezone

import pytest

import meerschaum as mrsm
from tests.connectors import conns, get_flavors

pytestmark = pytest.mark.skipif(
    'postgis' not in get_flavors(),
    reason="set MRSM_TEST_FLAVORS to include postgis",
)


@pytest.fixture
def upsert_pipe():
    conn = conns['postgis']
    columns = {'datetime': 'dt', 'id': 'id'}
    kwargs = dict(
        instance=conn,
        columns=columns,
        upsert=True,
        parameters={'null_indices': False},
        dtypes={'dt': 'datetime', 'id': 'string'},
    )
    pipe = mrsm.Pipe('w16idx', 'upsert_rows', **kwargs)
    pipe.delete()
    pipe.drop()
    pipe = mrsm.Pipe('w16idx', 'upsert_rows', **kwargs)
    success, msg = pipe.sync([{'dt': datetime(2024, 1, 1, tzinfo=timezone.utc), 'id': 'a'}])
    assert success, msg
    yield pipe
    pipe.delete()
    pipe.drop()


def _constraint_sql(conn, pipe) -> str:
    return f'ALTER TABLE "public"."{pipe.target}" DROP CONSTRAINT "UQ_{pipe.target}_dt_id"'


def test_postgresql_upsert_creates_one_unique_constraint(upsert_pipe):
    conn = upsert_pipe.instance_connector
    conn.exec(_constraint_sql(conn, upsert_pipe), silent=True)
    upsert_pipe._invalidate_cache(hard=True)

    queries = conn.get_create_index_queries(upsert_pipe)
    unique_queries = queries[f'"IX_{upsert_pipe.target}_dt_id"']

    assert len(unique_queries) == 1
    assert f'ADD CONSTRAINT "UQ_{upsert_pipe.target}_dt_id" UNIQUE' in unique_queries[0]
    assert 'CREATE UNIQUE INDEX' not in unique_queries[0]


def test_postgresql_upsert_recognizes_and_drops_constraint_index(upsert_pipe):
    conn = upsert_pipe.instance_connector

    create_queries = conn.get_create_index_queries(upsert_pipe)
    assert f'"IX_{upsert_pipe.target}_dt_id"' not in create_queries

    drop_queries = conn.get_drop_index_queries(upsert_pipe)
    assert drop_queries['unique'] == [
        f'ALTER TABLE "public"."{upsert_pipe.target}"\n'
        f'DROP CONSTRAINT "UQ_{upsert_pipe.target}_dt_id"'
    ]
