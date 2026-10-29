"""
Wave 9 (task U): the instance façade reuses one connection pool per connector.

Repeated reads (`exists`, `get_rowcount`, `get_sync_time`, `get_data`, `get_columns_types`)
on one pipe, and on fresh `Pipe` objects with the same keys, must not open new database
connections once the pools are warm. The connector's URI carries a unique `application_name`, so
`pg_stat_activity` lists exactly this test's backends (SQLAlchemy's and
the Rust pool's). A pool rebuilt per call (or per `Pipe`) shows up as new backend PIDs.

Runs against PostgreSQL (the `postgis` test container) when `postgis` is in `MRSM_TEST_FLAVORS`.
Schema `w9u`.
"""

from tests._ports import service_port

import os
import uuid
from datetime import datetime, timedelta, timezone

import pytest

import meerschaum as mrsm
from meerschaum import _core

PG_BASE_URI = f"postgresql://test:test1234@localhost:{service_port('postgis')}/testdb"
SCHEMA = 'w9u'

pytestmark = pytest.mark.skipif(
    'postgis' not in [
        f.strip() for f in os.environ.get('MRSM_TEST_FLAVORS', '').split(',')
    ],
    reason="needs the postgis test container (MRSM_TEST_FLAVORS=postgis)",
)


def _backend_pids(observer, app_name: str) -> set:
    df = observer.read(
        "SELECT pid FROM pg_stat_activity "
        f"WHERE application_name = '{app_name}' AND datname = current_database()"
    )
    return set(int(pid) for pid in df['pid']) if df is not None else set()


def test_repeated_reads_open_no_new_connections():
    app_name = f"w9u_pool_{uuid.uuid4().hex[:10]}"
    label = f"w9u_pool_{uuid.uuid4().hex[:6]}"
    conn = mrsm.get_connector(
        'sql', label,
        uri=f"{PG_BASE_URI}?application_name={app_name}",
        schema=SCHEMA,
    )
    observer = mrsm.get_connector(
        'sql', label + '_obs', uri=PG_BASE_URI, schema=SCHEMA,
    )
    observer.exec(f'CREATE SCHEMA IF NOT EXISTS "{SCHEMA}"')
    pipe_keys = ('w9u', 'pool', label)
    pipe = mrsm.Pipe(*pipe_keys, instance=conn, columns={'datetime': 'ts', 'id': 'id'})
    pipe.delete()
    pipe = mrsm.Pipe(*pipe_keys, instance=conn, columns={'datetime': 'ts', 'id': 'id'})
    base = datetime(2024, 1, 1, tzinfo=timezone.utc)
    success, msg = pipe.sync([
        {'ts': base + timedelta(minutes=i), 'id': i % 3, 'val': float(i)}
        for i in range(12)
    ])
    assert success, msg

    def read_round(p):
        assert p.exists()
        assert p.get_rowcount() == 12
        assert p.get_sync_time() == base + timedelta(minutes=11)
        assert len(p.get_data()) == 12
        assert p.get_columns_types()

    try:
        ### Finish setup's background status/metrics writes and their cold initialization
        ### before measuring connections opened by reads.
        _core.flush_sync_statuses(timeout=10.0)
        ### Warm up every pool this connector uses.
        for _ in range(3):
            read_round(pipe)
            read_round(mrsm.Pipe(*pipe_keys, instance=conn))
        warm_pids = _backend_pids(observer, app_name)
        assert warm_pids, f"no backends found for application_name '{app_name}'"

        for _ in range(15):
            read_round(pipe)
            read_round(mrsm.Pipe(*pipe_keys, instance=conn))
        after_pids = _backend_pids(observer, app_name)

        assert after_pids - warm_pids == set(), (
            f"new connections were opened by repeated reads: "
            f"warm={sorted(warm_pids)}, after={sorted(after_pids)}"
        )
        ### One SQLAlchemy pool and one Rust pool, used sequentially.
        assert len(after_pids) <= 4, sorted(after_pids)
    finally:
        pipe.delete()
