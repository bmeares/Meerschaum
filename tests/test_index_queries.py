from types import SimpleNamespace

from meerschaum.connectors.sql._pipes import (
    get_create_index_queries,
    get_drop_index_queries,
)


def _pipe(existing_indices=None):
    existing_indices = existing_indices or {}
    columns = {'datetime': 'dt', 'id': 'id'}
    pipe = SimpleNamespace(
        target='upsert_rows',
        parameters={'upsert': True, 'null_indices': False},
        indices=columns,
        columns=columns,
        dtypes={'dt': 'datetime', 'id': 'string'},
        upsert=True,
        exists=lambda debug=False: True,
        get_indices=lambda: {
            'datetime': 'IX_upsert_rows_dt',
            'id': 'IX_upsert_rows_id',
            'unique': 'IX_upsert_rows_id_dt',
        },
        get_columns_types=lambda debug=False: {
            'dt': 'TIMESTAMP WITH TIME ZONE',
            'id': 'TEXT',
        },
        get_columns_indices=lambda debug=False: existing_indices,
    )
    pipe.get_columns = lambda axis, error=False: pipe.columns.get(axis)
    return pipe


def _connector(existing_indices=None):
    return SimpleNamespace(
        flavor='postgresql',
        get_pipe_schema=lambda pipe: 'public',
        get_pipe_columns_indices=lambda pipe, debug=False: existing_indices or {},
    )


def test_postgresql_upsert_creates_one_unique_constraint():
    queries = get_create_index_queries(_connector(), _pipe())
    unique_queries = queries['"IX_upsert_rows_id_dt"']

    assert len(unique_queries) == 1
    assert 'ADD CONSTRAINT "UQ_upsert_rows_id_dt" UNIQUE' in unique_queries[0]
    assert 'CREATE UNIQUE INDEX' not in unique_queries[0]


def test_postgresql_upsert_recognizes_and_drops_constraint_index():
    existing = {
        'dt': [{'name': 'UQ_upsert_rows_id_dt', 'type': 'UNIQUE'}],
        'id': [{'name': 'UQ_upsert_rows_id_dt', 'type': 'UNIQUE'}],
    }
    pipe = _pipe(existing)
    connector = _connector(existing)

    create_queries = get_create_index_queries(connector, pipe)
    assert '"IX_upsert_rows_id_dt"' not in create_queries

    drop_queries = get_drop_index_queries(connector, pipe)
    unique_queries = drop_queries['unique']
    assert unique_queries == [
        'ALTER TABLE "public"."upsert_rows"\n'
        'DROP CONSTRAINT "UQ_upsert_rows_id_dt"'
    ]
