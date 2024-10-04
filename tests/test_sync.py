#! /usr/bin/env python
# -*- coding: utf-8 -*-
# vim:fenc=utf-8

import pytest
import sys
from datetime import datetime, timedelta
from decimal import Decimal
from uuid import UUID
from tests import debug
from tests.pipes import all_pipes, stress_pipes, remote_pipes
from tests.connectors import conns, get_flavors
from tests.test_users import test_register_user
import meerschaum as mrsm
from meerschaum import Pipe
from meerschaum.actions import actions

@pytest.fixture(autouse=True)
def run_before_and_after(flavor: str):
    """
    Ensure the test user is registered before running tests.
    """
    test_register_user(flavor)
    yield


@pytest.mark.parametrize("flavor", get_flavors())
def test_register_and_delete(flavor: str):
    """
    Verify user registration and deletion.
    """
    pipe = all_pipes[flavor][0]
    params = pipe.parameters.copy()
    assert params is not None
    output = pipe.delete()
    pipe.parameters = params
    assert pipe.parameters is not None
    success, msg = pipe.register(debug=debug)
    assert success, msg
    assert pipe.get_id(debug=debug) is not None
    success, msg = pipe.delete(debug=debug)
    assert success, msg
    pipe.parameters = params
    success, msg = pipe.register(debug=debug)
    assert success, msg
    assert pipe.parameters is not None

@pytest.mark.parametrize("flavor", get_flavors())
def test_drop_and_sync(flavor: str):
    """
    Verify dropping and resyncing pipes.
    """
    pipe = all_pipes[flavor][0]
    pipe.drop()
    assert pipe.exists(debug=debug) is False
    assert pipe.columns is not None
    now1 = datetime(2021, 1, 1, 12, 0)
    data = {'datetime' : [now1], 'id' : [1], 'val': [1]}
    success, msg = pipe.sync(data, debug=debug)
    assert success, msg
    assert pipe.exists(debug=debug)
    now2 = datetime(2021, 1, 1, 12, 1)
    data = {'datetime' : [now2], 'id' : [1], 'val': [1]}
    success, msg = pipe.sync(data, debug=debug)
    assert success, msg
    assert pipe.exists(debug=debug)
    data = pipe.get_data(debug=debug)
    assert data is not None
    assert len(data) == 2

@pytest.mark.parametrize("flavor", get_flavors())
def test_drop_and_sync_duplicate(flavor: str):
    """
    Verify dropping a table and syncing duplicate rows are filtered out.
    """
    pipe = all_pipes[flavor][0]
    pipe.drop(debug=debug)
    assert not pipe.exists(debug=debug)

    now1 = datetime(2021, 1, 1, 12, 0)
    data = {'datetime': [now1], 'id': [1], 'val': [1]}
    success, msg = pipe.sync(data, debug=debug)
    assert success, msg
    data = pipe.get_data(debug=debug)
    assert len(data) == 1

    now1 = datetime(2021, 1, 1, 12, 0)
    data = {'datetime': [now1], 'id': [1], 'val': [1]}
    success, msg = pipe.sync(data, debug=debug)
    assert success, msg
    data = pipe.get_data(debug=debug)
    assert len(data) == 1

@pytest.mark.parametrize("flavor", get_flavors())
def test_drop_and_sync_stress(flavor: str):
    pipe = stress_pipes[flavor]
    pipe.drop(debug=debug)
    success, msg = pipe.sync(debug=debug)
    assert success, msg

@pytest.mark.parametrize("flavor", get_flavors())
def test_drop_and_sync_remote(flavor: str):
    pipe = None
    for p in remote_pipes[flavor]:
        if str(p.connector) == str(p.instance_connector):
            pipe = p
            break
    if pipe is None:
        return
    pipe.delete(debug=debug)
    parent_pipe = Pipe('plugin:stress', 'test', instance=pipe.connector)
    parent_pipe.delete(debug=debug)
    begin, end = datetime(2020, 1, 1), datetime(2020, 1, 2)
    success, msg = parent_pipe.sync(begin=begin, end=end, debug=debug)
    parent_len = parent_pipe.get_rowcount(debug=debug)
    assert success, msg

    success, msg = pipe.sync(debug=debug)
    assert success, msg
    child_len = pipe.get_rowcount(debug=debug)
    assert parent_len == child_len

    success, msg = parent_pipe.sync(
        [{'datetime': '2020-01-03', 'id': -1, 'foo': 'a'}],
        debug=debug,
    )
    assert success, msg
    parent_len2 = parent_pipe.get_rowcount(debug=debug)
    assert parent_len2 == (parent_len + 1)
    success, msg = pipe.sync(debug=debug)
    assert len(pipe.get_columns_types(debug=debug)) == 4
    child_len2 = pipe.get_rowcount(debug=debug)
    assert parent_len2 == child_len2
    success, msg = parent_pipe.sync(
        [{'datetime': '2020-01-03', 'id': -1, 'foo': 'b'}],
        debug=debug,
    )
    assert success, msg
    parent_len3 = parent_pipe.get_rowcount(debug=debug)
    assert parent_len2 == parent_len3
    success, msg = pipe.sync(debug=debug, begin='2020-01-01')
    assert success, msg
    child_len3 = pipe.get_rowcount(debug=debug)
    assert child_len3 == parent_len3
    df = pipe.get_data(params={'id': -1}, debug=debug)
    assert len(df) == 1
    assert df.to_dict(orient='records')[0]['foo'] == 'b'


@pytest.mark.parametrize("flavor", get_flavors())
def test_sync_engine(flavor: str):
    ### Weird concurrency issues with our tests.
    if flavor == 'duckdb':
        return
    pipe = stress_pipes[flavor]
    _ = pipe.register()
    mrsm_instance = str(pipe.instance_connector)
    _ = actions['drop'](
        ['pipes'],
        connector_keys=[pipe.connector_keys],
        metric_keys=[pipe.metric_key],
        location_keys=[pipe.location_key],
        mrsm_instance=mrsm_instance,
        yes=True,
    )

    success, msg = actions['sync'](
        ['pipes'],
        connector_keys=[pipe.connector_keys],
        metric_keys=[pipe.metric_key],
        location_keys=[pipe.location_key],
        mrsm_instance=mrsm_instance,
    )
    assert success, msg


@pytest.mark.parametrize("flavor", get_flavors())
def test_target_mutable(flavor: str):
    conn = conns[flavor]
    if conn.type != 'sql':
        return
    target = 'FooBar!'
    pipe = Pipe('foo', 'bar', target=target, instance=conn, columns={'datetime': 'dt', 'id': 'id'})
    pipe.drop(debug=debug)
    assert not pipe.exists(debug=debug)
    success, msg = pipe.sync(
        {'dt': [datetime(2022, 6, 8)], 'id': [1], 'vl': [10]},
        debug=debug,
    )
    df = conn.read(target)
    assert len(df) == 1
    success, msg = pipe.sync(
        {'dt': [datetime(2022, 6, 8)], 'id': [1], 'vl': [10]},
        debug=debug,
    )
    df = conn.read(target)
    assert len(df) == 1
    success, msg = pipe.sync(
        {'dt': [datetime(2022, 6, 8)], 'id': [1], 'vl': [100]},
        debug=debug,
    )
    df = conn.read(target)
    assert len(df) == 1
    pipe.drop()
    result = conn.read(target, silent=True)
    assert result is None


@pytest.mark.parametrize("flavor", get_flavors())
def test_sync_new_columns(flavor: str):
    """
    Test that new columns are added.
    """
    conn = conns[flavor]
    pipe = Pipe('new', 'cols', columns={'datetime': 'dt', 'id': 'id'}, instance=conn)
    pipe.delete(debug=debug)
    pipe = Pipe('new', 'cols', columns={'datetime': 'dt', 'id': 'id'}, instance=conn)
    docs = [
        {'dt': '2022-01-01', 'id': 1, 'a': 10},
    ]
    pipe.sync(docs, debug=debug)
    assert len(pipe.get_data().columns) == 3

    docs = [
        {'dt': '2022-01-01', 'id': 1, 'b': 20},
    ]
    pipe.sync(docs, debug=debug)
    df = pipe.get_data()
    assert len(df.columns) == 4
    assert len(df) == 1


@pytest.mark.parametrize("flavor", get_flavors())
def test_temporary_pipes(flavor: str):
    """
    Verify that `temporary=True` will not create instance tables.
    """
    from meerschaum.utils.misc import generate_password
    from meerschaum.utils.sql import table_exists
    if flavor != 'sqlite':
        return
    session_id = generate_password(6)
    db_path = '/tmp/' + session_id + '.db'
    conn = mrsm.get_connector('sql', session_id, flavor='sqlite', database=db_path) 
    pipe = Pipe('foo', 'bar', instance=conn, temporary=True, columns={'id': 'id'})
    _ = pipe.parameters
    _ = pipe.id
    _ = pipe.get_rowcount(debug=debug)
    success, msg = pipe.sync([{'id': 1, 'a': 2}], debug=debug)
    assert success, msg
    success, msg = pipe.sync([{'id': 2, 'b': 3}], debug=debug)
    assert success, msg
    success, msg = pipe.sync(
        [
            {'id': 1, 'b': 4},
            {'id': 2, 'a': 5},
        ],
        debug = debug,
    )
    success, msg = pipe.delete(debug=debug)
    assert (not success), msg
    assert pipe.get_rowcount(debug=debug) == 2
    assert not table_exists('pipes', conn, debug=debug)
    assert not table_exists('users', conn, debug=debug)
    assert not table_exists('plugins', conn, debug=debug)


@pytest.mark.parametrize("flavor", get_flavors())
def test_id_index_col(flavor: str):
    """
    Verify that the ID column is able to be synced.
    """
    conn = conns[flavor]
    pipe = Pipe(
        'test_id', 'index_col', 'table',
        instance=conn,
        dtypes={'id': 'Int64'},
        columns={'datetime': 'id'},
    )
    pipe.delete()
    docs = [{'id': i, 'a': i*2, 'b': {'c': i/2}} for i in range(100)]
    success, msg = pipe.sync(docs, debug=debug)
    assert success, msg
    df = pipe.get_data(debug=debug)
    synced_docs = df.to_dict(orient='records')
    assert len(df) == len(docs)
    assert synced_docs == docs

    ### Sync the same docs again and verify nothing has changed.
    success, msg = pipe.sync(docs, debug=debug)
    assert success, msg
    df = pipe.get_data(debug=debug)
    synced_docs = df.to_dict(orient='records')
    assert synced_docs == docs

    ### Update the first 10 docs.
    new_docs = [{'id': i, 'a': i*3, 'b': {'c': round(i/3, 2)}} for i in range(10)]
    success, msg = pipe.sync(new_docs, debug=debug)
    small_df = pipe.get_data(end=10, debug=debug)
    assert len(small_df) == len(new_docs)
    small_synced_docs = small_df.to_dict(orient='records')
    assert small_synced_docs == new_docs


@pytest.mark.parametrize("flavor", get_flavors())
def test_utc_offset_datetimes(flavor: str):
    """
    Verify that we are able to sync rows with UTC offset datetimes.
    """
    conn = conns[flavor]
    pipe = Pipe(
        'test_utc_offset', 'datetimes',
        instance=conn,
        columns={'datetime': 'dt'},
    )
    pipe.delete()

    docs = [
        {'dt': '2023-01-01 00:00:00+00:00'},
        {'dt': '2023-01-02 00:00:00+01:00'},
    ]

    expected_docs = [
        {'dt': datetime(2023, 1, 1)},
        {'dt': datetime(2023, 1, 1, 23, 0, 0)}
    ]

    success, msg = pipe.sync(docs, debug=debug)
    assert success, msg
    df = pipe.get_data(debug=debug)
    synced_docs = df.to_dict(orient='records')
    assert synced_docs == expected_docs


@pytest.mark.parametrize("flavor", get_flavors())
def test_no_indices_inferred_datetime_to_text(flavor: str):
    """
    Verify that changing dtypes are handled.
    """
    conn = conns[flavor]
    pipe = Pipe(
        'test_no_indices', 'datetimes', 'text',
        instance=conn,
    )
    pipe.delete()
    docs = [
        {'fake-dt': '2023-01-01', 'a': 1},
    ]
    success, msg = pipe.sync(docs, debug=debug)
    assert success, msg

    docs = [
        {'fake-dt': '2023-01-01', 'a': 1},
        {'fake-dt': '2023-01-02', 'a': 2},
    ]
    success, msg = pipe.sync(docs, debug=debug)
    assert success, msg
    df = pipe.get_data()
    assert len(df) == len(docs)

    docs = [
        {'fake-dt': '2023-01-01', 'a': 1},
        {'fake-dt': '2023-01-02', 'a': 2},
        {'fake-dt': 'foo', 'a': 3},
    ]
    success, msg = pipe.sync(docs, debug=debug)
    assert success, msg
    df = pipe.get_data()
    assert len(df) == len(docs)


@pytest.mark.parametrize("flavor", get_flavors())
def test_sync_generators(flavor: str):
    """
    Verify that we are able to sync generators of chunks.
    """
    conn = conns[flavor]
    pipe = Pipe(
        'test_generators', 'foo',
        instance=conn,
        columns={'datetime': 'dt'},
    )
    pipe.delete()
    start_time = datetime(2023, 1, 1)
    num_docs = 3
    generator = ([{'dt': start_time + timedelta(days=i)}] for i in range(num_docs))
    success, msg = pipe.sync(generator, debug=debug)
    assert success, msg
    rowcount = pipe.get_rowcount()
    assert rowcount == num_docs


@pytest.mark.parametrize("flavor", get_flavors())
def test_add_new_columns(flavor: str):
    """
    Verify that we are able to add new columns dynamically.
    """
    conn = conns[flavor]
    pipe = Pipe('test_add', 'columns', instance=conn)
    pipe.delete()
    pipe = Pipe(
        'test_add', 'columns',
        instance=conn,
        columns={'datetime': 'dt'},
    )

    docs = [{'dt': '2023-01-01', 'a': 1}]
    success, msg = pipe.sync(docs, debug=debug)
    assert success, msg
    docs = [{'dt': '2023-01-01', 'b': 'foo', 'c': 12.3, 'd': {'e': 5}}]
    success, msg = pipe.sync(docs, debug=debug)
    assert success, msg
    assert len(pipe.dtypes) == 5
    df = pipe.get_data()
    assert len(df) == 1
    assert 'dt' in df.columns
    assert 'a' in df.columns
    assert 'b' in df.columns
    assert 'c' in df.columns
    assert 'd' in df.columns
    assert 'e' in df['d'][0]


@pytest.mark.parametrize("flavor", get_flavors())
def test_get_data_iterator(flavor: str):
    """
    Test the new `as_iterator` flag in `Pipe.get_data()`.
    """
    from meerschaum.utils.packages import import_pandas
    pd = import_pandas()
    conn = conns[flavor]
    pipe = mrsm.Pipe('test', 'get_data_iterator', 'foo', instance=conn)
    pipe.delete()
    pipe = mrsm.Pipe(
        'test', 'get_data_iterator', 'foo',
        instance=conn,
        columns={'datetime': 'id'},
        dtypes={'id': 'Int64'},
    )
    docs = [{'id': i, 'color': ('a' if i % 2 == 0 else 'b')} for i in range(7)]
    success, message = pipe.sync(docs, debug=debug)
    assert success, message
    gen = pipe.get_data(
        begin=1,
        end=6,
        as_iterator=True,
        chunk_interval=2,
        debug=debug,
    )
    chunks = [chunk for chunk in gen]
    assert len(chunks) == 3
    assert len(chunks[2]) == 1

    gen = pipe.get_data(
        params={'color': 'a'},
        as_iterator=True,
        chunk_interval=3,
        debug=debug,
    )
    chunks = [chunk for chunk in gen]
    print(chunks)
    for c in chunks:
        print(c)
    df = pd.concat(chunks)
    assert list(df['id']) == [0, 2, 4, 6]


@pytest.mark.parametrize("flavor", get_flavors())
def test_sync_inplace(flavor: str):
    """
    Verify that in-place syncing works as expected.
    """
    from meerschaum.utils.sql import sql_item_name
    conn = conns[flavor]
    if conn.type != 'sql':
        return
    source_pipe = Pipe('test', 'inplace', 'src', instance=conn)
    source_pipe.delete()
    source_pipe = Pipe(
        'test', 'inplace', 'src',
        instance=conn,
        columns={'datetime': 'dt', 'id': 'id'}
    )
    dest_pipe = Pipe(str(conn), 'inplace', 'dest', instance=conn)
    dest_pipe.delete()
    query = f"SELECT * FROM {sql_item_name(source_pipe.target, flavor)}"
    dest_pipe = Pipe(
        str(conn), 'inplace', 'dest',
        instance=conn,
        columns={'datetime': 'dt', 'id': 'id'},
        dtypes={'a': 'int', 'id': 'uuid'}, ### NOTE: casts to str for DuckDB
        parameters={
            "fetch": {
                "definition": query,
                "backtrack_minutes": 1440,
            }
        },
    )

    docs = [
        {'dt': '2023-01-01 00:00:00', 'id': UUID('9f680a72-b5f7-4336-8f7c-30927ec21cb1')},
        {'dt': '2023-01-01 00:01:00', 'id': UUID('335b0322-4b54-40aa-8019-07666cbefa52')},
        {'dt': '2023-01-01 00:02:00', 'id': UUID('d7d42913-2dfe-47d6-b0e0-7f71e13e814e')},
        {'dt': '2023-01-01 00:03:00', 'id': UUID('7e194a2c-26b4-4632-af02-e0a8b2c6ce1e')},
        {'dt': '2023-01-01 00:04:00', 'id': UUID('31e5fd08-fb81-47f4-8a1c-0c9dcf08ac5e')},
    ]
    success, msg = source_pipe.sync(docs)
    assert success, msg

    success, msg = dest_pipe.sync(debug=debug)
    assert success, msg
    assert dest_pipe.get_rowcount(debug=debug) == len(docs)

    success, msg = dest_pipe.sync(debug=debug)
    assert success, msg
    assert dest_pipe.get_rowcount(debug=debug) == len(docs)

    new_docs = [
        {'dt': '2023-01-02 00:00:00', 'id': UUID('afb0b31b-15dc-485e-ac6f-d8622b6d03d4')},
        {'dt': '2023-01-02 00:01:00', 'id': UUID('8b7bf428-d0ed-40fa-951b-bb115a03eac5')},
        {'dt': '2023-01-02 00:02:00', 'id': UUID('36aed9b4-4c7a-4566-a321-d1774ef1015a')},
        {'dt': '2023-01-02 00:03:00', 'id': UUID('7a4ef6cc-37d8-4899-9ddb-07b4998d0b53')},
        {'dt': '2023-01-02 00:04:00', 'id': UUID('59244211-fdb8-46f1-b14b-8631146758c0')},
    ]
    success, msg = source_pipe.sync(new_docs)
    assert success, msg

    success, msg = dest_pipe.sync(debug=debug)
    assert success, msg
    assert dest_pipe.get_rowcount(debug=debug) == len(docs) + len(new_docs)

    update_docs = [
        {'dt': '2023-01-01 00:00:00', 'id': UUID('9f680a72-b5f7-4336-8f7c-30927ec21cb1'), 'a': 1},
        {'dt': '2023-01-01 00:01:00', 'id': UUID('335b0322-4b54-40aa-8019-07666cbefa52'), 'a': 2},
        {'dt': '2023-01-01 00:02:00', 'id': UUID('d7d42913-2dfe-47d6-b0e0-7f71e13e814e'), 'a': 3},
        {'dt': '2023-01-01 00:03:00', 'id': UUID('7e194a2c-26b4-4632-af02-e0a8b2c6ce1e'), 'a': 4},
        {'dt': '2023-01-01 00:04:00', 'id': UUID('31e5fd08-fb81-47f4-8a1c-0c9dcf08ac5e'), 'a': 5},
    ]
    success, msg = source_pipe.sync(update_docs)
    assert success, msg
    assert source_pipe.get_rowcount(debug=debug) == len(docs) + len(new_docs)

    success, msg = dest_pipe.verify(debug=debug)
    assert success, msg
    assert dest_pipe.get_rowcount(debug=debug) == len(docs) + len(new_docs)

    df = dest_pipe.get_data(params={'id': 'd7d42913-2dfe-47d6-b0e0-7f71e13e814e'})
    assert len(df) == 1
    assert df['a'][0] == 3


@pytest.mark.parametrize("flavor", get_flavors())
def test_nested_chunks(flavor: str):
    """
    Sync nested chunk generators.
    """
    conn = conns[flavor]
    pipe = mrsm.Pipe('test', 'chunks', 'nested', instance=conn)
    pipe.delete()
    pipe = mrsm.Pipe('test', 'chunks', 'nested', instance=conn)

    num_docs = 3
    docs = [{'a': i} for i in range(num_docs)]
    chunks = ([{'chunk_ix': i, **doc}] for i, doc in enumerate(docs))
    chunks_of_chunks = (
        chunk
        for chunk in chunks
    )
    chunks_of_chunks_of_chunks = (
        g for g in chunks_of_chunks
    )
    success, msg = pipe.sync(chunks_of_chunks_of_chunks, debug=debug)
    from meerschaum.utils.formatting import print_tuple
    print_tuple((success, msg))
    assert success, msg
    df = pipe.get_data()
    assert len(df) == num_docs


@pytest.mark.parametrize("flavor", get_flavors())
def test_sync_dask_dataframe(flavor: str):
    """
    Verify that we are able to sync Dask DataFrames.
    """
    conn = conns[flavor]
    pipe = mrsm.Pipe(
        'dask', 'demo',
        columns={'datetime': 'dt'},
        instance=conn,
    )
    pipe.drop()
    pipe.sync([
        {'dt': '2023-01-01', 'id': 1},
        {'dt': '2023-01-02', 'id': 2},
        {'dt': '2023-01-03', 'id': 3},
    ])
    ddf = pipe.get_data(as_dask=True, debug=debug)

    pipe2 = mrsm.Pipe(
        'dask', 'insert',
        columns=pipe.columns,
        instance=conn,
    )
    pipe2.drop()
    pipe2.sync(ddf, debug=debug)
    assert pipe.get_data().to_dict() == pipe2.get_data().to_dict()


@pytest.mark.parametrize("flavor", get_flavors())
def test_sync_null_indices(flavor: str):
    """
    Test that null indices are accounted for.
    """
    conn = conns[flavor]
    pipe = mrsm.Pipe('sync', 'null', 'indices', instance=conn)
    pipe.delete()
    pipe = mrsm.Pipe(
        'sync', 'null', 'indices',
        instance=conn,
        columns=['a', 'b'],
    )
    docs = [{'a': 1, 'b': 1, 'c': 1}]
    success, msg = pipe.sync(docs, debug=debug)
    assert success, msg
    docs = [{'a': None, 'b': 1, 'c': 1}]
    success, msg = pipe.sync(docs, debug=debug)
    assert success, msg
    df = pipe.get_data(debug=debug)
    assert len(df) == 2

    docs = [{'a': 1, 'b': None, 'c': 1}]
    success, msg = pipe.sync(docs, debug=debug)
    assert success, msg
    df = pipe.get_data(debug=debug)
    assert len(df) == 3

    docs = [{'a': 1, 'b': None, 'c': 1}]
    success, msg = pipe.sync(docs, debug=debug)
    assert success, msg
    df = pipe.get_data(debug=debug)
    assert len(df) == 3

    docs = [{'a': None, 'b': None, 'c': 1}]
    success, msg = pipe.sync(docs, debug=debug)
    assert success, msg
    df = pipe.get_data(debug=debug)
    assert len(df) == 4

    docs = [{'c': Decimal('2.2')}]
    success, msg = pipe.sync(docs, debug=debug)
    assert success, msg
    df = pipe.get_data(debug=debug)
    assert len(df) == 4
    df = pipe.get_data(['c'], params={'a': None, 'b': [None]}, debug=debug)
    assert len(df) == 1
    assert df['c'][0] == Decimal('2.2')


@pytest.mark.parametrize("flavor", get_flavors())
def test_upsert_sync(flavor: str):
    """
    Test that setting `upsert` to `True` is able to sync successfully.
    """
    conn = conns[flavor]
    pipe = Pipe('test', 'upsert', instance=conn)
    pipe.delete()
    pipe = Pipe(
        'test', 'upsert', instance=conn,
        columns={'datetime': 'dt', 'id': 'id'},
        dtypes={'num': 'numeric'},
        parameters={'upsert': True},
    )

    docs = [
        {'dt': '2023-01-01', 'id': 1, 'yes': True, 'num': '1.1'},
        {'dt': '2023-01-02', 'id': 2, 'yes': False, 'num': '2'},
    ]
    success, msg = pipe.sync(docs, debug=debug)
    assert success, msg

    docs = [
        {'dt': '2023-01-02', 'id': 2, 'yes': None, 'num': '2'},
    ]
    success, msg = pipe.sync(docs, debug=debug)
    assert success, msg
    df = pipe.get_data(params={'id': 2})
    assert 'na' in str(df['yes'][0]).lower()
    assert isinstance(df['num'][0], Decimal)
    assert df['num'][0] == Decimal('2')


@pytest.mark.parametrize("flavor", get_flavors())
def test_upsert_no_value_cols(flavor: str):
    """
    Test that setting `upsert` to `True` is able to sync without value columns.
    """
    conn = conns[flavor]
    pipe = Pipe('test', 'upsert', 'no_vals', instance=conn)
    pipe.delete()
    pipe = Pipe(
        'test', 'upsert', 'no_vals', instance=conn,
        columns={'datetime': 'dt', 'id': 'id'},
        parameters={'upsert': True},
    )

    docs = [
        {'dt': '2023-01-01', 'id': 1},
        {'dt': '2023-01-02', 'id': 2},
        {'dt': '2023-01-03', 'id': 3},
    ]
    success, msg = pipe.sync(docs, debug=debug)
    assert success, msg

    docs = [
        {'dt': '2023-01-03', 'id': 3},
    ]
    success, msg = pipe.sync(docs, debug=debug)
    assert success, msg
    assert pipe.get_rowcount() == 3
