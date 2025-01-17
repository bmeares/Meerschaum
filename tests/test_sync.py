#! /usr/bin/env python
# -*- coding: utf-8 -*-
# vim:fenc=utf-8

import warnings
from datetime import datetime, timedelta
from decimal import Decimal
from uuid import UUID

import pytest
import meerschaum as mrsm
from meerschaum import Pipe
from meerschaum.actions import actions

from tests import debug
from tests.connectors import conns, get_flavors


@pytest.mark.parametrize("flavor", get_flavors())
def test_register_and_delete(flavor: str):
    """
    Verify user registration and deletion.
    """
    conn = conns[flavor]
    pipe = mrsm.Pipe('test', 'register', 'delete', instance=conn)
    pipe.delete()
    pipe = mrsm.Pipe('test', 'register', 'delete', instance=conn)
    params = pipe.parameters.copy()
    assert params is not None
    pipe.delete()
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
    conn = conns[flavor]
    pipe = mrsm.Pipe('test', 'drop_sync', instance=conn)
    pipe.delete()
    pipe = mrsm.Pipe(
        'test', 'drop_sync',
        instance=conn,
        columns=['datetime', 'id'],
    )
    assert pipe.exists(debug=debug) is False
    assert pipe.columns is not None
    now1 = datetime(2021, 1, 1, 12, 0)
    data = {'datetime': [now1], 'id' : [1], 'val': [1]}
    success, msg = pipe.sync(data, debug=debug)
    assert success, msg
    assert pipe.exists(debug=debug)
    now2 = datetime(2021, 1, 1, 12, 1)
    data = {'datetime': [now2], 'id' : [1], 'val': [1]}
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
    conn = conns[flavor]
    pipe = mrsm.Pipe('test', 'drop_sync', 'duplicate', instance=conn)
    pipe.delete()
    pipe = mrsm.Pipe(
        'test', 'drop_sync', 'duplicate',
        instance=conn,
        columns=['datetime', 'id'],
    )

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
def test_sync_engine(flavor: str):
    """
    Test that we can sync a test pipe via the `sync pipes` action.
    """
    ### Weird concurrency issues with our tests.
    if flavor == 'duckdb':
        return
    conn = conns[flavor]
    pipe = mrsm.Pipe('plugin:stress', 'test', 'engine', instance=conn)
    pipe.delete()
    pipe = mrsm.Pipe(
        'plugin:stress', 'test', 'engine',
        instance=conn,
        columns=['datetime', 'id'],
    )
    success, msg = pipe.register()
    assert success, msg
    mrsm_instance = str(pipe.instance_connector)
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
    pipe = Pipe('target', 'mutable', target=target, instance=conn)
    pipe.delete()
    pipe = Pipe('target', 'mutable', target=target, instance=conn, columns={'datetime': 'dt', 'id': 'id'})
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
        debug=debug,
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
    pipe = Pipe('test_id', 'index_col', 'table', instance=conn)
    pipe.delete()
    pipe = Pipe(
        'test_id', 'index_col', 'table',
        instance=conn,
        dtypes={'id': 'Int64'},
        columns={'datetime': 'id'},
    )
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
        columns={'datetime': 'dt', 'id': 'id'},
        indices={'all': ['dt', 'id']},
    )
    dest_pipe = Pipe(str(conn), 'inplace', 'dest', instance=conn)
    dest_pipe.delete()
    query = f"""
    WITH {sql_item_name('subquery', flavor)} AS (
        SELECT *
        FROM {sql_item_name(source_pipe.target, flavor)}
    )
    SELECT *
    FROM {sql_item_name('subquery', flavor)}
    """ if flavor not in ('mysql',) else f"""
    SELECT *
    FROM {sql_item_name(source_pipe.target, flavor)}
    """
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
def test_sync_inplace_upsert(flavor: str):
    """
    Verify that in-place syncing works as expected.
    """
    from meerschaum.utils.sql import sql_item_name
    conn = conns[flavor]
    if conn.type != 'sql':
        return
    source_pipe = Pipe('test', 'inplace', 'upsert', instance=conn)
    source_pipe.delete()
    source_pipe = Pipe(
        'test', 'inplace', 'upsert',
        instance=conn,
        columns={'datetime': 'dt', 'id': 'id'},
        indices={'all': ['dt', 'id']},
        upsert=True,
    )
    dest_pipe = Pipe(str(conn), 'inplace', 'upsert', instance=conn)
    dest_pipe.delete()
    query = f"""
    WITH {sql_item_name('subquery', flavor)} AS (
        SELECT *
        FROM {sql_item_name(source_pipe.target, flavor)}
    )
    SELECT *
    FROM {sql_item_name('subquery', flavor)}
    """ if flavor not in ('mysql',) else f"""
    SELECT *
    FROM {sql_item_name(source_pipe.target, flavor)}
    """
    dest_pipe = Pipe(
        str(conn), 'inplace', 'upsert',
        instance=conn,
        columns={'datetime': 'dt', 'id': 'id'},
        dtypes={'a': 'int', 'id': 'uuid'},
        upsert=True,
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
        {'dt': '2023-01-01 00:02:00', 'id': UUID('d7d42913-2dfe-47d6-b0e0-7f71e13e814e'), 'd': 7},
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
        {'dt': '2023-01-02 00:00:00', 'id': UUID('afb0b31b-15dc-485e-ac6f-d8622b6d03d4'), 'c': 9},
        {'dt': '2023-01-02 00:01:00', 'id': UUID('8b7bf428-d0ed-40fa-951b-bb115a03eac5'), 'c': 8},
        {'dt': '2023-01-02 00:02:00', 'id': UUID('36aed9b4-4c7a-4566-a321-d1774ef1015a'), 'c': 7},
        {'dt': '2023-01-02 00:03:00', 'id': UUID('7a4ef6cc-37d8-4899-9ddb-07b4998d0b53'), 'c': 6},
        {'dt': '2023-01-02 00:04:00', 'id': UUID('59244211-fdb8-46f1-b14b-8631146758c0'), 'c': 5},
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
    assert df['d'][0] == 7


@pytest.mark.parametrize("flavor", get_flavors())
def test_sync_inplace_no_datetime(flavor: str):
    """
    Verify that in-place syncing works as expected.
    """
    from meerschaum.utils.sql import sql_item_name
    conn = conns[flavor]
    if conn.type != 'sql':
        return
    source_pipe = Pipe('test', 'inplace', 'no-datetime', instance=conn)
    source_pipe.delete()
    source_pipe = Pipe(
        'test', 'inplace', 'no-datetime',
        instance=conn,
        columns={'id': 'id'},
        upsert=False,
    )
    dest_pipe = Pipe(str(conn), 'inplace', 'no-datetime', instance=conn)
    dest_pipe.delete()
    query = f"""
    WITH {sql_item_name('subquery', flavor)} AS (
        SELECT *
        FROM {sql_item_name(source_pipe.target, flavor)}
    )
    SELECT *
    FROM {sql_item_name('subquery', flavor)}
    """ if flavor not in ('mysql',) else f"""
    SELECT *
    FROM {sql_item_name(source_pipe.target, flavor)}
    """
    dest_pipe = Pipe(
        str(conn), 'inplace', 'no-datetime',
        instance=conn,
        columns={'id': 'id'},
        dtypes={'a': 'int', 'id': 'uuid'},
        upsert=False,
        static=False,
        parameters={
            "fetch": {
                "definition": query,
            }
        },
    )

    docs = [
        {'id': UUID('9f680a72-b5f7-4336-8f7c-30927ec21cb1')},
        {'id': UUID('335b0322-4b54-40aa-8019-07666cbefa52')},
        {'id': UUID('d7d42913-2dfe-47d6-b0e0-7f71e13e814e'), 'd': 7},
        {'id': UUID('7e194a2c-26b4-4632-af02-e0a8b2c6ce1e')},
        {'id': UUID('31e5fd08-fb81-47f4-8a1c-0c9dcf08ac5e')},
    ]
    success, msg = source_pipe.sync(docs)
    assert success, msg

    success, msg = dest_pipe.sync(debug=debug, chunksize=1)
    assert success, msg
    assert dest_pipe.get_rowcount(debug=debug) == len(docs)

    success, msg = dest_pipe.sync(debug=debug, chunksize=1)
    assert success, msg
    assert dest_pipe.get_rowcount(debug=debug) == len(docs)

    new_docs = [
        {'id': UUID('afb0b31b-15dc-485e-ac6f-d8622b6d03d4'), 'c': 9},
        {'id': UUID('8b7bf428-d0ed-40fa-951b-bb115a03eac5'), 'c': 8},
        {'id': UUID('36aed9b4-4c7a-4566-a321-d1774ef1015a'), 'c': 7},
        {'id': UUID('7a4ef6cc-37d8-4899-9ddb-07b4998d0b53'), 'c': 6},
        {'id': UUID('59244211-fdb8-46f1-b14b-8631146758c0'), 'c': 5},
    ]
    success, msg = source_pipe.sync(new_docs)
    assert success, msg

    success, msg = dest_pipe.sync(debug=debug, chunksize=1)
    assert success, msg
    assert dest_pipe.get_rowcount(debug=debug) == len(docs) + len(new_docs)

    update_docs = [
        {'id': UUID('9f680a72-b5f7-4336-8f7c-30927ec21cb1'), 'a': 1},
        {'id': UUID('335b0322-4b54-40aa-8019-07666cbefa52'), 'a': 2},
        {'id': UUID('d7d42913-2dfe-47d6-b0e0-7f71e13e814e'), 'a': 3},
        {'id': UUID('7e194a2c-26b4-4632-af02-e0a8b2c6ce1e'), 'a': 4},
        {'id': UUID('31e5fd08-fb81-47f4-8a1c-0c9dcf08ac5e'), 'a': 5},
    ]
    success, msg = source_pipe.sync(update_docs)
    assert success, msg
    assert source_pipe.get_rowcount(debug=debug) == len(docs) + len(new_docs)

    success, msg = dest_pipe.verify(debug=debug, chunksize=1)
    assert success, msg
    assert dest_pipe.get_rowcount(debug=debug) == len(docs) + len(new_docs)

    df = dest_pipe.get_data(params={'id': 'd7d42913-2dfe-47d6-b0e0-7f71e13e814e'})
    assert len(df) == 1
    assert df['a'][0] == 3
    assert df['d'][0] == 7


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


@pytest.mark.skip(reason="Python 3.13 Dask, numpy compatability.")
@pytest.mark.parametrize("flavor", get_flavors())
def test_sync_dask_dataframe(flavor: str):
    """
    Verify that we are able to sync Dask DataFrames.
    """
    conn = conns[flavor]
    pipe = mrsm.Pipe('dask', 'demo', instance=conn)
    pipe.delete()
    pipe = mrsm.Pipe(
        'dask', 'demo',
        columns={'datetime': 'dt'},
        dtypes={'dt': 'datetime'},
        instance=conn,
    )
    pipe.sync([
        {'dt': '2023-01-01', 'id': 1},
        {'dt': '2023-01-02', 'id': 2},
        {'dt': '2023-01-03', 'id': 3},
    ])
    ddf = pipe.get_data(as_dask=True, debug=debug)

    pipe2 = mrsm.Pipe('dask', 'insert', instance=conn)
    pipe2.delete()
    pipe2 = mrsm.Pipe(
        'dask', 'insert',
        columns=pipe.columns,
        dtypes=pipe.dtypes,
        instance=conn,
    )
    pipe2.sync(ddf, debug=debug)
    df = pipe.get_data()
    df2 = pipe2.get_data()
    docs = df.to_dict(orient='records')
    docs2 = df2.to_dict(orient='records')
    assert docs == docs2


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
        indices={'all': ['dt', 'id']},
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


@pytest.mark.filterwarnings("ignore:UNIQUE constraint failed")
@pytest.mark.parametrize("flavor", get_flavors())
def test_primary_key(flavor: str):
    """
    Test that regular primary keys are enforced.
    """
    conn = conns[flavor]
    if conn.type != 'sql':
        return
    pipe = mrsm.Pipe('test_sync', 'primary', 'key', instance=conn)
    pipe.delete()
    pipe = mrsm.Pipe(
        'test_sync', 'primary', 'key',
        instance=conn,
        columns={
            'primary': 'pk',
        },
    )
    with warnings.catch_warnings():
        warnings.filterwarnings('ignore')
        success, msg = pipe.sync([
            {'pk': 1},
            {'pk': 1},
        ], debug=debug)
    assert not success

    pipe.drop()

    success, msg = pipe.sync([{'pk': 1}], debug=debug)
    assert success, msg

    success, msg = pipe.sync([{'pk': 1}], debug=debug)
    assert success

    success, msg = pipe.sync([{'pk': 2}], debug=debug)
    assert success, msg

    assert pipe.get_rowcount(debug=debug) == 2


@pytest.mark.parametrize("flavor", get_flavors())
def test_autoincrement_primary_key(flavor: str):
    """
    Test that explicitly incrementing primary keys behave as expected.
    """
    from meerschaum.utils.sql import SKIP_AUTO_INCREMENT_FLAVORS
    conn = conns[flavor]
    if conn.type != 'sql':
        return
    if conn.flavor in SKIP_AUTO_INCREMENT_FLAVORS:
        return
    pipe = mrsm.Pipe('test_sync', 'primary_key', 'autoincrement', instance=conn)
    pipe.delete()
    pipe = mrsm.Pipe(
        'test_sync', 'primary_key', 'autoincrement',
        instance=conn,
        columns={
            'primary': 'id',
        },
        parameters={
            'autoincrement': True,
        },
        dtypes={'id': 'int'},
    )
    success, msg = pipe.sync([
        {'color': 'red'},
        {'color': 'blue'},
    ], debug=debug)
    assert success

    df = pipe.get_data(['id'])
    assert list(df['id']) == [1, 2]

    success, msg = pipe.sync([{'id': 1, 'color': 'green'}], debug=debug)
    assert success, msg

    df = pipe.get_data(params={'id': 1})
    assert df['color'][0] == 'green'

    success, msg = pipe.sync([{'id': 4, 'shirt_size': 'L'}, {'id': 5, 'shirt_size': 'M'}], debug=debug)
    assert success

    df = pipe.get_data(['shirt_size'], params={'id': [4, 5]}, debug=debug)
    assert list(df['shirt_size']) == ['L', 'M']

    success, msg = pipe.sync([{'color': 'purple'}, {'shirt_size': 'S'}], debug=debug)
    assert success, msg

    df = pipe.get_data()
    print(df)
    df = pipe.get_data(['shirt_size'], params={'id': 7}, debug=debug)
    assert df['shirt_size'][0] == 'S'

    assert pipe.get_rowcount(debug=debug) == 6


@pytest.mark.parametrize("flavor", get_flavors())
def test_autoincrement_primary_key_inferred(flavor: str):
    """
    Test that implicitly incrementing primary keys behave as expected.
    """
    from meerschaum.utils.sql import SKIP_AUTO_INCREMENT_FLAVORS
    conn = conns[flavor]
    if conn.type != 'sql':
        return
    if conn.flavor in SKIP_AUTO_INCREMENT_FLAVORS:
        return

    pipe = mrsm.Pipe('test_sync', 'pk', 'implicit', instance=conn)
    pipe.delete()
    pipe = mrsm.Pipe(
        'test_sync', 'pk', 'implicit',
        instance=conn,
        columns={
            'primary': 'id',
        },
    )
    success, msg = pipe.sync([
        {'color': 'red'},
        {'color': 'blue'},
    ], debug=debug)
    assert success

    df = pipe.get_data(['id'])
    assert list(df['id']) == [1, 2]

    success, msg = pipe.sync([{'id': 1, 'color': 'green'}], debug=debug)
    assert success, msg

    df = pipe.get_data(params={'id': 1})
    assert df['color'][0] == 'green'

    success, msg = pipe.sync([{'id': 4, 'shirt_size': 'L'}, {'id': 5, 'shirt_size': 'M'}], debug=debug)
    assert success

    df = pipe.get_data(['shirt_size'], params={'id': [4, 5]}, debug=debug)
    assert list(df['shirt_size']) == ['L', 'M']

    success, msg = pipe.sync([{'color': 'purple'}], debug=debug)
    assert success, msg

    df = pipe.get_data(['color'], params={'id': 6}, debug=debug)
    assert df['color'][0] == 'purple'

    assert pipe.get_rowcount(debug=debug) == 5


@pytest.mark.parametrize("flavor", get_flavors())
def test_add_primary_key_to_existing(flavor: str):
    """
    Test that a pipe can have a primary key added later.
    """
    conn = conns[flavor]
    if conn.type != 'sql':
        return
    if conn.flavor == 'duckdb':
        return
    pipe = mrsm.Pipe('test_sync', 'primary_key', 'later', instance=conn)
    pipe.delete()
    pipe = mrsm.Pipe(
        'test_sync', 'primary_key', 'later',
        instance=conn,
    )
    success, msg = pipe.sync([
        {'id': 1, 'color': 'red'},
        {'id': 2, 'color': 'blue'},
    ], debug=debug)
    assert success

    df = pipe.get_data(['id'])
    assert list(df['id']) == [1, 2]

    pipe.columns = {'primary': 'id'}
    success = pipe.instance_connector.create_indices(pipe, debug=debug)
    assert success

    success, msg = pipe.sync([{'id': 3, 'color': 'green'}], debug=debug)
    assert success, msg

    columns_indices = pipe.get_columns_indices(debug=debug)
    id_index_types = [
        columns_indices['id'][i]['type']
        for i in range(len(columns_indices['id']))
    ]

    assert 'PRIMARY KEY' in id_index_types
    df = pipe.get_data(params={'id': 3}, debug=debug)
    assert df['color'][0] == 'green'


@pytest.mark.parametrize("flavor", get_flavors())
def test_static_schema(flavor: str):
    """
    Test that pipes marked as `static` do not mutate their schemata.
    """
    conn = conns[flavor]
    if conn.type not in ('sql', 'api'):
        return
    pipe = mrsm.Pipe('test', 'static', instance=conn)
    pipe.delete()
    pipe = mrsm.Pipe(
        'test', 'static',
        instance=conn,
        upsert=True,
        static=True,
        columns={
            'datetime': 'dt',
            'primary': 'id',
        },
        dtypes={
            'id': 'uuid',
            'dt': 'datetime64[ns]',
            'num': 'numeric',
            'val': 'float',
            'meta': 'json',
        },
    )
    docs = [
        {
            'dt': '2024-01-01 01:02:03.456789',
            'id': 'e7a57f9b-da26-4c70-bffc-ed27cdd74565',
            'num': '1.23',
            'val': '2.34',
            'meta': '{"a": 1}',
        },
    ]
    success, msg = pipe.sync(docs, debug=debug)
    assert success, msg

    new_docs = [
        {
            'dt': '2024-01-01 01:02:03.456789',
            'id': '44be2c5a-3d42-4e5f-b118-93a56697ae14',
            'val': 'foo',
            'bar': 1,
        },
    ]

    with warnings.catch_warnings():
        warnings.filterwarnings('ignore')
        try:
            success, msg = pipe.sync(new_docs, debug=debug)
        except Exception as e:
            success, msg = False, str(e)
    assert not success

    cols_types = pipe.get_columns_types(debug=debug)
    assert 'bar' not in cols_types
    assert cols_types['val'].upper() in ('DOUBLE', 'DOUBLE PRECISION', 'FLOAT', 'REAL')
    assert pipe.get_rowcount() == 1


@pytest.mark.parametrize("flavor", get_flavors())
def test_create_drop_indices(flavor):
    """
    Verify that pipes are able to drop and rebuild indices.
    """
    conn = conns[flavor]
    if conn.type not in ('sql',):
        return
    pipe = mrsm.Pipe('test', 'indices', 'drop', instance=conn)
    pipe.delete()
    pipe = mrsm.Pipe(
        'test', 'indices', 'drop',
        instance=conn,
        columns={'primary': 'Id', 'datetime': 'dt'},
        upsert=True,
    )
    docs = [
        {'Id': 1, 'dt': '2025-01-01', 'val': 1.1},
        {'Id': 2, 'dt': '2025-01-02', 'val': 1.2},
    ]
    with warnings.catch_warnings():
        warnings.filterwarnings('ignore')
        success, msg = pipe.sync(docs, debug=debug)
    assert success, msg

    cols_indices = pipe.get_columns_indices(debug=debug) 
    assert len(cols_indices) > 1

    og_cols_indices = cols_indices

    success, msg = pipe.drop_indices(debug=debug)
    assert success, msg

    pipe.indices = {}
    pipe.edit()

    cols_indices = pipe.get_columns_indices(debug=debug) 
    assert len(cols_indices) <= len(og_cols_indices)

    success, msg = pipe.create_indices(debug=debug)
    assert success, msg

    cols_indices = pipe.get_columns_indices(debug=debug) 
    assert len(cols_indices) == len(og_cols_indices)


@pytest.mark.parametrize("flavor", get_flavors())
def test_no_null_indices(flavor):
    """
    Test that setting `null_indices` to `False` syncs as expected.
    """
    conn = conns[flavor]
    if conn.type != 'sql':
        return
    pipe = mrsm.Pipe('test', 'null_indices', 'false', instance=conn)
    pipe.delete()
    pipe = mrsm.Pipe(
        'test', 'null_indices', 'false',
        instance=conn,
        columns={
            'datetime': 'dt',
            'id': 'id',
        },
        null_indices=False,
    )
    docs = [
        {'dt': '2025-01-01', 'id': 1, 'val': 1.1},
        {'dt': '2025-01-01', 'id': 2, 'val': 2.2},
    ]
    success, msg = pipe.sync(docs, debug=debug)
    assert success, msg

    inplace_pipe = mrsm.Pipe(conn, 'null_indices', 'false', instance=conn)
    inplace_pipe.delete()
    inplace_pipe = mrsm.Pipe(
        conn, 'null_indices', 'false',
        instance=conn,
        columns=pipe.columns,
        null_indices=False,
        upsert=False,
        parameters={
            'sql': "SELECT * FROM {{" + str(pipe) + "}}",
        },
    )
    success, msg = inplace_pipe.sync(debug=debug)

    new_docs = [
        {'dt': '2025-01-02', 'id': 3, 'val': 3.3},
        {'dt': '2025-01-02', 'id': 4, 'val': 4.4},
    ]
    success, msg = pipe.sync(new_docs, debug=debug)
    assert success, msg

    success, msg = inplace_pipe.verify(debug=debug)
    assert success, msg

    assert inplace_pipe.get_rowcount() == len(docs + new_docs)


@pytest.mark.parametrize("flavor", get_flavors())
def test_sync_sql_small_chunksize(flavor):
    """
    Test that syncing a small chunksize produces the expected results.
    """
    conn = conns[flavor]
    if conn.type != 'sql':
        return

    pipe = mrsm.Pipe('test', 'sync_sql_chunksize', 'small', instance=conn)
    pipe.delete()
    pipe = mrsm.Pipe(
        'test', 'sync_sql_chunksize', 'small',
        instance=conn,
        columns={'primary': 'id', 'datetime': 'id'},
        dtypes={'id': 'int'},
    )
    docs = [
        {'id': 1, 'foo': 'abc'},
        {'id': 2, 'foo': 'def'},
        {'id': 3, 'foo': 'ghi'},
        {'id': 4, 'foo': 'jkl'},
    ]
    success, msg = pipe.sync(docs, debug=debug)
    assert success, msg

    downstream_instance_conn = (
        conn
        if flavor != 'sqlite'
        else mrsm.get_connector(
            'sql:test_sql_small_chunksize',
            database=conn.database + '.test_sql_small_chunksize',
            flavor='sqlite',
        )
    )

    downstream_pipe = mrsm.Pipe(conn, 'test', 'small_chunksize', instance=downstream_instance_conn)
    downstream_pipe.delete()
    downstream_pipe = mrsm.Pipe(
        conn, 'test', 'small_chunksize',
        instance=downstream_instance_conn,
        columns=pipe.columns,
        dtypes=pipe.dtypes,
        parameters={
            'fetch': {
                'definition': "SELECT * FROM {{" + str(pipe) + "}}",
            },
        }
    )

    chunksize = 1
    success, msg = downstream_pipe.sync(chunksize=chunksize, debug=debug, _inplace=False)
    assert success, msg

    mrsm.pprint((success, msg))
    assert msg.lower().count('inserted') == int(len(docs) / chunksize)
