#! /usr/bin/env python
# -*- coding: utf-8 -*-
# vim:fenc=utf-8

import pytest
import datetime
from decimal import Decimal
from uuid import UUID
from tests import debug
from tests.connectors import conns, get_flavors
from tests.test_users import test_register_user
import meerschaum as mrsm
from meerschaum import Pipe
from meerschaum.utils.dtypes import are_dtypes_equal
from meerschaum.utils.sql import sql_item_name
from meerschaum.utils.dtypes.sql import PD_TO_DB_DTYPES_FLAVORS

@pytest.fixture(autouse=True)
def run_before_and_after(flavor: str):
    test_register_user(flavor)
    yield


@pytest.mark.parametrize("flavor", get_flavors())
def test_sync_change_columns_dtypes(flavor: str):
    """
    Test that new columns are added.
    """
    conn = conns[flavor]
    pipe = Pipe('change', 'cols', 'dtypes', columns={'datetime': 'dt', 'id': 'id'}, instance=conn)
    pipe.delete(debug=debug)
    pipe = Pipe('change', 'cols', 'dtypes', columns={'datetime': 'dt', 'id': 'id'}, instance=conn)
    docs = [
        {'dt': '2022-01-01', 'id': 1, 'a': 10},
    ]
    pipe.sync(docs, debug=debug)
    assert len(pipe.get_data().columns) == 3

    docs = [
        {'dt': '2022-01-01', 'id': 1, 'a': 'foo'},
    ]
    pipe.sync(docs, debug=debug)
    df = pipe.get_data()
    assert len(df.columns) == 3
    assert len(df) == 1
    assert are_dtypes_equal(str(df.dtypes['a']), 'string')


@pytest.mark.parametrize("flavor", get_flavors())
def test_dtype_enforcement(flavor: str):
    """
    Test that incoming rows are enforced to the correct data type.
    """
    conn = conns[flavor]
    pipe = mrsm.Pipe('dtype', 'enforcement', instance=conn)
    pipe.delete(debug=debug)
    pipe = Pipe(
        'dtype', 'enforcement',
        columns={
            'datetime': 'dt',
            'id': 'id',
        },
        dtypes={
            'int': 'int',
            'float': 'float',
            'bool': 'bool',
            'object': 'object',
            'json': 'json',
            'numeric': 'numeric',
            'str': 'str',
            'uuid': 'uuid',
        },
        instance=conn,
    )
    pipe.sync([{'dt': '2022-01-01', 'id': 1, 'int': '1'}], debug=debug)
    pipe.sync([{'dt': '2022-01-01', 'id': 1, 'float': '1.0'}], debug=debug)
    pipe.sync([{'dt': '2022-01-01', 'id': 1, 'bool': 'True'}], debug=debug)
    pipe.sync([{'dt': '2022-01-01', 'id': 1, 'object': 'foo'}], debug=debug)
    pipe.sync([{'dt': '2022-01-01', 'id': 1, 'str': 'bar'}], debug=debug)
    pipe.sync([{'dt': '2022-01-01', 'id': 1, 'json': '{"a": {"b": 1}}'}], debug=debug)
    pipe.sync([{'dt': '2022-01-01', 'id': 1, 'numeric': '1'}], debug=debug)
    pipe.sync([{'dt': '2022-01-01', 'id': 1, 'uuid': '00000000-1234-5678-0000-000000000000'}], debug=debug)
    df = pipe.get_data(debug=debug)
    assert len(df) == 1
    assert len(df.columns) == 10
    for col, typ in df.dtypes.items():
        pipe_dtype = pipe.dtypes[col]
        if pipe_dtype == 'json':
            assert isinstance(df[col][0], dict)
            pipe_dtype = 'object'
        elif pipe_dtype == 'numeric':
            assert isinstance(df[col][0], Decimal)
            pipe_dtype = 'object'
        elif pipe_dtype == 'uuid':
            assert isinstance(df[col][0], UUID)
            pipe_dtype = 'object'
        elif pipe_dtype == 'str':
            assert isinstance(df[col][0], str)
        assert are_dtypes_equal(pipe_dtype.lower(), typ)


@pytest.mark.parametrize("flavor", get_flavors())
def test_infer_json_dtype(flavor: str):
    """
    Ensure that new pipes with complex columns (dict or list) as enforced as JSON. 
    """
    from meerschaum.utils.formatting import pprint
    from meerschaum.utils.misc import generate_password
    session_id = generate_password(6)
    conn = conns[flavor]
    pipe = Pipe('foo', 'bar', session_id, instance=conn)
    _ = pipe.delete(debug=debug)
    pipe = Pipe('foo', 'bar', session_id, instance=conn)
    success, msg = pipe.sync([
        {'a': ['b', 'c']},
        {'a': {'b': 1}},
    ])
    assert success, msg
    pprint(pipe.get_columns_types())
    df = pipe.get_data(debug=debug)
    print(df)
    assert isinstance(df['a'][0], list)
    assert isinstance(df['a'][1], dict)


@pytest.mark.parametrize("flavor", get_flavors())
def test_force_json_dtype(flavor: str):
    """
    Ensure that new pipes with complex columns (dict or list) as enforced as JSON. 
    """
    import json
    from meerschaum.utils.formatting import pprint
    from meerschaum.utils.misc import generate_password
    session_id = generate_password(6)
    conn = conns[flavor]
    pipe = Pipe('foo', 'bar', session_id, instance=conn, dtypes={'a': 'json'})
    _ = pipe.delete(debug=debug)
    pipe = Pipe('foo', 'bar', session_id, instance=conn, dtypes={'a': 'json'})
    success, msg = pipe.sync([
        {'a': json.dumps(['b', 'c'])},
        {'a': json.dumps({'b': 1})},
    ])
    assert success, msg
    pprint(pipe.get_columns_types())
    df = pipe.get_data(debug=debug)
    print(df)
    assert isinstance(df['a'][0], list)
    assert isinstance(df['a'][1], dict)


@pytest.mark.parametrize("flavor", get_flavors())
def test_infer_numeric_dtype(flavor: str):
    """
    Ensure that `Decimal` objects are persisted as `numeric`.
    """
    from meerschaum.utils.formatting import pprint
    from meerschaum.utils.misc import generate_password
    from meerschaum.utils.dtypes.sql import NUMERIC_PRECISION_FLAVORS
    scale, precision = NUMERIC_PRECISION_FLAVORS.get(
        flavor, NUMERIC_PRECISION_FLAVORS['sqlite']
    )
    digits = (list(reversed(range(0, 10))) * 4)[:(-1 * (40 - scale))]
    decimal_digits = digits[(-1 * precision):]
    numeric_digits = digits[:(-1 * precision)]
    numeric_str = (
        ''.join([str(digit) for digit in numeric_digits])
        + '.' +
        ''.join([str(digit) for digit in decimal_digits])
    )
    conn = conns[flavor]
    pipe = Pipe('infer', 'numeric', instance=conn)
    _ = pipe.delete(debug=debug)
    pipe = Pipe('infer', 'numeric', instance=conn)
    success, msg = pipe.sync([
        {'a': Decimal('1')},
        {'a': Decimal(numeric_str)},
    ])
    assert success, msg
    pprint(pipe.get_columns_types())
    df = pipe.get_data(debug=debug)
    print(df)
    assert isinstance(df['a'][0], Decimal)
    assert df['a'][0] == Decimal('1')
    assert isinstance(df['a'][1], Decimal)
    assert df['a'][1] == Decimal(numeric_str)


@pytest.mark.parametrize("flavor", get_flavors())
def test_infer_uuid_dtype(flavor: str):
    """
    Ensure that `UUID` objects are persisted as `uuid`.
    """
    from meerschaum.utils.formatting import pprint
    conn = conns[flavor]
    pipe = Pipe('infer', 'uuid', instance=conn)
    _ = pipe.delete(debug=debug)
    pipe = Pipe('infer', 'uuid', instance=conn)
    uuid_str = "e6f3a4ea-f1af-4e93-8da9-716b57672206"
    success, msg = pipe.sync(
        [
            {'a': UUID(uuid_str)},
        ],
        debug=debug,
    )
    assert success, msg
    pprint(pipe.get_columns_types())
    df = pipe.get_data(debug=debug)
    print(df)
    assert isinstance(df['a'][0], UUID)
    assert df['a'][0] == UUID(uuid_str)


@pytest.mark.parametrize("flavor", get_flavors())
def test_infer_numeric_from_mixed_types(flavor: str):
    """
    Ensure that new pipes with complex columns (dict or list) as enforced as NUMERIC. 
    """
    from meerschaum.utils.formatting import pprint
    from meerschaum.utils.misc import generate_password
    session_id = generate_password(6)
    conn = conns[flavor]
    pipe = Pipe('foo', 'bar', session_id, instance=conn)
    _ = pipe.delete(debug=debug)
    pipe = Pipe('foo', 'bar', session_id, instance=conn)
    success, msg = pipe.sync([{'a': 1}])
    assert success, msg
    success, msg = pipe.sync([{'a': 2.1}])
    assert success, msg
    pprint(pipe.get_columns_types())
    df = pipe.get_data(debug=debug)
    print(df)
    assert isinstance(df['a'][0], Decimal)
    assert df['a'][0] == Decimal('1')
    assert isinstance(df['a'][1], Decimal)
    assert df['a'][1] == Decimal('2.1')


@pytest.mark.parametrize("flavor", get_flavors())
def test_force_numeric_dtype(flavor: str):
    """
    Ensure that new pipes with complex columns (dict or list) as enforced as NUMERIC. 
    """
    from meerschaum.utils.formatting import pprint
    from meerschaum.utils.misc import generate_password
    session_id = generate_password(6)
    conn = conns[flavor]
    pipe = Pipe('foo', 'bar', session_id, instance=conn, dtypes={'a': 'numeric'})
    _ = pipe.delete(debug=debug)
    pipe = Pipe('foo', 'bar', session_id, instance=conn, dtypes={'a': 'numeric'})
    success, msg = pipe.sync([
        {'a': '1'},
        {'a': '2.1'},
    ])
    assert success, msg
    pprint(pipe.get_columns_types())
    df = pipe.get_data(debug=debug)
    print(df)
    assert isinstance(df['a'][0], Decimal)
    assert df['a'][0] == Decimal('1')
    assert isinstance(df['a'][1], Decimal)
    assert df['a'][1] == Decimal('2.1')

    success, msg = pipe.sync([{'a': None}], debug=debug)
    assert success, msg
    pprint(pipe.get_columns_types())
    df = pipe.get_data(debug=debug)
    docs = df.to_dict(orient='records')
    assert str(docs[-1]['a']) == 'NaN'
    for doc in docs:
        assert isinstance(doc['a'], Decimal)


@pytest.mark.parametrize("flavor", get_flavors())
def test_utc_offset_datetimes(flavor: str):
    """
    Verify that we are able to sync rows with UTC offset datetimes.
    """
    conn = conns[flavor]
    pipe = Pipe(
        'test_utc_offset', 'datetimes',
        instance = conn,
        columns = {'datetime': 'dt'},
    )
    pipe.delete()

    docs = [
        {'dt': '2023-01-01 00:00:00+00:00'},
        {'dt': '2023-01-02 00:00:00+01:00'},
    ]

    expected_docs = [
        {'dt': datetime.datetime(2023, 1, 1)},
        {'dt': datetime.datetime(2023, 1, 1, 23, 0, 0)}
    ]

    success, msg = pipe.sync(docs, debug=debug)
    assert success, msg
    df = pipe.get_data(debug=debug)
    synced_docs = df.to_dict(orient='records')
    assert synced_docs == expected_docs


@pytest.mark.parametrize("flavor", get_flavors())
def test_ignore_datetime_conversion(flavor: str):
    """
    If the user specifies, skip columns from being detected as datetimes.
    """
    conn = conns[flavor]
    pipe = Pipe(
        'test_utc_offset', 'datetimes', 'ignore',
        instance = conn,
        dtypes = {
            'dt': 'str',
        },
    )
    pipe.delete()

    docs = [
        {'dt': '2023-01-01 00:00:00+00:00'},
        {'dt': '2023-01-02 00:00:00+01:00'},
    ]

    expected_docs = [
        {'dt': '2023-01-01 00:00:00+00:00'},
        {'dt': '2023-01-02 00:00:00+01:00'},
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
        instance = conn,
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
def test_sync_bools(flavor: str):
    """
    Test that pipes are able to sync bools.
    """
    conn = conns[flavor]
    pipe = mrsm.Pipe('test', 'bools', instance=conn)
    _ = pipe.delete()
    pipe = mrsm.Pipe(
        'test', 'bools',
        instance = conn,
        columns = {'datetime': 'dt'},
        dtypes = {'is_bool': 'bool'},
    )
    _ = pipe.drop()
    docs = [
        {'dt': '2023-01-01', 'is_bool': True},
        {'dt': '2023-01-02', 'is_bool': False},
    ]
    success, msg = pipe.sync(docs, debug=False)
    assert success, msg

    df = pipe.get_data()
    assert 'bool' in str(df.dtypes['is_bool'])

    synced_docs = [
        {
            'dt': doc['dt'].strftime('%Y-%m-%d'),
            'is_bool': doc['is_bool'],
        }
        for doc in df.to_dict(orient='records')
    ]
    assert synced_docs == docs

    new_docs = [
        {'dt': '2023-01-01', 'is_bool': False},
        {'dt': '2023-01-02', 'is_bool': True},
    ]
    success, msg = pipe.sync(new_docs, debug=debug)
    assert success, msg

    df = pipe.get_data()
    synced_docs = [
        {
            'dt': doc['dt'].strftime('%Y-%m-%d'),
            'is_bool': doc['is_bool'],
        }
        for doc in df.to_dict(orient='records')
    ]

    assert synced_docs == new_docs


@pytest.mark.parametrize("flavor", get_flavors())
def test_sync_bools_inferred(flavor: str):
    """
    Test that pipes are able to sync bools.
    """
    conn = conns[flavor]
    pipe = mrsm.Pipe('test', 'bools', instance=conn)
    _ = pipe.delete()
    pipe = mrsm.Pipe(
        'test', 'bools',
        instance = conn,
        columns = {'datetime': 'dt'},
    )
    _ = pipe.drop()
    docs = [
        {'dt': '2023-01-01', 'is_bool': True},
        {'dt': '2023-01-02', 'is_bool': False},
    ]
    success, msg = pipe.sync(docs, debug=True)
    assert success, msg

    df = pipe.get_data()
    assert 'bool' in str(df.dtypes['is_bool'])

    synced_docs = [
        {
            'dt': doc['dt'].strftime('%Y-%m-%d'),
            'is_bool': doc['is_bool'],
        }
        for doc in df.to_dict(orient='records')
    ]
    assert synced_docs == docs

    new_docs = [
        {'dt': '2023-01-01', 'is_bool': False},
        {'dt': '2023-01-02', 'is_bool': True},
    ]
    success, msg = pipe.sync(new_docs, debug=debug)
    assert success, msg

    df = pipe.get_data()
    synced_docs = [
        {
            'dt': doc['dt'].strftime('%Y-%m-%d'),
            'is_bool': doc['is_bool'],
        }
        for doc in df.to_dict(orient='records')
    ]

    assert synced_docs == new_docs


@pytest.mark.parametrize("flavor", get_flavors())
def test_sync_bools_inplace(flavor: str):
    """
    Test that pipes are able to sync bool in-place.
    """
    conn = conns[flavor]
    if conn.type not in ('api', 'sql'):
        return
    pipe = mrsm.Pipe('test', 'bools', 'inplace', instance=conn)
    _ = pipe.delete()
    pipe = mrsm.Pipe(
        'test', 'bools', 'inplace',
        instance=conn,
        columns={'datetime': 'dt', 'id': 'id'},
    )
    pipe_table = sql_item_name(pipe.target, conn.flavor) if conn.type == 'sql' else pipe.target
    inplace_pipe = mrsm.Pipe(conn, 'bools', 'inplace', instance=conn)
    _ = inplace_pipe.delete()
    inplace_pipe = mrsm.Pipe(
        conn, 'bools', 'inplace',
        instance=conn,
        columns=pipe.columns,
        dtypes={
            'is_bool': 'bool',
        },
        parameters={
            'fetch': {
                'definition': f"SELECT * FROM {pipe_table}",
                'pipe': pipe.keys(),
            },
        },
    )
    _ = pipe.drop()
    docs = [
        {'dt': '2023-01-01', 'id': 1, 'is_bool': True},
        {'dt': '2023-01-02', 'id': 2, 'is_bool': False},
        {'dt': '2023-01-03', 'id': 3, 'is_bool': None},
    ]
    success, msg = pipe.sync(docs)
    assert success, msg

    success, msg = inplace_pipe.sync(debug=debug)
    assert success, msg

    assert 'bool' in inplace_pipe.dtypes['is_bool']
    assert inplace_pipe.get_rowcount() == len(docs)

    df = inplace_pipe.get_data()

    pipe.sync([{'dt': '2023-01-03', 'id': 3, 'is_bool': True}])
    success, msg = inplace_pipe.sync(debug=debug)
    assert success, msg
    df = inplace_pipe.get_data(params={'id': 3})
    assert 'true' in str(df['is_bool'][0]).lower()

    pipe.sync([{'dt': '2023-01-03', 'id': 3, 'is_bool': None}])
    success, msg = inplace_pipe.sync(debug=debug)
    assert success, msg
    df = inplace_pipe.get_data(params={'id': 3})
    assert 'na' in str(df['is_bool'][0]).lower()


@pytest.mark.parametrize("flavor", get_flavors())
def test_sync_uuids_inplace(flavor: str):
    """
    Test that pipes are able to sync UUIDs in-place.
    """
    conn = conns[flavor]
    if conn.type not in ('api', 'sql'):
        return
    pipe = mrsm.Pipe('test', 'uuid', 'inplace', instance=conn)
    _ = pipe.delete()
    pipe = mrsm.Pipe(
        'test', 'uuid', 'inplace',
        instance=conn,
        columns={'datetime': 'dt', 'id': 'id'},
    )
    pipe_table = sql_item_name(pipe.target, conn.flavor) if conn.type == 'sql' else pipe.target
    inplace_pipe = mrsm.Pipe(conn, 'uuid', 'inplace', instance=conn)
    _ = inplace_pipe.delete()
    inplace_pipe = mrsm.Pipe(
        conn, 'uuid', 'inplace',
        instance=conn,
        columns=pipe.columns,
        dtypes={
            'uuid_col': 'uuid',
        },
        parameters={
            'fetch': {
                'definition': f"SELECT * FROM {pipe_table}",
                'pipe': pipe.keys(),
            },
        },
    )
    _ = pipe.drop()
    docs = [
        {'dt': '2023-01-01', 'id': 1, 'uuid_col': UUID('77e704d2-7513-45c7-b806-7b5cb0badc37')},
        {'dt': '2023-01-02', 'id': 2, 'uuid_col': UUID('2854eeed-2911-4641-8d67-6ecd217392cc')},
        {'dt': '2023-01-03', 'id': 3},
    ]
    success, msg = pipe.sync(docs)
    assert success, msg

    success, msg = inplace_pipe.sync(debug=debug)
    assert success, msg

    assert 'uuid' in inplace_pipe.dtypes['uuid_col']
    assert inplace_pipe.get_rowcount() == len(docs)
    db_col = inplace_pipe.get_columns_types()['uuid_col']
    if flavor in PD_TO_DB_DTYPES_FLAVORS['uuid']:
        uuid_typ = PD_TO_DB_DTYPES_FLAVORS['uuid'][flavor]
        assert db_col == uuid_typ

    df = inplace_pipe.get_data()

    update_uuid = UUID('a12bbc7c-4595-4bfe-a9b6-8b81c6e329c8')
    pipe.sync([{'dt': '2023-01-03', 'id': 3, 'uuid_col': update_uuid}])
    success, msg = inplace_pipe.sync(debug=debug)
    assert success, msg
    df = inplace_pipe.get_data(params={'id': 3})
    assert df['uuid_col'][0] == update_uuid

    pipe.sync([{'dt': '2023-01-03', 'id': 3, 'uuid_col': None}])
    success, msg = inplace_pipe.sync(debug=debug)
    assert success, msg
    df = inplace_pipe.get_data(params={'id': 3})
    assert df['uuid_col'][0] is None


@pytest.mark.parametrize("flavor", get_flavors())
def test_sync_uuids_simple_upsert(flavor: str):
    """
    Testing syncing UUIDs normally.
    """
    conn = conns[flavor]
    pipe = mrsm.Pipe('test', 'uuids', 'upsert', instance=conn)
    pipe.delete()
    pipe = mrsm.Pipe(
        'test', 'uuids', 'upsert',
        instance=conn,
        columns=['datetime', 'id'],
        parameters={
            'upsert': False,
        },
    )
    docs = [
        {
            'datetime': '2024-01-01',
            'id': UUID('7d78f4a7-8c0d-4cc3-9636-9516aa0c32ce'),
            'val': UUID('603e2509-8c53-4942-8395-4c8775455df1'),
        },
        {
            'datetime': '2024-01-02',
            'id': UUID('07557810-5662-449d-b0da-8360fe6134fe'),
            'val': UUID('de8b4f65-1bbb-41ac-9572-be637a647349'),
        },
    ]
    success, msg = pipe.sync(docs, debug=debug)
    assert success, msg

    upsert_docs = [
        {
            'datetime': '2024-01-01',
            'id': UUID('7d78f4a7-8c0d-4cc3-9636-9516aa0c32ce'),
            'val': UUID('603e2509-8c53-4942-8395-4c8775455df1'),
        },
        {
            'datetime': '2024-01-02',
            'id': UUID('07557810-5662-449d-b0da-8360fe6134fe'),
            'val': UUID('d1cc1516-16e5-4471-8ab9-e969a1def655'),
        },
    ]
    success, msg = pipe.sync(upsert_docs, debug=debug)
    assert pipe.get_rowcount(debug=debug) == len(docs)
    df = pipe.get_data(params={'id': UUID('07557810-5662-449d-b0da-8360fe6134fe')})
    assert len(df) == 1
    assert isinstance(df['val'][0], UUID)
    assert df['val'][0] == UUID('d1cc1516-16e5-4471-8ab9-e969a1def655')
