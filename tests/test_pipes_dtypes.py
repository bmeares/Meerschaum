#! /usr/bin/env python
# -*- coding: utf-8 -*-
# vim:fenc=utf-8

import pytest
import warnings
from datetime import datetime, timezone
from decimal import Decimal
from uuid import UUID
from tests import debug
from tests.connectors import conns, get_flavors
import meerschaum as mrsm
from meerschaum import Pipe
from meerschaum.utils.dtypes import are_dtypes_equal
from meerschaum.utils.sql import sql_item_name
from meerschaum.utils.dtypes.sql import PD_TO_DB_DTYPES_FLAVORS


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
        static=False,
        upsert=True, ### TODO: Test with `upsert=True`.
        enforce=True,
        columns={
            'datetime': 'dt',
            'id': 'id',
        },
        dtypes={
            'id': 'int',
            'int': 'int',
            'float': 'float',
            'bool': 'bool',
            'object': 'object',
            'json': 'json',
            'numeric': 'numeric',
            'str': 'str',
            'uuid': 'uuid',
            'bytes': 'bytes',
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
    pipe.sync([{'dt': '2022-01-01', 'id': 1, 'bytes': 'Zm9vIGJhcg=='}], debug=debug)
    df = pipe.get_data(debug=debug)
    assert len(df) == 1
    assert len(df.columns) == 11
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
        elif pipe_dtype == 'bytes':
            assert isinstance(df[col][0], bytes)
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
    pipe = Pipe('foo', 'bar', session_id, instance=conn, columns=['id'])
    success, msg = pipe.sync([
        {'id': 1, 'a': ['b', 'c']},
        {'id': 2, 'a': {'b': 1}},
    ])
    assert success, msg
    pprint(pipe.get_columns_types())
    df = pipe.get_data(debug=debug)
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
    pipe = Pipe('foo', 'bar', session_id, instance=conn, dtypes={'a': 'json'}, columns=['id'])
    success, msg = pipe.sync([
        {'id': 1, 'a': json.dumps(['b', 'c'])},
        {'id': 2, 'a': json.dumps({'b': 1})},
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
        flavor, NUMERIC_PRECISION_FLAVORS['mssql']
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
    pipe = Pipe('infer', 'numeric', instance=conn, columns=['id'])
    success, msg = pipe.sync([
        {'id': 1, 'a': Decimal('1')},
        {'id': 2, 'a': Decimal(numeric_str)},
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
    pipe = Pipe('infer', 'uuid', instance=conn, columns=['id'])
    uuid_str = "e6f3a4ea-f1af-4e93-8da9-716b57672206"
    success, msg = pipe.sync(
        [
            {'id': 1, 'a': UUID(uuid_str)},
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
def test_infer_bytes_dtype(flavor: str):
    """
    Ensure that `bytes` are persisted as `bytes`.
    """
    from meerschaum.utils.formatting import pprint
    conn = conns[flavor]
    pipe = Pipe('infer', 'bytes', instance=conn)
    _ = pipe.delete(debug=debug)
    pipe = Pipe('infer', 'bytes', instance=conn, columns=['id'])
    bytes_data = b'foo bar'
    success, msg = pipe.sync(
        [
            {'id': 1, 'a': bytes_data},
        ],
        debug=debug,
    )
    assert success, msg
    pprint(pipe.get_columns_types())
    df = pipe.get_data(debug=debug)
    print(df)
    assert isinstance(df['a'][0], bytes)
    assert df['a'][0] == bytes_data


@pytest.mark.parametrize("flavor", get_flavors())
def test_infer_numeric_from_mixed_types(flavor: str):
    """
    Ensure that new pipes with mixed int and floats as enforced as NUMERIC. 
    """
    from meerschaum.utils.formatting import pprint
    from meerschaum.utils.misc import generate_password
    conn = conns[flavor]
    pipe = Pipe('test', 'infer_numeric', 'mixed', instance=conn)
    _ = pipe.delete(debug=debug)
    pipe = Pipe('test', 'infer_numeric', 'mixed', instance=conn, columns=['id'])
    success, msg = pipe.sync([{'id': 1, 'a': 1}], debug=debug)
    assert success, msg
    success, msg = pipe.sync([{'id': 2, 'a': 2.1}], debug=debug)
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
    pipe = Pipe('foo', 'bar', session_id, instance=conn, dtypes={'a': 'numeric'}, columns=['id'])
    success, msg = pipe.sync([
        {'id': 1, 'a': '1'},
        {'id': 2, 'a': '2.1'},
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
    pipe = Pipe('test_utc_offset', 'datetimes', instance=conn)
    pipe.delete()
    pipe = Pipe(
        'test_utc_offset', 'datetimes',
        instance=conn,
        columns={'datetime': 'dt'},
    )

    seed_docs = [
        {'dt': datetime(2023, 1, 1)},
        {'dt': datetime(2023, 1, 1, 23, 0, 0)},
    ]
    success, msg = pipe.sync(seed_docs, debug=debug)
    assert success, msg

    docs = [
        {'dt': '2023-01-01 00:00:00+00:00'},
        {'dt': '2023-01-02 00:00:00+01:00'},
    ]

    expected_docs = [
        {'dt': datetime(2023, 1, 1, tzinfo=timezone.utc)},
        {'dt': datetime(2023, 1, 1, 23, 0, 0, tzinfo=timezone.utc)}
    ]

    success, msg = pipe.sync(docs, debug=debug)
    assert success, msg
    df = pipe.get_data(debug=debug)
    synced_docs = df.to_dict(orient='records')
    mrsm.pprint(synced_docs)
    mrsm.pprint(expected_docs)
    assert synced_docs == expected_docs


@pytest.mark.parametrize("flavor", get_flavors())
def test_explicit_utc_datetimes(flavor: str):
    """
    Verify that we are able to sync rows with UTC offset datetimes.
    """
    conn = conns[flavor]
    pipe = Pipe('test_explicit', 'datetimes', 'utc', instance=conn)
    pipe.delete()
    pipe = Pipe(
        'test_explicit', 'datetimes', 'utc',
        instance=conn,
        columns={'datetime': 'dt'},
        dtypes={'dt': 'datetime64[ns, UTC]'},
    )

    docs = [
        {'dt': '2024-01-01 00:00:00+00:00'},
        {'dt': '2024-01-02 00:00:00+01:00'},
    ]

    expected_docs = [
        {'dt': datetime(2024, 1, 1, tzinfo=timezone.utc)},
        {'dt': datetime(2024, 1, 1, 23, 0, 0, tzinfo=timezone.utc)},
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
    pipe = Pipe('test_utc_offset', 'datetimes', 'ignore', instance=conn)
    pipe.delete()

    pipe = Pipe(
        'test_utc_offset', 'datetimes', 'ignore',
        instance=conn,
        dtypes={'dt': 'str'},
    )

    docs = [
        {'dt': '2023-01-01 00:00:00+00:00'},
        {'dt': '2023-01-02 00:00:00+01:00'},
    ]

    expected_docs = [
        {'dt': '2023-01-01 00:00:00+00:00'},
        {'dt': '2023-01-02 00:00:00+01:00'},
    ]

    with warnings.catch_warnings():
        warnings.filterwarnings('ignore')
        success, msg = pipe.sync(docs, debug=debug)

    assert success, msg
    df = pipe.get_data(['dt'], debug=debug)
    print(df)
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
    pipe = Pipe(
        'test_no_indices', 'datetimes', 'text',
        instance=conn,
    )

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
        instance=conn,
        columns={'datetime': 'dt'},
        dtypes={'is_bool': 'bool'},
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
    pipe = mrsm.Pipe('test', 'bools', 'inferred', instance=conn)
    _ = pipe.delete()
    pipe = mrsm.Pipe(
        'test', 'bools', 'inferred',
        instance=conn,
        columns={'datetime': 'dt'},
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
            'parents': [pipe.meta],
        },
    )
    _ = pipe.drop()
    docs = [
        {'dt': '2023-01-01', 'id': 1, 'is_bool': True},
        {'dt': '2023-01-02', 'id': 2, 'is_bool': False},
        {'dt': '2023-01-03', 'id': 3, 'is_bool': None},
    ]
    success, msg = pipe.sync(docs, debug=debug)
    assert success, msg

    success, msg = inplace_pipe.sync(debug=debug)
    assert success, msg

    assert 'bool' in inplace_pipe.dtypes['is_bool']
    assert inplace_pipe.get_rowcount() == len(docs)

    pipe.sync([{'dt': '2023-01-03', 'id': 3, 'is_bool': True}], debug=debug)
    success, msg = inplace_pipe.sync(debug=debug)
    assert success, msg

    df = pipe.get_data(params={'id': 3}, debug=debug)
    assert 'true' in str(df['is_bool'][0]).lower()

    df = inplace_pipe.get_data(params={'id': 3}, debug=debug)
    assert 'true' in str(df['is_bool'][0]).lower()

    pipe.sync([{'dt': '2023-01-03', 'id': 3, 'is_bool': None}], debug=debug)
    success, msg = inplace_pipe.sync(debug=debug)
    assert success, msg
    df = inplace_pipe.get_data(params={'id': 3}, debug=True)
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
    success, msg = pipe.sync(docs, debug=debug)
    assert success, msg

    success, msg = inplace_pipe.sync(debug=debug)
    assert success, msg

    assert 'uuid' in inplace_pipe.dtypes['uuid_col']
    assert inplace_pipe.get_rowcount() == len(docs)
    db_col = inplace_pipe.get_columns_types(refresh=True)['uuid_col']
    if flavor in PD_TO_DB_DTYPES_FLAVORS['uuid']:
        uuid_typ = PD_TO_DB_DTYPES_FLAVORS['uuid'][flavor]
        assert db_col.split('(',  maxsplit=1)[0] == uuid_typ.split('(', maxsplit=1)[0]

    update_uuid = UUID('7befa9bd-fbb3-404b-b8e1-6389db6b6e84')
    pipe.sync([{'dt': '2023-01-03', 'id': 3, 'uuid_col': update_uuid}], debug=debug)
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
def test_sync_bytes_inplace(flavor: str):
    """
    Test that pipes are able to sync bytes in-place.
    """
    conn = conns[flavor]
    if conn.type not in ('api', 'sql'):
        return
    pipe = mrsm.Pipe('test', 'bytes', 'inplace', instance=conn)
    _ = pipe.delete()
    pipe = mrsm.Pipe(
        'test', 'bytes', 'inplace',
        instance=conn,
        columns={'datetime': 'dt', 'id': 'id'},
    )
    pipe_table = sql_item_name(pipe.target, conn.flavor) if conn.type == 'sql' else pipe.target
    inplace_pipe = mrsm.Pipe(conn, 'bytes', 'inplace', instance=conn)
    _ = inplace_pipe.delete()
    inplace_pipe = mrsm.Pipe(
        conn, 'bytes', 'inplace',
        instance=conn,
        columns=pipe.columns,
        dtypes={
            'bytes_col': 'bytes',
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
        {'dt': '2024-01-01', 'id': 1, 'bytes_col': b'foo bar'},
        {'dt': '2024-01-02', 'id': 2, 'bytes_col': b'do re mi'},
        {'dt': '2024-01-03', 'id': 3},
    ]
    success, msg = pipe.sync(docs, debug=debug)
    assert success, msg

    success, msg = inplace_pipe.sync(debug=debug)
    assert success, msg

    assert 'bytes' in inplace_pipe.dtypes['bytes_col']
    assert inplace_pipe.get_rowcount() == len(docs)

    df = inplace_pipe.get_data()
    assert df['bytes_col'][0] == b'foo bar'
    assert df['bytes_col'][1] == b'do re mi'
    assert df['bytes_col'][2] is None


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


@pytest.mark.parametrize("flavor", get_flavors())
def test_mixed_timezone_aware_and_naive(flavor: str):
    conn = conns[flavor]
    if conn.type != 'sql':
        return

    pd = mrsm.attempt_import('pandas')
    target = 'test_timezone_mix'
    pipe = mrsm.Pipe('test', 'timezone', 'aware_naive', instance=conn)
    pipe.delete()
    pipe = mrsm.Pipe(
        'test', 'timezone', 'aware_naive',
        instance=conn,
        columns={'datetime': 'ts'},
        target=target,
    )

    src_df = pd.DataFrame([{'ts': datetime(2024, 1, 1), 'val': 2}])
    conn.to_sql(src_df, target, debug=debug)

    df = pipe.get_data(debug=debug)
    unseen, update, delta = pipe.filter_existing(src_df, debug=debug)
    assert len(unseen) == 0
    assert len(update) == 0
    assert len(delta) == 0

    success, msg = pipe.sync([{'ts': '2024-01-01 05:00:00', 'val': 3}])
    assert success, msg

    df = pipe.get_data(begin='2024-01-01 05:00:00', debug=debug)
    assert len(df) == 1
    assert df['val'][0] == 3

    success, msg = pipe.sync([{'ts': '2024-01-01 05:00:00+00:00', 'val': 4}])
    assert success, msg

    df = pipe.get_data(begin='2024-01-01 05:00:00+00:00', debug=debug)
    assert len(df) == 1
    assert df['val'][0] == 4


@pytest.mark.parametrize("flavor", get_flavors())
def test_parse_date_bounds(flavor: str):
    """
    Test that datetime bounds are parsed correctly.
    """
    conn = conns[flavor]
    pipe = mrsm.Pipe('test', 'parse_date_bounds', 'tz', instance=conn)
    pipe.delete()
    pipe = mrsm.Pipe('test', 'parse_date_bounds', 'tz', instance=conn, columns={'datetime': 'ts'})
    success, msg = pipe.sync([{'ts': '2024-01-01'}], debug=debug)
    assert success, msg

    success, msg = pipe.sync([{'ts': '2024-01-02'}], debug=debug)
    assert success, msg

    naive_begin = datetime(2024, 1, 1)
    naive_end = datetime(2024, 1, 2)
    aware_begin = datetime(2024, 1, 1, tzinfo=timezone.utc)
    aware_end = datetime(2024, 2, 1, tzinfo=timezone.utc)

    begin, end = pipe.parse_date_bounds(naive_begin, naive_end)
    assert begin.tzinfo is not None
    assert end.tzinfo is not None
    begin, end = pipe.parse_date_bounds(aware_begin, aware_end)
    assert begin.tzinfo is not None
    assert end.tzinfo is not None
    begin, end = pipe.parse_date_bounds(naive_begin, aware_end)
    assert begin.tzinfo is not None
    assert end.tzinfo is not None

    pipe = mrsm.Pipe('test', 'parse_date_bounds', 'naive', instance=conn)
    pipe.delete()
    pipe = mrsm.Pipe(
        'test', 'parse_date_bounds', 'naive',
        instance=conn,
        columns={'datetime': 'ts'},
        dtypes={'ts': 'datetime64[ns]'},
    )

    success, msg = pipe.sync([{'ts': '2024-01-01'}], debug=debug)
    assert success, msg

    success, msg = pipe.sync([{'ts': '2024-01-02'}], debug=debug)
    assert success, msg

    begin, end = pipe.parse_date_bounds(naive_begin, naive_end)
    assert begin.tzinfo is None
    assert end.tzinfo is None
    begin, end = pipe.parse_date_bounds(aware_begin, aware_end)
    assert begin.tzinfo is None
    assert end.tzinfo is None
    begin, end = pipe.parse_date_bounds(naive_begin, aware_end)
    assert begin.tzinfo is None
    assert end.tzinfo is None


@pytest.mark.parametrize("flavor", get_flavors())
def test_distant_datetimes(flavor: str):
    """
    Test that extremely distant datetimes may be synced.
    """
    conn = conns[flavor]
    pipe = mrsm.Pipe('test', 'datetimes', 'distant', instance=conn)
    pipe.delete()
    pipe = mrsm.Pipe(
        'test', 'datetimes', 'distant',
        instance=conn,
        columns={
            'datetime': 'ts',
        },
        enforce=False,
    )
    docs = [
        {'ts': datetime(1, 1, 1)},
    ]
    success, msg = pipe.sync(docs, debug=debug)
    assert success, msg

    df = pipe.get_data()
    print(f"df=\n{df}")
    assert df['ts'][0].year == 1


@pytest.mark.parametrize("flavor", get_flavors())
def test_enforce_false(flavor: str):
    """
    Test `enforce=False` behavior.
    """ 
    conn = conns[flavor]
    pipe = mrsm.Pipe('test', 'enforce', instance=conn)
    pipe.delete()
    pipe = mrsm.Pipe(
        'test', 'enforce',
        instance=conn,
        enforce=False,
        columns={
            'datetime': 'dt',
        },
        dtypes={
            'num': 'decimal',
        },
    )
    docs = [
        {'dt': datetime(2024, 12, 26, tzinfo=timezone.utc), 'num': Decimal('1.21')},
    ]
    success, msg = pipe.sync(docs, debug=debug)
    assert success, msg

    df = pipe.get_data(['num'], debug=debug)
    assert df['num'][0] == Decimal('1.21')
    
    new_docs = [
        {'dt': datetime(2024, 12, 26, tzinfo=timezone.utc), 'num': Decimal('2.34567')},
    ]
    success, msg = pipe.sync(new_docs, debug=debug)
    assert success, msg
    df = pipe.get_data(debug=debug)
    assert len(df) == 1
    assert df['dt'][0].tzinfo is not None
    assert df['num'][0] == Decimal('2.34567')
