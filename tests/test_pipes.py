#! /usr/bin/env python
# -*- coding: utf-8 -*-
# vim:fenc=utf-8

import pytest
import datetime
from tests import debug
from tests.pipes import all_pipes, stress_pipes, remote_pipes
from tests.connectors import conns
from tests.test_users import test_register_user
import meerschaum as mrsm
from meerschaum import Pipe
from meerschaum.actions import actions

@pytest.fixture(autouse=True)
def run_before_and_after(flavor: str):
    test_register_user(flavor)
    yield


@pytest.mark.parametrize("flavor", list(all_pipes.keys()))
def test_register_and_delete(flavor: str):
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

@pytest.mark.parametrize("flavor", list(all_pipes.keys()))
def test_drop_and_sync(flavor: str):
    pipe = all_pipes[flavor][0]
    pipe.drop()
    assert pipe.exists(debug=debug) is False
    assert pipe.columns is not None
    now1 = datetime.datetime(2021, 1, 1, 12, 0)
    data = {'datetime' : [now1], 'id' : [1], 'val': [1]}
    success, msg = pipe.sync(data, debug=debug)
    assert success, msg
    assert pipe.exists(debug=debug)
    now2 = datetime.datetime(2021, 1, 1, 12, 1)
    data = {'datetime' : [now2], 'id' : [1], 'val': [1]}
    success, msg = pipe.sync(data, debug=debug)
    assert success, msg
    assert pipe.exists(debug=debug)
    data = pipe.get_data(debug=debug)
    assert data is not None
    assert len(data) == 2

@pytest.mark.parametrize("flavor", list(all_pipes.keys()))
def test_drop_and_sync_duplicate(flavor: str):
    pipe = all_pipes[flavor][0]
    pipe.drop(debug=debug)
    assert not pipe.exists(debug=debug)

    now1 = datetime.datetime(2021, 1, 1, 12, 0)
    data = {'datetime': [now1], 'id': [1], 'val': [1]}
    success, msg = pipe.sync(data, debug=debug)
    assert success, msg
    data = pipe.get_data(debug=debug)
    assert len(data) == 1

    now1 = datetime.datetime(2021, 1, 1, 12, 0)
    data = {'datetime': [now1], 'id': [1], 'val': [1]}
    success, msg = pipe.sync(data, debug=debug)
    assert success, msg
    data = pipe.get_data(debug=debug)
    assert len(data) == 1

@pytest.mark.parametrize("flavor", list(stress_pipes.keys()))
def test_drop_and_sync_stress(flavor: str):
    pipes = stress_pipes[flavor]
    for pipe in pipes:
        pipe.drop(debug=debug)
        success, msg = pipe.sync(debug=debug)
        assert success, msg

@pytest.mark.parametrize("flavor", list(remote_pipes.keys()))
def test_drop_and_sync_remote(flavor: str):
    pipe = None
    for p in remote_pipes[flavor]:
        if str(p.connector) == str(p.instance_connector):
            pipe = p
            break
    assert pipe is not None
    pipe.drop(debug=debug)
    parent_pipe = Pipe('plugin:stress', 'test', instance=pipe.connector)
    parent_pipe.drop(debug=debug)
    success, msg = parent_pipe.sync(debug=debug)
    parent_len = parent_pipe.get_rowcount(debug=debug)
    assert success, msg
    success, msg = pipe.sync(debug=debug)
    assert success, msg
    child_len = pipe.get_rowcount(debug=debug)
    assert parent_len == child_len

    success, msg = parent_pipe.sync(
        [{'datetime': '2100-01-01', 'id': 999, 'foo': 'bar'}],
        debug = debug,
    )
    assert success, msg
    parent_len2 = parent_pipe.get_rowcount(debug=debug)
    assert parent_len2 == (parent_len + 1)
    success, msg = pipe.sync(debug=debug)
    assert len(pipe.get_columns_types(debug=debug)) == 4
    child_len2 = pipe.get_rowcount(debug=debug)
    assert parent_len2 == child_len2
    success, msg = parent_pipe.sync(
        [{'datetime': '2100-01-01', 'id': 999, 'foo': 'baz'}],
        debug = debug,
    )
    assert success, msg
    parent_len3 = parent_pipe.get_rowcount(debug=debug)
    assert parent_len2 == parent_len3
    success, msg = pipe.sync(debug=debug, begin='2020-01-01')
    assert success, msg
    child_len3 = pipe.get_rowcount(debug=debug)
    assert child_len3 == parent_len3
    df = pipe.get_data(params={'id': 999}, debug=debug)
    assert len(df) == 1
    assert df.to_dict(orient='records')[0]['foo'] == 'baz'


@pytest.mark.parametrize("flavor", list(all_pipes.keys()))
def test_sync_engine(flavor: str):
    ### Weird concurrency issues with our tests.
    if flavor == 'duckdb':
        return
    pipes = stress_pipes[flavor]
    mrsm_instance = str(pipes[0].instance_connector)
    success, msg = actions['drop'](
        ['pipes'],
        connector_keys = [p.connector_keys for p in pipes],
        metric_keys = [p.metric_key for p in pipes],
        location_keys = [p.location_key for p in pipes],
        mrsm_instance = mrsm_instance,
        yes = True,
        #  debug = True,
    )
    assert success, msg

    success, msg = actions['sync'](
        ['pipes'],
        connector_keys = [p.connector_keys for p in pipes],
        metric_keys = [p.metric_key for p in pipes],
        location_keys = [p.location_key for p in pipes],
        mrsm_instance = mrsm_instance,
        #  debug = True,
    )
    assert success, msg


@pytest.mark.parametrize("flavor", list(all_pipes.keys()))
def test_target_mutable(flavor: str):
    conn = conns[flavor]
    if conn.type != 'sql':
        return
    target = 'FooBar!'
    pipe = Pipe('foo', 'bar', target=target, instance=conn, columns={'datetime': 'dt', 'id': 'id'})
    pipe.drop(debug=debug)
    assert not pipe.exists(debug=debug)
    success, msg = pipe.sync(
        {'dt': [datetime.datetime(2022, 6, 8)], 'id': [1], 'vl': [10]},
        debug = debug
    )
    df = conn.read(target)
    assert len(df) == 1
    success, msg = pipe.sync(
        {'dt': [datetime.datetime(2022, 6, 8)], 'id': [1], 'vl': [10]},
        debug = debug
    )
    df = conn.read(target)
    assert len(df) == 1
    success, msg = pipe.sync(
        {'dt': [datetime.datetime(2022, 6, 8)], 'id': [1], 'vl': [100]},
        debug = debug
    )
    df = conn.read(target)
    assert len(df) == 1
    pipe.drop()
    result = conn.read(target, silent=True)
    assert result is None


@pytest.mark.parametrize("flavor", list(remote_pipes.keys()))
def test_sync_new_columns(flavor: str):
    """
    Test that new columns are added.
    """
    conn = conns[flavor]
    pipe = Pipe('foo', 'bar', columns={'datetime': 'dt', 'id': 'id'}, instance=conn)
    pipe.drop(debug=debug)
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


@pytest.mark.parametrize("flavor", list(remote_pipes.keys()))
def test_sync_change_columns_dtypes(flavor: str):
    """
    Test that new columns are added.
    """
    conn = conns[flavor]
    pipe = Pipe('foo', 'bar', columns={'datetime': 'dt', 'id': 'id'}, instance=conn)
    pipe.drop(debug=debug)
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
    print(df)
    assert str(df.dtypes['a']) == 'object'


@pytest.mark.parametrize("flavor", list(remote_pipes.keys()))
def test_dtype_enforcement(flavor: str):
    """
    Test that incoming rows are enforced to the correct data type.
    """
    conn = conns[flavor]
    pipe = Pipe(
        'foo', 'bar',
        columns = {
            'datetime': 'dt',
            'id': 'id',
        },
        dtypes = {
            'int': 'Int64',
            'float': 'float64',
            'bool': 'bool',
            'object': 'object',
            'json': 'json',
            'str': 'str',
        },
        instance = conn,
    )
    pipe.drop(debug=debug)
    pipe.sync([{'dt': '2022-01-01', 'id': 1, 'int': '1'}], debug=debug)
    pipe.sync([{'dt': '2022-01-01', 'id': 1, 'float': '1.0'}], debug=debug)
    pipe.sync([{'dt': '2022-01-01', 'id': 1, 'bool': 'True'}], debug=debug)
    pipe.sync([{'dt': '2022-01-01', 'id': 1, 'object': 'foo'}], debug=debug)
    pipe.sync([{'dt': '2022-01-01', 'id': 1, 'str': 'bar'}], debug=debug)
    pipe.sync([{'dt': '2022-01-01', 'id': 1, 'json': {'a': {'b': 1}}}], debug=debug)
    df = pipe.get_data(debug=debug)
    assert len(df) == 1
    assert len(df.columns) == 8
    for col, typ in df.dtypes.items():
        pipe_dtype = pipe.dtypes[col]
        if pipe_dtype == 'json':
            assert isinstance(df[col][0], dict)
            pipe_dtype = 'object'
        elif pipe_dtype == 'str':
            assert isinstance(df[col][0], str)
            pipe_dtype = 'object'
        assert str(typ) == pipe_dtype


@pytest.mark.parametrize("flavor", list(all_pipes.keys()))
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


@pytest.mark.parametrize("flavor", list(all_pipes.keys()))
def test_infer_json_dtype(flavor: str):
    """
    Ensure that new pipes with complex columns (dict or list) as enforced as JSON. 
    """
    from meerschaum.utils.formatting import pprint
    from meerschaum.utils.misc import generate_password
    session_id = generate_password(6)
    conn = conns[flavor]
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
    success, msg = pipe.delete(debug=debug)
    assert success, msg


@pytest.mark.parametrize("flavor", list(all_pipes.keys()))
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
    success, msg = pipe.delete(debug=debug)
    assert success, msg
