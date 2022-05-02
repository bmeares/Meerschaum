#! /usr/bin/env python
# -*- coding: utf-8 -*-
# vim:fenc=utf-8

import pytest
import datetime
from tests import debug
from tests.pipes import all_pipes, stress_pipes, remote_pipes
from tests.connectors import conns
from tests.test_users import test_register_user
from meerschaum import Pipe
from meerschaum.actions import actions

@pytest.fixture(autouse=True)
def run_before_and_after(flavor: str):
    test_register_user(flavor)
    yield

@pytest.mark.parametrize("flavor", list(all_pipes.keys()))
def test_register_and_delete(flavor: str):
    pipes = all_pipes[flavor]
    for pipe in pipes:
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
    pipes = all_pipes[flavor]
    for pipe in pipes:
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
    pipes = all_pipes[flavor]
    for pipe in pipes:
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

#  @pytest.mark.skip(reason="")
@pytest.mark.parametrize("flavor", list(remote_pipes.keys()))
def test_drop_and_sync_remote(flavor: str):
    pipes = remote_pipes[flavor]
    for pipe in pipes:
        #  pipe.drop(debug=debug)
        parent_pipe = Pipe('plugin:stress', 'test', instance=pipe.connector)
        if not parent_pipe.exists(debug=debug):
            success, msg = parent_pipe.sync(debug=debug)
            assert success, msg
        success, msg = pipe.sync(debug=debug)
        assert success, msg

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


#  @oytest.mark.parametrize("flavor", list(all_pipes.keys()))
#  def test_target(flavor: str):


