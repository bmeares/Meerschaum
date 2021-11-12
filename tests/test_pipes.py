#! /usr/bin/env python
# -*- coding: utf-8 -*-
# vim:fenc=utf-8

import pytest
import datetime
from tests import debug
from tests.pipes import all_pipes, stress_pipes, remote_pipes
from tests.test_users import test_register_user
from meerschaum import Pipe

@pytest.fixture(autouse=True)
def run_before_and_after(flavor: str):
    test_register_user(flavor)
    yield

@pytest.mark.parametrize("flavor", list(all_pipes.keys()))
def test_register_and_delete(flavor: str):
    pipes = all_pipes[flavor]
    for pipe in pipes:
        cols = pipe.columns.copy()
        assert cols is not None
        output = pipe.delete()
        pipe.columns = cols
        assert pipe.columns is not None
        assert pipe.register(debug=debug)
        assert pipe.delete(debug=debug)
        assert pipe.register(debug=debug)
        assert pipe.columns is not None

@pytest.mark.parametrize("flavor", list(all_pipes.keys()))
def test_drop_and_sync(flavor: str):
    pipes = all_pipes[flavor]
    for pipe in pipes:
        pipe = Pipe(**pipe.meta)
        #  pipe.drop()
        #  assert pipe.exists(debug=debug) is False
        now1 = datetime.datetime(2021, 1, 1, 12, 0)
        data = {'datetime' : [now1], 'id' : [1], 'val': [1]}
        assert pipe.sync(data, debug=debug)
        assert pipe.exists(debug=debug)
        now2 = datetime.datetime(2021, 1, 1, 12, 1)
        data = {'datetime' : [now2], 'id' : [1], 'val': [1]}
        assert pipe.sync(data, debug=debug)
        assert pipe.exists(debug=debug)
        #  data = p.get_data(debug=debug)
        #  assert data is not None
        #  assert len(data) == 2

#  def test_drop_and_sync_duplicate():
    #  for _label, pipes in all_pipes.items():
        #  for p in pipes:
            #  assert p.drop()
            #  now1 = datetime.datetime.utcnow()
            #  d = {'datetime' : [now1], 'id' : [1], 'val': [1]}
            #  assert p.sync(d)
            #  d = {'datetime' : [now1], 'id' : [1], 'val': [1]}
            #  assert p.sync(d)
            #  data = p.get_data()
            #  assert len(data) == 1

#  def test_drop_and_sync_stress():
    #  for _label, pipes in stress_pipes.items():
        #  for p in pipes:
            #  assert p.drop()
            #  assert p.sync()

#  def test_drop_and_sync_remote():
    #  for _label, pipes in remote_pipes.items():
        #  for p in pipes:
            #  assert p.drop()
            #  assert p.sync()
