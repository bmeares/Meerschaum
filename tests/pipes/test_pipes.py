#! /usr/bin/env python
# -*- coding: utf-8 -*-
# vim:fenc=utf-8

import pytest
from meerschaum import Pipe, get_connector

sqlite_pipe = Pipe(
    'test', 'test', 'sqlite',
    mrsm_instance = get_connector('sql', 'test', flavor='sqlite', database='test.db')
)
timescale_pipe = Pipe('test', 'test', 'timescale', mrsm_instance='sql:main')
api_pipe = Pipe('test', 'test', 'api', mrsm_instance='api:local')

@pytest.fixture(autouse=True)
def run_before_and_after():
    yield

@pytest.mark.parametrize("pipe", [sqlite_pipe, timescale_pipe, api_pipe])
def test_register_and_delete(pipe):
    output = pipe.delete()
    assert pipe.register()
    assert pipe.delete()
    assert pipe.register()

@pytest.mark.parametrize("pipe", [sqlite_pipe, timescale_pipe, api_pipe])
def test_sync(pipe):
    import datetime
    pipe.columns = {'datetime' : 'dt', 'id' : 'id'}
    assert pipe.drop()
    d = {'dt' : [datetime.datetime.utcnow()], 'id' : [1]}
    assert pipe.sync(d)
    d2 = {'dt' : [datetime.datetime.utcnow()], 'id' : [1]}
    assert pipe.sync(d2)
    data = pipe.get_data()
    assert len(data) == 2
