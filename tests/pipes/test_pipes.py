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
api_pipe = Pipe('test', 'test', 'test', mrsm_instance='api:local')

@pytest.mark.parametrize("pipe", [sqlite_pipe])

def test_register_pipe(pipe):
    output = pipe.register()
    if output[0] is False:
        pipe.delete()
        output = pipe.register()
    assert output

@pytest.mark.parametrize("pipe", [sqlite_pipe])
def test_delete_pipe(pipe):
    output = pipe.delete()
    assert output[0]


