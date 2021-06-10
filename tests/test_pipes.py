#! /usr/bin/env python
# -*- coding: utf-8 -*-
# vim:fenc=utf-8

import pytest
import datetime
from tests.pipes import all_pipes, stress_pipes, remote_pipes

@pytest.fixture(autouse=True)
def run_before_and_after():
    yield

#  @pytest.mark.parametrize("pipe", [sqlite_pipe, timescale_pipe, api_pipe])
def test_register_and_delete():
    for _label, pipes in all_pipes.items():
        for p in pipes:
            output = p.delete()
            assert p.register()
            assert p.delete()
            assert p.register()

#  @pytest.mark.parametrize("pipe", [sqlite_pipe, timescale_pipe, api_pipe])
def test_drop_and_sync():
    for _label, pipes in all_pipes.items():
        for p in pipes:
            assert p.drop()
            now1 = datetime.datetime.utcnow()
            d = {'datetime' : [now1], 'id' : [1], 'val': [1]}
            assert p.sync(d)
            now2 = datetime.datetime.utcnow()
            d = {'datetime' : [now2], 'id' : [1], 'val': [1]}
            assert p.sync(d)
            data = p.get_data()
            assert len(data) == 2

def test_drop_and_sync_duplicate():
    for _label, pipes in all_pipes.items():
        for p in pipes:
            assert p.drop()
            now1 = datetime.datetime.utcnow()
            d = {'datetime' : [now1], 'id' : [1], 'val': [1]}
            assert p.sync(d)
            d = {'datetime' : [now1], 'id' : [1], 'val': [1]}
            assert p.sync(d)
            data = p.get_data()
            assert len(data) == 2

def test_drop_and_sync_stress():
    for _label, pipes in stress_pipes.items():
        for p in pipes:
            assert p.drop()
            sync_tuple = p.sync()
            assert sync_tuple[0]

def test_drop_and_sync_remote():
    for _label, pipes in remote_pipes.items():
        for p in pipes:
            assert p.drop()
            sync_tuple = p.sync()
            assert sync_tuple[0]
