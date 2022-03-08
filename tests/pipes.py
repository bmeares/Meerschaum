#! /usr/bin/env python3
# -*- coding: utf-8 -*-
# vim:fenc=utf-8

"""
Build pipes from the conns in the connectors module.
"""

from meerschaum import Pipe
from tests.connectors import conns

all_pipes = {}
stress_pipes = {}
remote_pipes = {}

for _label, instance in conns.items():
    all_pipes[_label], stress_pipes[_label], remote_pipes[_label] = [], [], []
    stress_pipe = Pipe(
        'plugin:stress', 'test',
        mrsm_instance = instance,
        parameters = {
            'columns': {
                'datetime': 'datetime' ,
                'id': 'id',
            },
            'fetch': {
                'rows': 100,
                'id': 3,
            },
        },
    )
    all_pipes[_label].append(stress_pipe)
    stress_pipes[_label].append(stress_pipe)
    for __label, conn in conns.items():
        if _label == __label:
            continue
        remote_pipe = Pipe(
            str(conn), 'test', None,
            mrsm_instance=instance,
            parameters={
                'columns': {'datetime': 'datetime', 'id': 'id'},
                'fetch': ({
                    'definition': (
                        'SELECT * FROM ' +
                        ('plugin_stress_test' if conn.flavor != 'oracle' else 'PLUGIN_STRESS_TEST')
                    )
                } if conn.type == 'sql' else {
                    'connector_keys': 'plugin:stress',
                    'metric_key': 'test',
                    'location_key': None,
                }),
            },
        )
        all_pipes[_label].append(remote_pipe)
        remote_pipes[_label].append(remote_pipe)
