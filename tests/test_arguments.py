#! /usr/bin/env python3
# -*- coding: utf-8 -*-
# vim:fenc=utf-8

"""
Test argument parsing.
"""

import pytest

from meerschaum._internal.arguments import (
    parse_arguments,
    split_pipeline_sysargs,
    split_chained_sysargs,
)

@pytest.mark.parametrize(
    "sysargs,expected_kwargs", [
        (
            ['show', 'pipes', '--debug'],
            {
                'action': ['show', 'pipes'],
                'debug': True,
            }
        ),
        (
            ['sing', 'song', '--loop', '--min-seconds', '0'],
            {
                'action': ['sing', 'song'],
                'loop': True,
                'min_seconds': 0,
            },
        ),
        (
            ['foo', ':', '-s', 'every 3 seconds'],
            {
                'action': ['foo', ':'],
                'schedule': 'every 3 seconds',
            },
        ),
    ]
)
def test_parse_sysargs(sysargs, expected_kwargs):
    """
    Test parsing sysargs into an args dictionary.
    """
    kwargs = parse_arguments(sysargs)
    for key, val in expected_kwargs.items():
        assert key in kwargs
        assert kwargs[key] == val


@pytest.mark.parametrize(
    "sysargs,expected_sysargs,expected_pipeline_args", [
        (
            ['show', 'pipes', '--debug'],
            ['show', 'pipes', '--debug'],
            [],
        ),
        (
            ['foo', ':', '-s', 'every 3 seconds'],
            ['foo'],
            ['-s', 'every 3 seconds'],
        ),
        (
            ['sing', 'song', ':', '--min-seconds', '0'],
            ['sing', 'song'],
            ['--min-seconds', '0'],
        ),
        (
            ['blue', '::', '-s', 'daily', ':', '--loop'],
            ['blue', '::', '-s', 'daily'],
            ['--loop'],
        ),
        (
            ['a', ':', 'b', ':', 'c'],
            ['a', 'b'],
            ['c'],
        ),
    ]
)
def test_split_pipeline_sysargs(sysargs, expected_sysargs, expected_pipeline_args):
    """
    Test splitting sysargs into pipeline args.
    """
    new_sysargs, pipeline_args = split_pipeline_sysargs(sysargs)
    assert new_sysargs == expected_sysargs
    assert pipeline_args == expected_pipeline_args


@pytest.mark.parametrize(
    "sysargs,expected_chained_sysargs", [
        (
            ['show', 'pipes', '+', 'show', 'version'],
            [['show', 'pipes'], ['show', 'version']],
        ),
        (
            ['show', 'users', '+', 'sync', 'pipes', '+', 'show', 'version', ':', '-s', 'daily'],
            [['show', 'users'], ['sync', 'pipes'], ['show', 'version', ':', '-s', 'daily']],
        ),
        (
            ['show', 'version'],
            [['show', 'version']],
        ),
    ]
)
def test_split_chained_sysargs(sysargs, expected_chained_sysargs):
    """
    Test splitting chained sysargs into individual lines.
    """
    chained_sysargs = split_chained_sysargs(sysargs)
    assert chained_sysargs == expected_chained_sysargs
