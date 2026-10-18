#! /usr/bin/env python3
# -*- coding: utf-8 -*-
# vim:fenc=utf-8

"""
A plugin action whose name contains `_` is reached both as `mrsm foo_bar` and as `mrsm foo bar`
(v4's `get_action`), including v4's quirks. The expected lines were recorded from v4.1.4.
"""

import pytest

from tests._plugin_cli import marked_line, run_mrsm

_PLUGIN = """
from meerschaum.plugins import make_action


@make_action
def w17bx_hello(**kw):
    '''Underscore action.'''
    print('ARGS', kw.get('action'), kw.get('sysargs'))
    return True, 'hello ok'


@make_action
def show_w17b(**kw):
    '''Collides with the built-in `show`.'''
    print('ARGS', kw.get('action'))
    return True, 'show_w17b ok'


@make_action
def w17bx(action=None, **kw):
    '''Plain action named like the plugin.'''
    print('ARGS', kw.get('action'))
    return True, 'w17bx ok'
"""


@pytest.mark.parametrize(
    'args, expected',
    [
        (['w17bx_hello'], "ARGS ['CORE>hello'] ['w17bx_hello']"),
        (['w17bx', 'hello'], "ARGS [] ['w17bx', 'hello']"),
        (['w17bx_hello', 'extra'], "ARGS ['CORE>hello', 'extra'] ['w17bx_hello', 'extra']"),
        (['w17bx', 'hello', 'extra'], "ARGS ['extra'] ['w17bx', 'hello', 'extra']"),
        (['w17bx', 'hellofoo'], "ARGS ['oo'] ['w17bx', 'hellofoo']"),
        (['w17bx_hello_more', 'x'], "ARGS ['CORE>hello_more', 'x'] ['w17bx_hello_more', 'x']"),
        (['show_w17b'], "ARGS ['SCORE>w17b']"),
        (['show', 'w17b'], "ARGS []"),
        (['w17bx'], "ARGS None"),
        (['w17bx', 'foo'], "ARGS None"),
    ],
)
def test_plugin_action_names_resolve_like_v4(tmp_path, args, expected):
    result = run_mrsm(tmp_path, {'w17bx.py': _PLUGIN}, args)
    assert marked_line(result, 'ARGS') == expected, result.stdout + result.stderr
    assert result.returncode == 0, result.stdout + result.stderr


def test_a_word_that_is_not_an_action_runs_as_a_shell_command(tmp_path):
    result = run_mrsm(tmp_path, {'w17bx.py': _PLUGIN}, ['w17bx_hellofoo'])
    assert 'ARGS' not in result.stdout
    assert 'command not found' in result.stdout + result.stderr
