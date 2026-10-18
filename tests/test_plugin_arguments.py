#! /usr/bin/env python3
# -*- coding: utf-8 -*-
# vim:fenc=utf-8

"""
The keyword arguments a plugin action receives from `add_plugin_argument` flags are exactly v4's:
`parse_arguments` pops every `False` and every `None` (a flag that was not given is absent), and the
string `'None'` becomes `None`. The expected dictionaries below were recorded from v4.1.4.

Plugins without a typed argument take the CLI's argument fast path (no interpreter is started to
parse them); a typed argument sends the whole set to the worker. Both must agree with v4.
"""

import json

import pytest

from tests._plugin_cli import marked_line, run_mrsm

_COMMON = """
add_plugin_argument('--m-true', action='store_true', help='t')
add_plugin_argument('--m-false', action='store_false', help='f')
add_plugin_argument('--m-def', default='dflt', help='d')
add_plugin_argument('--m-defbool', action='store_true', default=True, help='db')
add_plugin_argument('--m-star', nargs='*', help='ns')
add_plugin_argument('--m-plus', nargs='+', help='np')
add_plugin_argument('--m-opt', nargs='?', const='C', help='no')
add_plugin_argument('--m-two', nargs=2, help='n2')
add_plugin_argument('--m-count', action='count', help='c')
add_plugin_argument('--m-countdef', action='count', default=0, help='c')
add_plugin_argument('--m-append', action='append', help='a')
add_plugin_argument('--m-const', action='store_const', const=5, help='k')
add_plugin_argument('--m-choice', choices=['a', 'b'], help='ch')
add_plugin_argument('-q', '--m-short', action='store_true', help='s')
add_plugin_argument('--m-nonestr', default='None', help='nonestr')
add_plugin_argument('--m-bool-opt', action='store_true', default=False, help='dbf')
add_plugin_argument('--m-required-ish', default=None, help='dn')
add_plugin_argument('--m-bool-opt2', action='store_false', default=False, help='dbf2')
add_plugin_argument('--m-empty', default='', help='empty')
"""

_TYPED = """
add_plugin_argument('--m-int', type=int, help='i')
add_plugin_argument('--m-intdef', type=int, default=7, help='id')
add_plugin_argument('--m-float', type=float, default=1.5, help='fl')
add_plugin_argument('--m-strint', default='5', type=int, help='strint')
add_plugin_argument('--m-zero', type=int, default=0, help='zero')
"""

_ACTION = """

@make_action
def {name}(**kw):
    '''Matrix.'''
    import json
    out = {{k: kw[k] for k in sorted(kw) if k.startswith('m_') or k == 'short'}}
    print('KW', json.dumps(out, sort_keys=True))
    return True, 'ok'
"""


def _plugin(name: str, typed: bool) -> str:
    return (
        "from meerschaum.plugins import make_action, add_plugin_argument\n"
        + _COMMON
        + (_TYPED if typed else '')
        + _ACTION.format(name=name)
    )


_UNTYPED_ABSENT = {
    'm_countdef': 0, 'm_def': 'dflt', 'm_defbool': True, 'm_empty': '', 'm_false': True,
    'm_nonestr': None,
}
_UNTYPED_GIVEN_ARGS = [
    '--m-true', '--m-false', '--m-def', 'x', '--m-star', 'a', 'b', '--m-plus', 'c', '--m-opt',
    '--m-two', '1', '2', '--m-count', '--m-count', '--m-append', 'z', '--m-append', 'y',
    '--m-const', '--m-choice', 'a', '-q', '--m-nonestr', 'hi', '--m-bool-opt', '--m-bool-opt2',
    '--m-required-ish', 'v',
]
_UNTYPED_GIVEN = {
    'm_append': ['z', 'y'], 'm_bool_opt': True, 'm_choice': 'a', 'm_const': 5, 'm_count': 2,
    'm_countdef': 0, 'm_def': 'x', 'm_defbool': True, 'm_empty': '', 'm_nonestr': 'hi',
    'm_opt': 'C', 'm_plus': ['c'], 'm_required_ish': 'v', 'm_short': True,
    'm_star': ['a', 'b'], 'm_true': True, 'm_two': ['1', '2'],
}
_TYPED_ABSENT = {
    **_UNTYPED_ABSENT, 'm_float': 1.5, 'm_intdef': 7, 'm_strint': 5, 'm_zero': 0,
}
_TYPED_GIVEN_ARGS = _UNTYPED_GIVEN_ARGS + [
    '--m-int', '3', '--m-intdef', '9', '--m-float', '2', '--m-strint', '8', '--m-zero', '4',
]
_TYPED_GIVEN = {
    **_UNTYPED_GIVEN, 'm_int': 3, 'm_intdef': 9, 'm_float': 2.0, 'm_strint': 8, 'm_zero': 4,
}


def _keywords(tmp_path, name, typed, args):
    result = run_mrsm(tmp_path, {f'{name}.py': _plugin(name, typed)}, [name, *args])
    assert result.returncode == 0, result.stdout + result.stderr
    return json.loads(marked_line(result, 'KW ')[len('KW '):])


@pytest.mark.parametrize(
    'typed, args, expected',
    [
        (False, [], _UNTYPED_ABSENT),
        (False, _UNTYPED_GIVEN_ARGS, _UNTYPED_GIVEN),
        (True, [], _TYPED_ABSENT),
        (True, _TYPED_GIVEN_ARGS, _TYPED_GIVEN),
    ],
    ids=['fast-absent', 'fast-given', 'worker-absent', 'worker-given'],
)
def test_plugin_arguments_match_v4(tmp_path, typed, args, expected):
    name = 'w17bfast' if not typed else 'w17bmatrix'
    assert _keywords(tmp_path, name, typed, args) == expected


def test_a_flag_that_was_not_given_is_none_not_false(tmp_path):
    source = (
        "from meerschaum.plugins import make_action, add_plugin_argument\n"
        "add_plugin_argument('--w17b-flag', action='store_true', help='a flag')\n\n"
        "@make_action\n"
        "def w17bflag(**kw):\n"
        "    print('FLAG', repr(kw.get('w17b_flag')), 'w17b_flag' in kw)\n"
        "    return True, 'ok'\n"
    )
    result = run_mrsm(tmp_path, {'w17bflag.py': source}, ['w17bflag'])
    assert marked_line(result, 'FLAG ') == 'FLAG None False', result.stdout + result.stderr
    result = run_mrsm(tmp_path, {}, ['w17bflag', '--w17b-flag'])
    assert marked_line(result, 'FLAG ') == 'FLAG True True', result.stdout + result.stderr
