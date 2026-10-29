#! /usr/bin/env python3
# -*- coding: utf-8 -*-
# vim:fenc=utf-8

"""
`mrsm python file.py [args ...]` (rc15): every word after the file belongs to the script, as with
`python file.py ...`. The Python argument parser mirrors the Rust one
(`crates/meerschaum-cli/src/parse.rs::python_script_index`).
"""

import pytest

from meerschaum._internal.arguments._parse_arguments import (
    _python_script_index,
    parse_arguments,
    parse_dict_to_sysargs,
)


def test_the_words_after_the_file_are_the_scripts():
    script_words = ['--dry-run', '-y', '-i', 'sql:other', '-d', '[--z]', '-A', 'a b']
    ### Options may come before `python` and between `python` and the file.
    sysargs = ['--debug', 'python', '--venv', 'tools', '-i', 'sql:x', 's.py'] + script_words
    assert _python_script_index(sysargs) == (1, 6)
    args = parse_arguments(sysargs)
    assert args['action'] == ['python', 's.py'] + script_words
    assert args['venv'] == 'tools'
    assert args['mrsm_instance'] == 'sql:x'
    assert args['debug'] is True
    for key in ('dry_run', 'yes', 'daemon'):
        assert key not in args, key
    assert args['sub_args'] == []
    assert args['sysargs'] == sysargs
    assert args['filtered_sysargs'][-len(script_words):] == script_words


@pytest.mark.parametrize(
    'sysargs,index',
    [
        (['python', 'print(1)', '-y'], None),
        (['python', '--venv=tools', 's.py', '-y'], (0, 2)),
        (['python', '-isql:x', '[-u]', 's.pyw', '-y'], (0, 3)),
        (['python', '-A', 's.py', '-y'], None),
        (['python', '-c', 's.py'], None),
        (['sync', 's.py', '-y'], None),
        (['python'], None),
        (['python', '-m', 'a', 'b', '--debug', 's.py'], (0, 5)),
    ],
)
def test_only_a_python_file_splits_the_line(sysargs, index):
    assert _python_script_index(sysargs) == index


def test_an_existing_file_without_an_extension_is_a_script(tmp_path):
    script = tmp_path / 'run_me'
    script.write_text('print(1)\n')
    assert _python_script_index(['python', str(script), '--x']) == (0, 1)


def test_the_script_words_survive_the_round_trip():
    first = parse_arguments(['python', 's.py', '--x', 'a b', '-i', 'y'])
    kwargs = {k: v for k, v in first.items() if k not in ('sysargs', 'filtered_sysargs')}
    kwargs['mrsm_instance'] = 'sql:shell'
    kwargs['debug'] = True
    rebuilt = parse_dict_to_sysargs(kwargs)
    assert rebuilt[0] == 'python'
    assert rebuilt[-5:] == ['s.py', '--x', 'a b', '-i', 'y']
    second = parse_arguments(rebuilt)
    assert second['action'] == first['action']
    assert second['mrsm_instance'] == 'sql:shell'
    assert second['debug'] is True

    with_sub = parse_arguments(['python', '[-u]', 's.py', '--x'])
    assert with_sub['sub_args'] == ['-u']
    assert with_sub['action'] == ['python', 's.py', '--x']
    kwargs = {k: v for k, v in with_sub.items() if k not in ('sysargs', 'filtered_sysargs')}
    assert parse_dict_to_sysargs(kwargs) == ['python', '[-u]', 's.py', '--x']
