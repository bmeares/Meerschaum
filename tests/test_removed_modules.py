#! /usr/bin/env python3
# -*- coding: utf-8 -*-
# vim:fenc=utf-8

"""
Every public v4.1.4 module that v5 deleted raises a `ModuleNotFoundError` naming its replacement.
"""

import importlib
import importlib.util

import pytest

from meerschaum._internal._removed import removal_message


@pytest.mark.parametrize(
    'module, replacement',
    [
        ('meerschaum.utils.daemon.FileDescriptorInterceptor', 'meerschaum.jobs.Job'),
        ('meerschaum._internal.shell.resources', 'native shell'),
    ],
)
def test_removed_module_names_its_replacement(module, replacement):
    with pytest.raises(ModuleNotFoundError, match='removed in Meerschaum 5.0') as raised:
        importlib.import_module(module)
    assert replacement in str(raised.value)
    assert raised.value.name == module


def test_the_restored_console_modules_are_not_reported_removed():
    ### The Dash console came back (D21): its modules exist again, only the websocket plumbing
    ### stays removed (the dashboard follows jobs over the server's event stream).
    for module in (
        'meerschaum.api.dash.assets',
        'meerschaum.api.dash.callbacks',
        'meerschaum.api.dash.pages',
        'meerschaum.api.dash.jobs',
        'meerschaum.api.dash.webterm',
    ):
        assert removal_message(module) is None, module
    message = removal_message('meerschaum.api.dash.websockets')
    assert message is not None and 'web console' in message


def test_probing_the_new_entries_does_not_raise():
    for module in (
        'meerschaum.utils.daemon.FileDescriptorInterceptor',
        'meerschaum._internal.shell.resources',
    ):
        assert importlib.util.find_spec(module) is not None
