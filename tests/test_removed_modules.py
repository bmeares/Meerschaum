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


def test_the_dash_assets_package_names_the_native_console():
    ### (Importing it would first import `meerschaum.api.dash`, which needs the `api` extra.)
    message = removal_message('meerschaum.api.dash.assets')
    assert message is not None and 'native' in message
    assert removal_message('meerschaum.api.dash.assets.webterm_keys') == message


def test_probing_the_new_entries_does_not_raise():
    for module in (
        'meerschaum.utils.daemon.FileDescriptorInterceptor',
        'meerschaum._internal.shell.resources',
    ):
        assert importlib.util.find_spec(module) is not None
