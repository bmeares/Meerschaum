#! /usr/bin/env python3
# -*- coding: utf-8 -*-
# vim:fenc=utf-8

"""
An undefined connector (rc15 S4): Python says where connectors come from, like the CLI does,
instead of a traceback and "Missing flavor" / "Missing 'host'".
"""

import pytest

import meerschaum as mrsm
from meerschaum.connectors.parse import parse_instance_keys


@pytest.fixture(autouse=True)
def _plugins_loaded():
    """The first lookup imports the plugins' connectors; their output is not under test."""
    mrsm.get_connector('sql:memory')


@pytest.mark.parametrize('typ', ['sql', 'api', 'valkey'])
def test_get_connector_warns_once_that_the_connector_is_not_defined(typ, capsys):
    keys = f'{typ}:e1_rc15_nowhere'
    capsys.readouterr()
    conn = mrsm.get_connector(keys)
    assert conn is None
    captured = capsys.readouterr()
    text = captured.out + captured.err
    message = (
        f"Connector '{keys}' is not defined. Add it to the config (`mrsm bootstrap connectors`), "
        f"set `MRSM_{typ.upper()}_E1_RC15_NOWHERE`, or install the plugin that provides it."
    )
    assert text.count(message) == 1, text
    assert 'Traceback' not in text
    assert 'Missing' not in text


def test_parsing_undefined_keys_raises_the_message_once(capsys):
    capsys.readouterr()
    with pytest.raises(Exception) as info:
        parse_instance_keys('sql:e1_rc15_nowhere')
    ### v4's sentence stays first (code may match it), then the reason.
    assert (
        "Unable to parse connector keys 'sql:e1_rc15_nowhere'. "
        "Connector 'sql:e1_rc15_nowhere' is not defined."
    ) in str(info.value)
    assert 'MRSM_SQL_E1_RC15_NOWHERE' in str(info.value)
    captured = capsys.readouterr()
    assert 'e1_rc15_nowhere' not in captured.out + captured.err, (
        "the reason is the exception's, not a warning too"
    )


def test_connectors_with_attributes_or_config_are_defined(capsys):
    conn = mrsm.get_connector('sql', 'e1_rc15_adhoc', flavor='sqlite', database=':memory:')
    assert conn is not None
    capsys.readouterr()
    assert mrsm.get_connector('sql:e1_rc15_adhoc') is conn
    assert mrsm.get_connector('sql:memory') is not None
    captured = capsys.readouterr()
    assert 'is not defined' not in captured.out + captured.err
