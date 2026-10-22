#! /usr/bin/env python
# -*- coding: utf-8 -*-
# vim:fenc=utf-8

"""
Launch into a CLI environment to interact with the SQL Connector
"""

from __future__ import annotations
### NOTE: This import adds `Iterable` to collections, which is needed by some CLIs.
from meerschaum.utils.typing import SuccessTuple

flavor_clis = {
    'postgresql'     : 'pgcli',
    'postgis'        : 'pgcli',
    'timescaledb'    : 'pgcli',
    'timescaledb-ha' : 'pgcli',
    'cockroachdb'    : 'pgcli',
    'citus'          : 'pgcli',
    'mysql'          : 'mycli',
    'mariadb'        : 'mycli',
    'percona'        : 'mycli',
    'sqlite'         : 'litecli',
    'geopackage'     : 'litecli',
    'mssql'          : 'mssqlcli',
    'duckdb'         : 'gadwall',
}
cli_deps = {
    'pgcli': ['pgspecial', 'pendulum', 'cli_helpers'],
    'mycli': ['cryptography'],
    'mssql': ['cli_helpers'],
}


def cli(
    self,
    debug: bool = False,
    nopretty: bool = False,
) -> SuccessTuple:
    """Open the native SQL client launcher using this connector's exact attributes.

    Rainfrog, configured preferences, fallback clients, credentials, and exit status use
    the same implementation as ``mrsm sql LABEL``. ``nopretty`` forces the plain client.
    No SQLAlchemy engine or database connection is needed to launch an external client.
    """
    from meerschaum import _core
    from meerschaum._internal.entry import _plain_value

    attributes = dict(self.meta)
    attributes.pop('type', None)
    attributes.pop('label', None)
    context = {'attributes': {str(self): {
        key: value for key, value in attributes.items() if _plain_value(value)
    }}}
    arguments = ['sql', self.label]
    if debug:
        arguments.append('--debug')
    if nopretty:
        arguments.append('--nopretty')
    return _core.entry(arguments, None, context)


def _cli_exit(self, debug: bool = False, nopretty: bool = False) -> SuccessTuple:
    """Compatibility alias for the native SQL client launcher."""
    return cli(self, debug=debug, nopretty=nopretty)
