#! /usr/bin/env python
# -*- coding: utf-8 -*-
# vim:fenc=utf-8

"""
Subpackage for SQLConnector subclass.

The PEP 249 exceptions are what `SQLConnector.read()`, `exec()`, `value()` and `exec_queries()`
raise for database errors (raw SQL runs through the Rust connector, see `_native`); when
SQLAlchemy is imported, they are also instances of SQLAlchemy's classes of the same name.
"""

from meerschaum.connectors.sql._SQLConnector import SQLConnector
from meerschaum.connectors.sql._native import (
    DatabaseError,
    InterfaceError,
    OperationalError,
    ProgrammingError,
    IntegrityError,
    DataError,
    InternalError,
    NotSupportedError,
    ResourceClosedError,
    NATIVE_SQL_FLAVORS,
)

__all__ = (
    'SQLConnector',
    'DatabaseError',
    'InterfaceError',
    'OperationalError',
    'ProgrammingError',
    'IntegrityError',
    'DataError',
    'InternalError',
    'NotSupportedError',
    'ResourceClosedError',
    'NATIVE_SQL_FLAVORS',
)
