#! /usr/bin/env python3
# -*- coding: utf-8 -*-
# vim:fenc=utf-8

"""
Define custom SQLAlchemy types.
"""

import uuid
from meerschaum.utils.packages import attempt_import
sqlalchemy, sqlalchemy_dialects_postgresql = attempt_import(
    'sqlalchemy', 'sqlalchemy.dialects.postgresql'
)

BINARY = sqlalchemy.types.BINARY
psqlUUID = sqlalchemy_dialects_postgresql.UUID

class UUID(sqlalchemy.types.TypeDecorator):
    """
    Platform-independent GUID type.
    
    Uses PostgreSQL's UUID type, otherwise uses
    BINARY(16), to store UUID.

    """
    impl = BINARY

    def load_dialect_impl(self, dialect):
        if dialect.name == 'postgresql':
            return dialect.type_descriptor(psqlUUID())
        else:
            return dialect.type_descriptor(BINARY(16))

    def process_bind_param(self, value, dialect):
        if value is None:
            return value
        else:
            if not isinstance(value, uuid.UUID):
                if isinstance(value, bytes):
                    value = uuid.UUID(bytes=value)
                elif isinstance(value, int):
                    value = uuid.UUID(int=value)
                elif isinstance(value, str):
                    value = uuid.UUID(value)
        if dialect.name == 'postgresql':
            return str(value)
        else:
            return value.bytes

    def process_result_value(self, value, dialect):
        if value is None:
            return value
        if dialect.name == 'postgresql':
            return uuid.UUID(value)
        else:
            return uuid.UUID(bytes=value)
