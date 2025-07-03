#! /usr/bin/env python3
# vim:fenc=utf-8

"""
Define the properties of a long-lived access token.
"""

from __future__ import annotations

import uuid
from random import randint
from typing import Optional, Union, List
from datetime import datetime, timedelta, timezone

import meerschaum as mrsm


class Token:
    """
    Tokens (long lived access tokens) may be registered and revoked to provide easier authentication (e.g. IoT devices).
    Tokens must be tied to a Meerschaum user account.
    """

    def __init__(
        self,
        label: Optional[str] = None,
        expiration: Optional[datetime] = None,
        instance: Optional[str] = None,
        user: Optional[mrsm.core.User] = None,
        scopes: Optional[List[str]] = None,
        is_valid: bool = True,
    ):
        from meerschaum.utils.dtypes import coerce_timezone
        from meerschaum.utils.daemon import get_new_daemon_name
        self.expiration = coerce_timezone(expiration) if expiration is not None else None
        self._instance_keys = str(instance) if instance is not None else None
        self.label = label or get_new_daemon_name()
        self.user = user
        self.scopes = scopes
        self.is_valid = is_valid

    def generate_secret(self) -> str:
        """
        Generate the internal secret value for this token.
        """
        from meerschaum.utils.misc import generate_password
        from meerschaum.config.static import STATIC_CONFIG
        min_len = STATIC_CONFIG['tokens']['minimum_length']
        max_len = STATIC_CONFIG['tokens']['maximum_length']

        secret_len = randint(min_len, max_len + 1)
        self.secret = generate_password(secret_len)
        return self.secret

    @property
    def instance_connector(self) -> mrsm.connectors.InstanceConnector:
        """
        Return the instance connector to use for this token.
        """
        from meerschaum.connectors.parse import parse_instance_keys
        return parse_instance_keys(self._instance_keys)

    def register(self, debug: bool = False) -> mrsm.SuccessTuple:
        """
        Register the new token to the configured instance.
        """
        _ = self.generate_secret()
        return self.instance_connector.register_token(self, debug=debug)

    def __str__(self):
        return f"Token('{self.label}')"

    def __repr__(self):
        return str(self)
