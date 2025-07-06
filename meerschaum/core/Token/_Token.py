#! /usr/bin/env python3
# vim:fenc=utf-8

"""
Define the properties of a long-lived access token.
"""

from __future__ import annotations

import base64
import uuid
from random import randint
from typing import Optional, Union, List, Dict
from datetime import datetime, timedelta, timezone

import meerschaum as mrsm
from meerschaum.models import TokenModel


class Token:
    """
    Tokens (long lived access tokens) may be registered and revoked to provide easier authentication (e.g. IoT devices).
    Tokens must be tied to a Meerschaum user account.
    """

    def __init__(
        self,
        label: Optional[str] = None,
        creation: Optional[datetime] = None,
        expiration: Optional[datetime] = None,
        instance: Optional[str] = None,
        user: Optional[mrsm.core.User] = None,
        user_id: Union[int, str, uuid.UUID, None] = None,
        scopes: Optional[List[str]] = None,
        is_valid: bool = True,
        id: Optional[uuid.UUID] = None,
        secret: Optional[str] = None,
        secret_hash: Optional[str] = None,
    ):
        from meerschaum.utils.dtypes import coerce_timezone
        from meerschaum.utils.daemon import get_new_daemon_name
        from meerschaum._internal.static import STATIC_CONFIG
        self.creation = coerce_timezone(creation) if creation is not None else None
        self.expiration = coerce_timezone(expiration) if expiration is not None else None
        self._instance_keys = str(instance) if instance is not None else None
        self.label = label or get_new_daemon_name()
        self._user = user
        self._user_id = user_id
        self.scopes = scopes or list(STATIC_CONFIG['tokens']['scopes'])
        self.is_valid = is_valid
        self.id = id
        self.secret = secret
        self.secret_hash = secret_hash

    def generate_secret(self) -> str:
        """
        Generate the internal secret value for this token.
        """
        if self.secret:
            return self.secret

        from meerschaum.utils.misc import generate_password
        from meerschaum._internal.static import STATIC_CONFIG
        min_len = STATIC_CONFIG['tokens']['minimum_length']
        max_len = STATIC_CONFIG['tokens']['maximum_length']

        secret_len = randint(min_len, max_len + 1)
        self.secret = generate_password(secret_len)
        return self.secret

    def get_api_key(self) -> str:
        """
        Return the API key to be sent in the `Authorization` header.
        """
        return base64.b64encode(f"{self.id}:{self.secret}".encode('utf-8')).decode('utf-8')

    @property
    def instance_connector(self) -> mrsm.connectors.InstanceConnector:
        """
        Return the instance connector to use for this token.
        """
        from meerschaum.connectors.parse import parse_instance_keys
        return parse_instance_keys(self._instance_keys)

    @property
    def user(self) -> Union[mrsm.core.User, None]:
        """
        Return the `User` for this token.
        """
        if self._user is not None:
            return self._user

        if self._user_id is not None:
            username = self.instance_connector.get_username(self._user_id)
            if not username:
                return None
            _user = mrsm.core.User(
                username,
                user_id=self._user_id,
                instance=str(self.instance_connector),
            )
            self._user = _user
            return _user

        return None

    def register(self, debug: bool = False) -> mrsm.SuccessTuple:
        """
        Register the new token to the configured instance.
        """
        _ = self.generate_secret()
        return self.instance_connector.register_token(self, debug=debug)

    def to_model(self, refresh: bool = False, debug: bool = False) -> TokenModel:
        """
        Export the current state to a `TokenModel`.
        """
        in_memory_doc = {
            'id': self.id,
            'label': self.label,
            'creation': self.creation,
            'expiration': self.expiration,
            'is_valid': self.is_valid,
            'user_id': self._user_id,
            'scopes': self.scopes,
        }
        doc = in_memory_doc

        if refresh:
            tokens_pipe = self.instance_connector.get_tokens_pipe()
            params: Dict[str, Union[str, uuid.UUID]] = {
                'id': self.id
            } if self.id else {
                'label': self.label,
            }
            doc = tokens_pipe.get_doc(params=params, debug=debug)
            if doc is None:
                raise ValueError(f"{self} does not exist on instance '{self.instance_connector}'.")

        return TokenModel(**doc)

    def __str__(self):
        return self.label

    def __repr__(self):
        return self.to_model(refresh=False).__repr__().replace('TokenModel(', 'Token(')
