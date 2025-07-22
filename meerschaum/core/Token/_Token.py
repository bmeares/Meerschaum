#! /usr/bin/env python3
# vim:fenc=utf-8

"""
Define the properties of a long-lived access token.
"""

from __future__ import annotations

import base64
import uuid
from random import randint
from typing import Optional, Union, List, Tuple
from datetime import datetime, timedelta, timezone

import meerschaum as mrsm

_PLACEHOLDER_EXPIRATION = datetime(2000, 1, 1)

class Token:
    """
    Tokens (long lived access tokens) may be registered and revoked to provide easier authentication (e.g. IoT devices).
    Tokens must be tied to a Meerschaum user account.
    """

    def __init__(
        self,
        label: Optional[str] = None,
        creation: Optional[datetime] = None,
        expiration: Optional[datetime] = _PLACEHOLDER_EXPIRATION,
        instance: Optional[str] = None,
        user: Optional[mrsm.core.User] = None,
        user_id: Union[int, str, uuid.UUID, None] = None,
        scopes: Optional[List[str]] = None,
        is_valid: bool = True,
        id: Optional[uuid.UUID] = None,
        secret: Optional[str] = None,
        secret_hash: Optional[str] = None,
    ):
        from meerschaum.utils.dtypes import coerce_timezone, round_time
        from meerschaum.utils.daemon import get_new_daemon_name
        from meerschaum._internal.static import STATIC_CONFIG
        now = datetime.now(timezone.utc)
        default_expiration_days = mrsm.get_config(
            'system', 'api', 'tokens', 'default_expiration_days',
        ) or 366
        default_expiration = round_time(
            now + timedelta(days=default_expiration_days),
            timedelta(days=1),
        )
        if expiration == _PLACEHOLDER_EXPIRATION:
            expiration = default_expiration
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

    def generate_credentials(self) -> Tuple[uuid.UUID, str]:
        """
        Generate and return the client ID and secret values for this token.
        """
        if self.id and self.secret:
            return self.id, self.secret

        from meerschaum.utils.misc import generate_password
        from meerschaum._internal.static import STATIC_CONFIG
        min_len = STATIC_CONFIG['tokens']['minimum_length']
        max_len = STATIC_CONFIG['tokens']['maximum_length']

        secret_len = randint(min_len, max_len + 1)
        self.secret = generate_password(secret_len)
        self.id = uuid.uuid4()
        return self.id, self.secret

    def get_api_key(self) -> str:
        """
        Return the API key to be sent in the `Authorization` header.
        """
        return 'mrsm-key:' + base64.b64encode(f"{self.id}:{self.secret}".encode('utf-8')).decode('utf-8')

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
        if self.user is None:
            raise ValueError("Cannot register a token with a user.")

        return self.instance_connector.register_token(self, debug=debug)

    def edit(self, debug: bool = False) -> mrsm.SuccessTuple:
        """
        Edit some of the token's attributes (expiration, scopes).
        """
        return self.instance_connector.edit_token(self, debug=debug)

    def invalidate(self, debug: bool = False) -> mrsm.SuccessTuple:
        """
        Set `is_valid` to False for this token.
        """
        self.is_valid = False
        return self.instance_connector.invalidate_token(self, debug=debug)

    def delete(self, debug: bool = False) -> mrsm.SuccessTuple:
        """
        Delete this token from the instance connector.
        """
        return self.instance_connector.delete_token(self, debug=debug)

    def exists(self, debug: bool = False) -> bool:
        """
        Return `True` if a token's ID exists in the tokens pipe.
        """
        if not self.id:
            return False
        return self.instance_connector.token_exists(self.id, debug=debug)

    def to_model(self, refresh: bool = False, debug: bool = False) -> 'TokenModel':
        """
        Export the current state to a `TokenModel`.
        """
        from meerschaum.models import TokenModel
        in_memory_doc = {
            'id': self.id,
            'label': self.label,
            'creation': self.creation,
            'expiration': self.expiration,
            'is_valid': self.is_valid,
            'user_id': self._user_id,
            'scopes': self.scopes,
        }
        if not refresh:
            return TokenModel(**in_memory_doc)

        if not self.id:
            raise ValueError(f"ID is not set for {self}.")

        token_model = self.instance_connector.get_token_model(self.id, debug=debug)
        if token_model is None:
            raise ValueError(f"{self} does not exist on instance '{self.instance_connector}'.")

        return token_model

    def get_scopes(self, refresh: bool = False, debug: bool = False) -> List[str]:
        """
        Return the scopes for this `Token`.
        """
        if not refresh:
            return self.scopes

        self.scopes = self.instance_connector.get_token_scopes(self, debug=debug)
        return self.scopes

    def get_expiration_status(self, debug: bool = False) -> bool:
        """
        Check the token's expiration against the current timestamp.
        If it's expired, invalidate the token.

        Returns
        -------
        A bool to indication whether the token has expired.
        A value of `True` means the token is invalid,
        and `False` indicates a valid token.
        """
        expiration = self.expiration
        if expiration is None:
            return False

        now = datetime.now(timezone.utc)
        is_expired = expiration <= now
        if is_expired:
            self.is_valid = False
            invalidate_success, invalidate_msg = self.invalidate(debug=debug)
            if not invalidate_success:
                from meerschaum.utils.warnings import warn
                warn(f"Failed to invalidate {self}:\n{invalidate_msg}")

        return is_expired

    def __str__(self):
        return self.label

    def __repr__(self):
        return self.to_model(refresh=False).__repr__().replace('TokenModel(', 'Token(')
