#! /usr/bin/env python3
# vim:fenc=utf-8

"""
Implement the `APIConnector` token methods (the Rust client, `meerschaum._core.ApiConnector`).
"""

import uuid
from typing import Union, List, Optional

import meerschaum as mrsm


def _token_id(token_id):
    """Return the ID of a token or the ID itself."""
    return getattr(token_id, 'id', token_id) if not isinstance(token_id, uuid.UUID) else token_id


def register_token(self, token: 'Token', debug: bool = False) -> mrsm.SuccessTuple:
    """
    Register the provided token to the API.
    """
    return self._rust_client().register_token(token)


def get_token_model(self, token_id: uuid.UUID, debug: bool = False) -> 'Union[TokenModel, None]':
    """
    Return a token's model from the API instance.
    """
    from meerschaum.models import TokenModel
    data = self._rust_client().get_token_model(_token_id(token_id))
    if data is None:
        return None
    return TokenModel(**data)


def get_tokens(self, labels: Optional[List[str]] = None, debug: bool = False) -> List['Token']:
    """
    Return the tokens registered to the current user.
    """
    from meerschaum.core import Token
    return [
        Token(instance=self, **payload)
        for payload in self._rust_client().get_tokens(labels)
    ]


def edit_token(self, token: 'Token', debug: bool = False) -> mrsm.SuccessTuple:
    """
    Persist the token's in-memory state to the API.
    """
    return self._rust_client().edit_token(token)


def invalidate_token(self, token: 'Token', debug: bool = False) -> mrsm.SuccessTuple:
    """
    Invalidate the token, disabling it for future requests.
    """
    return self._rust_client().invalidate_token(token)


def get_token_scopes(self, token_id: Union[uuid.UUID, 'Token'], debug: bool = False) -> List[str]:
    """
    Return the scopes for a token.
    """
    return self._rust_client().get_token_scopes(_token_id(token_id))


def token_exists(self, token_id: Union[uuid.UUID, 'Token'], debug: bool = False) -> bool:
    """
    Return `True` if a token exists.
    """
    return self._rust_client().token_exists(_token_id(token_id))


def delete_token(self, token: 'Token', debug: bool = False) -> mrsm.SuccessTuple:
    """
    Delete the token from the API.
    """
    return self._rust_client().delete_token(token)
