#! /usr/bin/env python3
# vim:fenc=utf-8

"""
Implement the `APIConnector` token methods.
"""

import json
import uuid
from datetime import datetime
from typing import Union, List, Optional

import meerschaum as mrsm
from meerschaum.core import Token
from meerschaum._internal.static import STATIC_CONFIG
tokens_endpoint = STATIC_CONFIG['api']['endpoints']['tokens']


def register_token(self, token: Token, debug: bool = False) -> mrsm.SuccessTuple:
    """
    Register the provided token to the API.
    """
    from meerschaum.utils.dtypes import json_serialize_value
    r_url = tokens_endpoint + '/register'
    response = self.post(
        r_url,
        data=json.dumps({
            'label': token.label,
            'scopes': token.scopes,
            'expiration': token.expiration,
        }, default=json_serialize_value),
        debug=debug,
    )
    if not response:
        return False, f"Failed to register token:\n{response.text}"

    data = response.json()
    token.label = data['label']
    token.secret = data['secret']
    token.id = uuid.UUID(data['id'])
    if data.get('expiration', None):
        token.expiration = datetime.fromisoformat(data['expiration'])

    return True, f"Registered token '{token.label}'."


def get_token_model(self, token_id: uuid.UUID, debug: bool = False) -> 'Union[TokenModel, None]':
    """
    Return a token's model from the API instance.
    """
    from meerschaum.models import TokenModel
    r_url = tokens_endpoint + f'/{token_id}'
    response = self.get(r_url, debug=debug)
    if not response:
        return None
    data = response.json()
    return TokenModel(**data)


def get_tokens(self, labels: Optional[List[str]] = None, debug: bool = False) -> List[Token]:
    """
    Return the tokens registered to the current user.
    """
    from meerschaum.utils.warnings import warn
    r_url = tokens_endpoint
    params = {}
    if labels:
        params['labels'] = ','.join(labels)
    response = self.get(r_url, params={'labels': labels}, debug=debug)
    if not response:
        warn(f"Could not get tokens from '{self}':\n{response.text}")
        return []

    tokens = [
        Token(instance=self, **payload)
        for payload in response.json()
    ]
    return tokens


def edit_token(self, token: Token, debug: bool = False) -> mrsm.SuccessTuple:
    """
    Persist the token's in-memory state to the API.
    """
    r_url = tokens_endpoint + f"/{token.id}/edit"
    response = self.post(
        r_url,
        json={
            'creation': token.creation.isoformat() if token.creation else None,
            'expiration': token.expiration.isoformat() if token.expiration else None,
            'label': token.label,
            'is_valid': token.is_valid,
            'scopes': token.scopes,
        },
    )
    if not response:
        return False, f"Failed to edit token:\n{response.text}"

    success, msg = response.json()
    return success, msg


def invalidate_token(self, token: Token, debug: bool = False) -> mrsm.SuccessTuple:
    """
    Invalidate the token, disabling it for future requests.
    """
    r_url = tokens_endpoint + f"/{token.id}/invalidate"
    response = self.post(r_url)
    if not response:
        return False, f"Failed to invalidate token:\n{response.text}"

    success, msg = response.json()
    return success, msg


def get_token_scopes(self, token_id: Union[uuid.UUID, Token], debug: bool = False) -> List[str]:
    """
    Return the scopes for a token.
    """
    _token_id = (token_id.id if isinstance(token_id, Token) else token_id)
    model = self.get_token_model(_token_id, debug=debug).scopes
    return getattr(model, 'scopes', [])


def token_exists(self, token_id: Union[uuid.UUID, Token], debug: bool = False) -> bool:
    """
    Return `True` if a token exists.
    """
    _token_id = (token_id.id if isinstance(token_id, Token) else token_id)
    model = self.get_token_model(_token_id, debug=debug)
    if model is None:
        return False
    return model.creation is not None


def delete_token(self, token: Token, debug: bool = False) -> mrsm.SuccessTuple:
    """
    Delete the token from the API.
    """
    r_url = tokens_endpoint + f"/{token.id}"
    response = self.delete(r_url, debug=debug)
    if not response:
        return False, f"Failed to delete token:\n{response.text}"
    
    success, msg = response.json()
    return success, msg
