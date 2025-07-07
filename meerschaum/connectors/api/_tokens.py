#! /usr/bin/env python3
# vim:fenc=utf-8

"""
Implement the `APIConnector` token methods.
"""

import json
import uuid
from datetime import datetime
from typing import Union

import meerschaum as mrsm
from meerschaum.core import Token
from meerschaum.models import TokenModel
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


def get_token_model(self, token_id: uuid.UUID, debug: bool = False) -> Union[TokenModel, None]:
    """
    Return a token's model from the API instance.
    """
    r_url = tokens_endpoint + f'/{token_id}'
    response = self.get(r_url, debug=debug)
    if not response:
        return None
    data = response.json()
    return TokenModel(**data)
