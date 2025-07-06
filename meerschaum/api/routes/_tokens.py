#! /usr/bin/env python3
# vim:fenc=utf-8

"""
Define the API token routes.
"""

import json
from datetime import datetime, timezone
from typing import Optional, Tuple, List

import meerschaum as mrsm
from meerschaum.core import Token
from meerschaum.api import (
    app,
    fastapi,
    debug,
    no_auth,
    manager,
    private,
    get_api_connector,
    endpoints,
)
from meerschaum.api.models import (
    RegisterTokenResponseModel,
    RegisterTokenRequestModel,
)
from meerschaum.api._tokens import optional_token, get_current_token
from meerschaum.utils.dtypes import coerce_timezone, json_serialize_value
from meerschaum._internal.static import STATIC_CONFIG

tokens_endpoint = endpoints['tokens']


@app.post(
    tokens_endpoint + '/register',
    tags=['Tokens'],
    response_model=RegisterTokenResponseModel,
)
def register_token(
    request_model: RegisterTokenRequestModel,
    curr_user=(fastapi.Depends(manager) if not no_auth else None),
) -> RegisterTokenResponseModel:
    """
    Register a new Token, returning its secret (unable to be retrieved later).
    """
    token = Token(
        user=curr_user,
        label=request_model.label,
        expiration=request_model.expiration,
        scopes=request_model.scopes,
        instance=get_api_connector(),
    )
    secret = token.generate_secret()
    register_success, register_msg = token.register(debug=debug)
    if not register_success:
        raise fastapi.HTTPException(
            status_code=409,
            detail=f"Could not register new token:\n{register_msg}",
        )
    token_model = token.to_model(refresh=True)

    return RegisterTokenResponseModel(
        label=token.label,
        secret=secret,
        id=str(token_model.id),
        expiration=token.expiration,
    )


@app.post(
    tokens_endpoint + '/validate',
    tags=['Tokens'],
)
def validate_api_key(
    curr_token=(fastapi.Depends(get_current_token) if not no_auth else None),
):
    """
    Return a 200 if the given Authorization token (API key) is valid.
    """
    return True, "Success"
