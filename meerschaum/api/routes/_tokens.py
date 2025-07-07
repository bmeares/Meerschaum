#! /usr/bin/env python3
# vim:fenc=utf-8

"""
Define the API token routes.
"""

import json
import uuid
from datetime import datetime, timezone
from typing import Optional, Tuple, List

import meerschaum as mrsm
from meerschaum.core import Token
from meerschaum.models import TokenModel
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
    SuccessTupleResponseModel,
    GetTokenResponseModel,
)
from meerschaum.api._tokens import optional_token, get_current_token
from meerschaum.utils.dtypes import coerce_timezone, json_serialize_value, value_is_null
from meerschaum._internal.static import STATIC_CONFIG
from meerschaum.utils.misc import is_uuid

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
    token_id, token_secret = token.generate_credentials()
    register_success, register_msg = token.register(debug=debug)
    if not register_success:
        raise fastapi.HTTPException(
            status_code=409,
            detail=f"Could not register new token:\n{register_msg}",
        )
    api_key = token.get_api_key()

    return RegisterTokenResponseModel(
        label=token.label,
        secret=token_secret,
        id=token_id,
        api_key=api_key,
        expiration=token.expiration,
    )


@app.post(
    tokens_endpoint + '/validate',
    tags=['Tokens'],
    response_model=SuccessTupleResponseModel,
)
def validate_api_key(
    curr_token=(fastapi.Depends(get_current_token) if not no_auth else None),
) -> SuccessTupleResponseModel:
    """
    Return a 200 if the given Authorization token (API key) is valid.
    """
    return True, "Success"


@app.get(
    tokens_endpoint + '/{token_id}',
    tags=['Tokens'],
    response_model=GetTokenResponseModel,
)
def get_token_model(
    token_id: str,
    curr_user=(fastapi.Depends(manager) if not no_auth else None)
):
    """
    Return the token model's fields.
    """
    if not is_uuid(token_id):
        raise fastapi.HTTPException(
            status_code=400,
            detail="Invalid token ID.",
        )
    real_token_id = uuid.UUID(token_id)
    conn = get_api_connector()
    token_model = conn.get_token_model(real_token_id)
    if token_model is None:
        raise fastapi.HTTPException(
            status_code=404,
            detail="Token does not exist.",
        )

    curr_user_id = get_api_connector().get_user_id(curr_user, debug=debug) if curr_user is not None else None
    if token_model.user_id and token_model.user_id != curr_user_id:
        curr_user_type = get_api_connector().get_user_type(curr_user, debug=debug)
        if curr_user_type != 'admin':
            raise fastapi.HTTPException(
                status_code=403,
                detail="Cannot edit another user's token.",
            )

    payload = {
        key: (None if value_is_null(val) else val)
        for key, val in dict(token_model).items()
        if key != 'secret_hash'
    }
    return fastapi.Response(
        json.dumps(payload, default=json_serialize_value),
        media_type='application/json',
    )
