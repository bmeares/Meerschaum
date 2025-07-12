#! /usr/bin/env python3
# vim:fenc=utf-8

"""
Define the API token routes.
"""

import json
import uuid

import meerschaum as mrsm
from meerschaum.core import Token
from meerschaum.api import (
    app,
    fastapi,
    debug,
    no_auth,
    manager,
    get_api_connector,
    endpoints,
)
from meerschaum.api.models import (
    RegisterTokenResponseModel,
    RegisterTokenRequestModel,
    SuccessTupleResponseModel,
    GetTokenResponseModel,
    GetTokensResponseModel,
)
from meerschaum.api._tokens import get_current_token
from meerschaum.utils.dtypes import json_serialize_value, value_is_null
from meerschaum.utils.misc import is_uuid

tokens_endpoint = endpoints['tokens']


@app.get(
    tokens_endpoint,
    tags=['Tokens'],
    response_model=GetTokensResponseModel,
)
def get_tokens(
    labels: str = '',
    curr_user=(fastapi.Depends(manager) if not no_auth else None),
):
    """
    Return the tokens registered to the current user.
    """
    _labels = None if not labels else labels.split(',')
    tokens = get_api_connector().get_tokens(user=curr_user, labels=_labels, debug=debug)
    return [
        {
            key: (None if value_is_null(val) else val)
            for key, val in dict(token.to_model()).items()
            if key != 'secret_hash'
        }
        for token in tokens
    ]


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
) -> mrsm.SuccessTuple:
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


@app.post(
    tokens_endpoint + '/{token_id}/edit',
    tags=['Tokens'],
    response_model=SuccessTupleResponseModel,
)
def edit_token(
    token_id: str,
    token_model: GetTokenResponseModel,
    curr_user=(fastapi.Depends(manager) if not no_auth else None),
) -> mrsm.SuccessTuple:
    """
    Edit the token's scope, expiration,, etc.
    """
    if not is_uuid(token_id):
        raise fastapi.HTTPException(
            status_code=400,
            detail="Token ID must be a UUID.",
        )

    token = Token(
        id=uuid.UUID(token_id),
        user=curr_user,
        is_valid=token_model.is_valid,
        creation=token_model.creation,
        expiration=token_model.expiration,
        scopes=token_model.scopes,
        label=token_model.label,
        instance=get_api_connector(),
    )
    return token.edit(debug=debug)


@app.post(
    tokens_endpoint + '/{token_id}/invalidate',
    tags=['Tokens'],
    response_model=SuccessTupleResponseModel,
)
def invalidate_token(
    token_id: str,
    curr_user=(fastapi.Depends(manager) if not no_auth else None),
) -> mrsm.SuccessTuple:
    """
    Invalidate the token, disabling it for future requests.
    """
    if not is_uuid(token_id):
        raise fastapi.HTTPException(
            status_code=400,
            detail="Token ID must be a UUID.",
        )

    _token_id = uuid.UUID(token_id)
    return get_api_connector().invalidate_token(
        Token(id=_token_id, instance=get_api_connector()),
        debug=debug,
    )


@app.delete(
    tokens_endpoint + '/{token_id}',
    tags=['Tokens'],
    response_model=SuccessTupleResponseModel,
)
def delete_token(
    token_id: str,
    curr_user=(fastapi.Depends(manager) if not no_auth else None),
) -> mrsm.SuccessTuple:
    """
    Delete the token from the instance.
    """
    if not is_uuid(token_id):
        raise fastapi.HTTPException(
            status_code=400,
            detail="Token ID must be a UUID.",
        )

    _token_id = uuid.UUID(token_id)
    return get_api_connector().delete_token(
        Token(id=_token_id, instance=get_api_connector()),
        debug=debug,
    )
