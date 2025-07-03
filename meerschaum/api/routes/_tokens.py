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
)
from meerschaum.utils.dtypes import coerce_timezone, json_serialize_value
from meerschaum._internal.static import STATIC_CONFIG

tokens_endpoint = endpoints['tokens']


@app.post(
    tokens_endpoint + '/register',
    tags=['Tokens'],
    response_model=RegisterTokenResponseModel,
)
def register_token(
    label: Optional[str] = None,
    expiration: Optional[str] = None,
    scopes: List[str] = STATIC_CONFIG['tokens']['scopes'],
    curr_user=(
        fastapi.Depends(manager) if not no_auth else None
    ),
) -> RegisterTokenResponseModel:
    """
    Register a new Token, returning its secret (unable to be retrieved later).
    """
    expiration = coerce_timezone(datetime.fromisoformat(expiration)) if expiration else None
    token = Token(
        user=curr_user,
        label=label,
        expiration=expiration,
        instance=get_api_connector(),
    )
    register_success, register_msg = token.register(debug=debug)
    if not register_success:
        raise fastapi.HTTPException(
            status_code=409,
            detail=f"Could not register new token:\n{register_msg}",
        )

    doc = {
        'label': token.label,
        'secret': token.secret,
        'expires_at': (
            token.expiration.isoformat()
            if token.expiration is not None
            else None
        ),
    }
    payload = json.dumps(doc, default=json_serialize_value)
    return fastapi.Response(payload, media_type='application/json')
