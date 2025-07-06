#! /usr/bin/env python
# -*- coding: utf-8 -*-
# vim:fenc=utf-8

"""
Define JWT authorization here.
"""

import os
import base64

from meerschaum.api import app, endpoints, CHECK_UPDATE
from meerschaum._internal.static import STATIC_CONFIG
from meerschaum.utils.packages import attempt_import
fastapi = attempt_import('fastapi', lazy=False, check_update=CHECK_UPDATE)
fastapi_responses = attempt_import('fastapi.responses', lazy=False, check_update=CHECK_UPDATE)
fastapi_login = attempt_import('fastapi_login', check_update=CHECK_UPDATE)


from typing import Optional

class CustomOAuth2PasswordRequestForm:
    def __init__(
        self,
        grant_type: str = fastapi.Form(None, regex="password|client_credentials"),
        username: Optional[str] = fastapi.Form(None),
        password: Optional[str] = fastapi.Form(None),
        scope: str = fastapi.Form(" ".join(STATIC_CONFIG['tokens']['scopes'])),
        client_id: Optional[str] = fastapi.Form(None),
        client_secret: Optional[str] = fastapi.Form(None),
        authorization: Optional[str] = fastapi.Header(None),
    ):
        self.grant_type = grant_type
        self.username = username
        self.password = password
        self.scope = scope
        self.client_id = client_id
        self.client_secret = client_secret

        # Parse Authorization header for client_id and client_secret if not provided in form
        if self.grant_type == 'client_credentials' and not self.client_id and not self.client_secret and authorization:
            try:
                scheme, credentials = authorization.split()
                if scheme.lower() == 'basic':
                    decoded_credentials = base64.b64decode(credentials).decode('utf-8')
                    _client_id, _client_secret = decoded_credentials.split(':', 1)
                    self.client_id = _client_id
                    self.client_secret = _client_secret
            except ValueError:
                pass # Malformed header, let validation handle it later


LoginManager = fastapi_login.LoginManager
def generate_secret_key() -> bytes:
    """
    Read or generate the secret keyfile.
    """
    from meerschaum.config._paths import API_SECRET_KEY_PATH
    if not API_SECRET_KEY_PATH.exists():
        secret_key = os.urandom(24).hex()
        with open(API_SECRET_KEY_PATH, 'w+', encoding='utf-8') as f:
            f.write(secret_key)
    else:
        with open(API_SECRET_KEY_PATH, 'r', encoding='utf-8') as f:
            secret_key = f.read()

    return secret_key.encode('utf-8')


SECRET = generate_secret_key()
manager = LoginManager(SECRET, token_url=endpoints['login'])
