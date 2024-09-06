#! /usr/bin/env python
# -*- coding: utf-8 -*-
# vim:fenc=utf-8

"""
Define JWT authorization here.
"""

import os
from meerschaum.api import app, endpoints, CHECK_UPDATE
from meerschaum.utils.packages import attempt_import
fastapi = attempt_import('fastapi', lazy=False, check_update=CHECK_UPDATE)
fastapi_responses = attempt_import('fastapi.responses', lazy=False, check_update=CHECK_UPDATE)
fastapi_login = attempt_import('fastapi_login', check_update=CHECK_UPDATE)

class CustomOAuth2PasswordRequestForm:
    def __init__(
        self,
        grant_type: str = fastapi.Form(None, regex="password|client_credentials"),
        username: str = fastapi.Form(...),
        password: str = fastapi.Form(...),
        scope: str = fastapi.Form(""),
        client_id: str = fastapi.Form(None),
        client_secret: str = fastapi.Form(None),
    ):
        self.grant_type = grant_type
        self.username = username
        self.password = password
        self.scope = scope
        self.client_id = client_id
        self.client_secret = client_secret


LoginManager = fastapi_login.LoginManager
def generate_secret_key() -> str:
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
