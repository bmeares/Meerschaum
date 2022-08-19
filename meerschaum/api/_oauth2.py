#! /usr/bin/env python
# -*- coding: utf-8 -*-
# vim:fenc=utf-8

"""
Define JWT authorization here.
"""

import os
from meerschaum.api import app, endpoints
from meerschaum.utils.packages import attempt_import
fastapi = attempt_import('fastapi', lazy=False)
fastapi_responses = attempt_import('fastapi.responses', lazy=False)
fastapi_login = attempt_import('fastapi_login')

LoginManager = fastapi_login.LoginManager
def generate_secret_key() -> str:
    """
    Read or generate the secret keyfile.
    """
    from meerschaum.config._paths import API_SECRET_KEY_PATH
    if not API_SECRET_KEY_PATH.exists():
        secret_key = os.urandom(24).hex()
        with open(API_SECRET_KEY_PATH, 'w+') as f:
            f.write(secret_key)
    else:
        with open(API_SECRET_KEY_PATH, 'r') as f:
            secret_key = f.read()

    return secret_key

SECRET = generate_secret_key()
manager = LoginManager(SECRET, token_url=endpoints['login'])
