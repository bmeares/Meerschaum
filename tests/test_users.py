#! /usr/bin/env python3
# -*- coding: utf-8 -*-
# vim:fenc=utf-8

"""
Test user registration, deletion, and more.
"""

import base64
import json

import pytest
import datetime
from tests import debug
from tests.connectors import conns, get_flavors
from meerschaum import get_connector
from meerschaum.core import User

@pytest.mark.parametrize("flavor", get_flavors())
def test_register_user(flavor: str):
    username, password, email = conns['api'].username, conns['api'].password, 'none@none.com'
    user = User(username, password, email=email)
    conn = conns[flavor]
    conn.register_user(user, debug=debug)


@pytest.mark.parametrize("flavor", get_flavors())
def test_login_preserves_custom_user_scopes(flavor: str):
    """
    The password grant must issue a token carrying the scopes stored in the
    user's attributes, not fall back to the default scopes.
    """
    if flavor != 'api':
        pytest.skip("The password grant is only served by the API.")

    conn = conns[flavor]
    username, password = 'test_custom_scopes', 'test1234'
    custom_scope = 'custom:read'
    user = User(
        username,
        password,
        email='none@none.com',
        attributes={'scopes': [custom_scope, 'users:delete']},
    )
    user_conn = get_connector(
        'api', 'test_custom_scopes_login',
        username=username,
        password=password,
        host=conn.host,
        port=conn.port,
    )

    ### Clean up a leftover account from a previous run before registering.
    user_conn.delete_user(user, debug=debug)

    success, msg = conn.register_user(user, debug=debug)
    assert success, msg
    try:
        login_success, login_msg = user_conn.login(debug=debug)
        assert login_success, login_msg

        token = user_conn._token
        payload_segment = token.split('.')[1]
        payload = json.loads(
            base64.urlsafe_b64decode(payload_segment + '=' * (-len(payload_segment) % 4))
        )
        assert custom_scope in payload.get('scopes', [])
    finally:
        delete_success, delete_msg = user_conn.delete_user(user, debug=debug)
    assert delete_success, delete_msg
