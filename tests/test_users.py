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


def test_api_connector_keeps_v4_private_token_attributes():
    """
    v4's `APIConnector` stored the session in `_token` and `_expires` (`None` before a login) and
    its tests and plugins read and assign them; the `token` property only logs in when the stored
    token is missing or expiring.
    """
    conn = get_connector('api', 'w17ba_offline', host='127.0.0.1', port=1, username='u', password='p')
    assert conn._token is None
    assert conn._expires is None

    expires = datetime.datetime.now(datetime.timezone.utc).replace(tzinfo=None) + datetime.timedelta(hours=1)
    conn._token = 'header.payload.signature'
    conn._expires = expires
    assert conn._token == 'header.payload.signature'
    assert conn._expires == expires
    ### A valid stored token is returned as is (no login request: nothing listens on the port).
    assert conn.token == 'header.payload.signature'

    conn._token = None
    assert conn._token is None

    ### An aware expiry is stored as naive UTC (converted, not stripped); ISO strings parse.
    eastern = datetime.timezone(datetime.timedelta(hours=-5))
    conn._expires = datetime.datetime(2099, 1, 1, 12, 0, tzinfo=eastern)
    assert conn._expires == datetime.datetime(2099, 1, 1, 17, 0)
    conn._expires = '2099-01-01T00:00:00+00:00'
    assert conn._expires == datetime.datetime(2099, 1, 1, 0, 0)
    conn._expires = None
    assert conn._expires is None
    with pytest.raises(ValueError):
        conn._expires = 'garbage'
    with pytest.raises(TypeError):
        conn._expires = datetime.date(2099, 1, 1)


@pytest.mark.parametrize("flavor", get_flavors())
def test_login_preserves_custom_user_scopes(flavor: str):
    """
    The password grant must issue a token carrying the scopes stored in the
    user's attributes, not fall back to the default scopes.
    """
    if flavor != 'api':
        pytest.skip("The password grant is only served by the API.")

    conn = conns[flavor]
    ### Scope attributes are administrative data. Seed this account on the owned backing
    ### instance, as the admin-scope test does; self-registration stays closed by default.
    instance_conn = get_connector('sql', 'test_postgresql')
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
    instance_conn.delete_user(user, debug=debug)

    success, msg = instance_conn.register_user(user, debug=debug)
    assert success, msg
    try:
        login_success, login_msg = user_conn.login(debug=debug)
        assert login_success, login_msg

        token = user_conn._token
        payload_segment = token.split('.')[1]
        payload = json.loads(
            base64.urlsafe_b64decode(payload_segment + '=' * (-len(payload_segment) % 4))
        )
        assert set(payload.get('scopes', [])) == {custom_scope, 'users:delete'}
    finally:
        delete_success, delete_msg = user_conn.delete_user(user, debug=debug)
    assert delete_success, delete_msg


@pytest.mark.parametrize("flavor", get_flavors())
def test_login_treats_admin_as_wildcard_scope(flavor: str):
    """
    The password grant must treat an admin as holding the `'*'` scope,
    so an admin's stored scope list does not filter the grant.
    """
    if flavor != 'api':
        pytest.skip("The password grant is only served by the API.")

    conn = conns[flavor]
    ### Register directly on the API's backing instance: the API's
    ### register route does not accept a user type.
    instance_conn = get_connector('sql', 'test_postgresql')
    username, password = 'test_admin_scopes', 'test1234'
    user = User(
        username,
        password,
        type='admin',
        email='none@none.com',
        attributes={'scopes': ['custom:read']},
    )
    user_conn = get_connector(
        'api', 'test_admin_scopes_login',
        username=username,
        password=password,
        host=conn.host,
        port=conn.port,
    )

    ### Clean up a leftover account from a previous run before registering.
    instance_conn.delete_user(user, debug=debug)

    success, msg = instance_conn.register_user(user, debug=debug)
    assert success, msg
    try:
        login_success, login_msg = user_conn.login(debug=debug)
        assert login_success, login_msg

        token = user_conn._token
        payload_segment = token.split('.')[1]
        payload = json.loads(
            base64.urlsafe_b64decode(payload_segment + '=' * (-len(payload_segment) % 4))
        )
        assert payload.get('scopes') == ['*']
        assert payload.get('type') == 'admin'
    finally:
        delete_success, delete_msg = instance_conn.delete_user(user, debug=debug)
    assert delete_success, delete_msg
