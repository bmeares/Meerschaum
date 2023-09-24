#! /usr/bin/env python3
# -*- coding: utf-8 -*-
# vim:fenc=utf-8

"""
Test user registration, deletion, and more.
"""

import pytest
import datetime
from tests import debug
from tests.pipes import all_pipes, stress_pipes, remote_pipes
from tests.connectors import conns, get_flavors
from meerschaum.core import User

@pytest.mark.parametrize("flavor", get_flavors())
def test_register_user(flavor: str):
    username, password, email = conns['api'].username, conns['api'].password, 'none@none.com'
    user = User(username, password, email=email)
    conn = conns[flavor]
    #  conn.delete_user(user, debug=debug)
    conn.register_user(user, debug=debug)
    #  success, msg = conn.register_user(user, debug=debug)
    #  assert success, msg
