#! /usr/bin/env python3
# vim:fenc=utf-8

"""
Implement the `APIConnector` token methods.
"""

import meerschaum as mrsm
from meerschaum.core import Token
from meerschaum._internal.static import STATIC_CONFIG
tokens_endpoint = STATIC_CONFIG['api']['endpoints']['tokens']


def register_token(self, token: Token, debug: bool = False) -> mrsm.SuccessTuple:
    """
    Register the provided token to the API.
    """
    r_url = tokens_endpoint + '/register'
    self.post(r_url, debug=debug)

    return False, "Not implemented."
