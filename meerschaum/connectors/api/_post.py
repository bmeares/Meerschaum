#! /usr/bin/env python
# -*- coding: utf-8 -*-
# vim:fenc=utf-8

"""
Wrappers for requests.post
"""

def post(
        self,
        r_url : str,
        headers : dict = {},
        use_token : bool = True,
        debug : bool = False,
        **kw
    ):
    """
    Wrapper for requests.post
    """
    if debug:
        from meerschaum.utils.debug import dprint

    if use_token:
        if debug:
            dprint(f"Checking token...")
        headers.update({ 'Authorization': f'Bearer {self.token}' })

    if debug:
        from meerschaum.utils.formatting import pprint
        dprint(f"Sending POST request to {self.url + r_url}")
        if headers:
            pprint(headers)
        pprint(kw)

    return self.session.post(
        self.url + r_url,
        headers = headers,
        **kw
    )
