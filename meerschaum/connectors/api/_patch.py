#! /usr/bin/env python
# -*- coding: utf-8 -*-
# vim:fenc=utf-8

"""
Wrappers for requests.post
"""

def patch(
        self,
        r_url : str,
        headers : dict = {},
        use_token : bool = True,
        debug : bool = False,
        **kw
    ):
    """
    Wrapper for requests.patch
    """
    if debug:
        from meerschaum.utils.debug import dprint

    if use_token:
        if debug:
            dprint(f"Checking login token.")
        headers.update({ 'Authorization': f'Bearer {self.token}' })

    if debug:
        from meerschaum.utils.formatting import pprint
        dprint(f"Sending PATCH request to {self.url + r_url}")
        if headers:
            pprint(headers)
        pprint(kw)

    return self.session.patch(
        self.url + r_url,
        headers = headers,
        **kw
    )
