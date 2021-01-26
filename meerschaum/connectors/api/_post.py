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
    from meerschaum.utils.debug import dprint
    from meerschaum.utils.formatting import pprint
    if use_token:
        if debug: dprint(f"Fetching token...")
        headers.update({ 'Authorization': f'Bearer {self.token}' })

    if debug:
        dprint(f"Posting arguments to '{self.url + r_url}'")
        pprint(kw)
        if headers: pprint(headers)

    return self.session.post(
        self.url + r_url,
        headers = headers,
        **kw
    )
