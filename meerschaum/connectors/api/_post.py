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
        **kw
    ):
    """
    Wrapper for requests.post
    """
    if use_token: headers.update({ 'Authorization': f'Bearer {self.token}' })

    return self.session.post(
        self.url + r_url,
        headers = headers,
        **kw
    )
