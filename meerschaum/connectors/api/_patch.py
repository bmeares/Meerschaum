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
        **kw
    ):
    """
    Wrapper for requests.patch
    """
    if use_token: headers.update({ 'Authorization': f'Bearer {self.token}' })

    return self.session.patch(
        self.url + r_url,
        headers = headers,
        **kw
    )
