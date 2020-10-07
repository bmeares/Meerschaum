#! /usr/bin/env python
# -*- coding: utf-8 -*-
# vim:fenc=utf-8

"""
Wrappers for requests.post
"""

import requests

def patch(
        self,
        r_url : str,
        **kw
    ):
    """
    Wrapper for requests.post
    """
    if 'auth' in kw:
        print('Ignoring auth, using existing configuration')
        del kw['auth']
    return requests.patch(self.url + r_url, auth=self.auth, **kw)
