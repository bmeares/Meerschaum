#! /usr/bin/env python
# -*- coding: utf-8 -*-
# vim:fenc=utf-8

"""
Wrappers for requests.get
"""

import requests

def get(
        self,
        r_url : str,
        **kw
    ):
    """
    Wrapper for requests.get
    """
    if 'auth' in kw:
        from meerschaum.utils.warnings import warn
        warn('Ignoring auth, using existing configuration')
        del kw['auth']
    return requests.get(self.url + r_url, auth=self.auth, **kw)

