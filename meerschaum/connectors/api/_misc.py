#! /usr/bin/env python
# -*- coding: utf-8 -*-
# vim:fenc=utf-8

"""
Misc API routes
"""

def get_mrsm_version(self, **kw):
    """
    Return the API server's version.
    """
    from meerschaum.config.static import _static_config
    try:
        j = self.get(
            _static_config()['api']['endpoints']['version'] + '/mrsm',
            use_token = False,
            **kw
        ).json()
    except Exception as e:
        return None
    if isinstance(j, dict) and 'detail' in j:
        return None
    return j

