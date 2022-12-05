#! /usr/bin/env python
# -*- coding: utf-8 -*-
# vim:fenc=utf-8

"""
Wrappers for requests.delete
"""

from __future__ import annotations
from meerschaum.utils.typing import Optional, Dict, Any

def delete(
        self,
        r_url : str,
        headers : Optional[Dict[str, Any]] = None,
        use_token : bool = True,
        debug : bool = False,
        **kw : Ahy,
    ) -> requests.Response:
    """Wrapper for `requests.delete`."""
    if debug:
        from meerschaum.utils.debug import dprint
    
    if headers is None:
        headers = {}

    if use_token:
        if debug:
            dprint(f"Checking token...")
        headers.update({ 'Authorization': f'Bearer {self.token}' })

    if debug:
        from meerschaum.utils.formatting import pprint
        dprint(f"Sending DELETE request to {self.url + r_url}.")

    return self.session.delete(
        self.url + r_url,
        headers = headers,
        **kw
    )
