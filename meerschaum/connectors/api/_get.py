#! /usr/bin/env python
# -*- coding: utf-8 -*-
# vim:fenc=utf-8

"""
Wrappers for requests.get
"""

from __future__ import annotations
from meerschaum.utils.typing import Optional, Any, Dict, Union

def get(
        self,
        r_url : str,
        headers : Dict[str, str] = {},
        use_token : bool = True,
        debug : bool = False,
        **kw : Any
    ) -> requests.Reponse:
    """
    Wrapper for requests.get
    """
    if debug:
        from meerschaum.utils.debug import dprint

    if use_token:
        if debug:
            dprint(f"Checking login token.")
        headers.update({ 'Authorization': f'Bearer {self.token}' })

    if debug:
        from meerschaum.utils.formatting import pprint
        dprint(f"Sending GET request to {self.url + r_url}.")
        pprint(headers)
        pprint(kw)

    return self.session.get(
        self.url + r_url,
        headers = headers,
        **kw
    )

def wget(
        self,
        r_url : str,
        dest : Optional[Union[str, pathlib.Path]] = None,
        **kw : Any
    ) -> pathlib.Path:
    """
    Mimic wget with requests.
    """
    from meerschaum.utils.misc import wget
    return wget(self.url + r_url, dest=dest, **kw)
