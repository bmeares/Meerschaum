#! /usr/bin/env python
# -*- coding: utf-8 -*-
# vim:fenc=utf-8

"""
Make requests to the API (the Rust client; no `requests` import).
"""

from __future__ import annotations

import pathlib
import urllib.parse

from meerschaum.utils.typing import Optional, Dict, Any, Union
from meerschaum.utils.debug import dprint

METHODS = {'GET', 'OPTIONS', 'HEAD', 'POST', 'PUT', 'PATCH', 'DELETE'}


def make_request(
    self,
    method: str,
    r_url: str,
    headers: Optional[Dict[str, Any]] = None,
    use_token: bool = True,
    debug: bool = False,
    **kwargs: Any
) -> 'meerschaum._core.ApiResponse':
    """
    Make a request to the API and return a response object
    (`status_code`, `ok`, `text`, `content`, `headers`, `json()`, `raise_for_status()`).

    Parameters
    ----------
    method: str
        The name of the request method (e.g. `'GET'`, `'POST'`).

    r_url: str
        The relative URL for the request (e.g. `'/pipes'`).

    headers: Optional[Dict[str, Any]], default None
        Headers to override the default headers.

    use_token: bool, default True
        If `True`, add the authorization token to the headers.

    debug: bool, default False
        Verbosity toggle.

    kwargs: Any
        `params` (the query), `json`, `data`, `files` and `timeout` are sent as `requests` would.
    """
    if method.upper() not in METHODS:
        raise ValueError(f"Method '{method}' is not supported.")

    if debug:
        dprint(f"[{self}] Sending a '{method.upper()}' request to {self.url}{r_url}")

    return self._rust_client().make_request(
        method,
        r_url,
        headers=headers,
        use_token=use_token,
        **kwargs
    )


def get(self, r_url: str, **kwargs: Any) -> 'meerschaum._core.ApiResponse':
    """
    Send a `GET` request (see `make_request()`).
    """
    return self.make_request('GET', r_url, **kwargs)


def post(self, r_url: str, **kwargs: Any) -> 'meerschaum._core.ApiResponse':
    """
    Send a `POST` request (see `make_request()`).
    """
    return self.make_request('POST', r_url, **kwargs)


def patch(self, r_url: str, **kwargs: Any) -> 'meerschaum._core.ApiResponse':
    """
    Send a `PATCH` request (see `make_request()`).
    """
    return self.make_request('PATCH', r_url, **kwargs)


def put(self, r_url: str, **kwargs: Any) -> 'meerschaum._core.ApiResponse':
    """
    Send a `PUT` request (see `make_request()`).
    """
    return self.make_request('PUT', r_url, **kwargs)


def delete(self, r_url: str, **kwargs: Any) -> 'meerschaum._core.ApiResponse':
    """
    Send a `DELETE` request (see `make_request()`).
    """
    return self.make_request('DELETE', r_url, **kwargs)


def wget(
    self,
    r_url: str,
    dest: Optional[Union[str, pathlib.Path]] = None,
    headers: Optional[Dict[str, Any]] = None,
    use_token: bool = True,
    debug: bool = False,
    **kw: Any
) -> pathlib.Path:
    """Mimic wget: download a resource of the API (with the token) to `dest`."""
    from meerschaum.utils.misc import wget
    if headers is None:
        headers = {}
    if use_token:
        headers.update({'Authorization': f'Bearer {self.token}'})
    request_url = urllib.parse.urljoin(self.url, r_url)
    if debug:
        dprint(
            f"[{self}] Downloading {request_url}"
            + (f' to {dest}' if dest is not None else '')
            + "..."
        )
    return wget(request_url, dest=dest, headers=headers, **kw)
