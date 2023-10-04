#! /usr/bin/env python3
# -*- coding: utf-8 -*-
# vim:fenc=utf-8

"""
Entrypoint for making requests.
"""

import copy
import urllib.parse
import pathlib
from meerschaum.utils.typing import Any, Optional, Dict, Union
from meerschaum.utils.debug import dprint
from meerschaum.utils.formatting import pprint

METHODS = {
    'GET',
    'OPTIONS',
    'HEAD',
    'POST',
    'PUT',
    'PATCH',
    'DELETE',
}

def make_request(
        self,
        method: str,
        r_url: str,
        headers: Optional[Dict[str, Any]] = None,
        use_token: bool = True,
        debug: bool = False,
        **kwargs: Any
    ) -> 'requests.Response':
    """
    Make a request to this APIConnector's endpoint using the in-memory session.

    Parameters
    ----------
    method: str
        The kind of request to make.
        Accepted values:
        - `'GET'`
        - `'OPTIONS'`
        - `'HEAD'`
        - `'POST'`
        - `'PUT'`
        - `'PATCH'`
        - `'DELETE'`

    r_url: str
        The relative URL for the endpoint (e.g. `'/pipes'`).

    headers: Optional[Dict[str, Any]], default None
        The headers to use for the request.
        If `use_token` is `True`, the authorization token will be added to a copy of these headers.

    use_token: bool, default True
        If `True`, add the authorization token to the headers.

    debug: bool, default False
        Verbosity toggle.

    kwargs: Any
        All other keyword arguments are passed to `requests.request`.

    Returns
    -------
    A `requests.Reponse` object.
    """
    if method.upper() not in METHODS:
        raise ValueError(f"Method '{method}' is not supported.")

    verify = self.__dict__.get('verify', None)
    if 'verify' not in kwargs and isinstance(verify, bool):
        kwargs['verify'] = verify

    headers = (
        copy.deepcopy(headers)
        if isinstance(headers, dict)
        else {}
    )

    if use_token:
        headers.update({'Authorization': f'Bearer {self.token}'})

    request_url = urllib.parse.urljoin(self.url, r_url)
    if debug:
        dprint(f"[{self}] Sending a '{method.upper()}' request to {request_url}")

    return self.session.request(
        method.upper(),
        request_url,
        headers = headers,
        **kwargs
    )


def get(self, r_url: str, **kwargs: Any) -> 'requests.Response':
    """
    Wrapper for `requests.get`.

    Parameters
    ----------
    r_url: str
        The relative URL for the endpoint (e.g. `'/pipes'`).

    headers: Optional[Dict[str, Any]], default None
        The headers to use for the request.
        If `use_token` is `True`, the authorization token will be added to a copy of these headers.

    use_token: bool, default True
        If `True`, add the authorization token to the headers.

    debug: bool, default False
        Verbosity toggle.

    kwargs: Any
        All other keyword arguments are passed to `requests.request`.

    Returns
    -------
    A `requests.Reponse` object.

    """
    return self.make_request('GET', r_url, **kwargs)


def post(self, r_url: str, **kwargs: Any) -> 'requests.Response':
    """
    Wrapper for `requests.post`.

    Parameters
    ----------
    r_url: str
        The relative URL for the endpoint (e.g. `'/pipes'`).

    headers: Optional[Dict[str, Any]], default None
        The headers to use for the request.
        If `use_token` is `True`, the authorization token will be added to a copy of these headers.

    use_token: bool, default True
        If `True`, add the authorization token to the headers.

    debug: bool, default False
        Verbosity toggle.

    kwargs: Any
        All other keyword arguments are passed to `requests.request`.

    Returns
    -------
    A `requests.Reponse` object.

    """
    return self.make_request('POST', r_url, **kwargs)


def patch(self, r_url: str, **kwargs: Any) -> 'requests.Response':
    """
    Wrapper for `requests.patch`.

    Parameters
    ----------
    r_url: str
        The relative URL for the endpoint (e.g. `'/pipes'`).

    headers: Optional[Dict[str, Any]], default None
        The headers to use for the request.
        If `use_token` is `True`, the authorization token will be added to a copy of these headers.

    use_token: bool, default True
        If `True`, add the authorization token to the headers.

    debug: bool, default False
        Verbosity toggle.

    kwargs: Any
        All other keyword arguments are passed to `requests.request`.

    Returns
    -------
    A `requests.Reponse` object.
    """
    return self.make_request('PATCH', r_url, **kwargs)


def put(self, r_url: str, **kwargs: Any) -> 'requests.Response':
    """
    Wrapper for `requests.put`.

    Parameters
    ----------
    r_url: str
        The relative URL for the endpoint (e.g. `'/pipes'`).

    headers: Optional[Dict[str, Any]], default None
        The headers to use for the request.
        If `use_token` is `True`, the authorization token will be added to a copy of these headers.

    use_token: bool, default True
        If `True`, add the authorization token to the headers.

    debug: bool, default False
        Verbosity toggle.

    kwargs: Any
        All other keyword arguments are passed to `requests.request`.

    Returns
    -------
    A `requests.Reponse` object.
    """
    return self.make_request('PUT', r_url, **kwargs)


def delete(self, r_url: str, **kwargs: Any) -> 'requests.Response':
    """
    Wrapper for `requests.delete`.

    Parameters
    ----------
    r_url: str
        The relative URL for the endpoint (e.g. `'/pipes'`).

    headers: Optional[Dict[str, Any]], default None
        The headers to use for the request.
        If `use_token` is `True`, the authorization token will be added to a copy of these headers.

    use_token: bool, default True
        If `True`, add the authorization token to the headers.

    debug: bool, default False
        Verbosity toggle.

    kwargs: Any
        All other keyword arguments are passed to `requests.request`.

    Returns
    -------
    A `requests.Reponse` object.
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
    """Mimic wget with requests.
    """
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
