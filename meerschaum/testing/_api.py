#! /usr/bin/env python3
# -*- coding: utf-8 -*-
# vim:fenc=utf-8

"""
The API host of a test session in the test process (`mrsm_api`), and console pages rendered for
a test (`render_page`).
"""

from __future__ import annotations

import json
import os
import secrets
import sys
import uuid
from datetime import timedelta
from typing import Any, Dict, List, Optional, Set, Union

from meerschaum.testing._root import MrsmRoot

#: The instance the session's API serves (`sql:<label>`, a SQLite file in the root).
API_INSTANCE_LABEL: str = 'api_test'
SESSION_COOKIE: str = 'mrsm-session-id'


class CallbackError(AssertionError):
    """A console callback answered with an error."""

    def __init__(self, status: int, body: str):
        super().__init__(f"The callback answered {status}: {body[:2000]}")
        self.status = status
        self.body = body


def _purge_api_modules() -> None:
    """
    Forget `meerschaum.api` imported outside the API host, so that it imports again in host mode.
    """
    for name in [name for name in sys.modules if name == 'meerschaum.api' or name.startswith('meerschaum.api.')]:
        sys.modules.pop(name, None)
    import meerschaum
    vars(meerschaum).pop('api', None)


class MrsmApi:
    """
    The API host app (`@api_plugin` routes, plugin MCP tools and the web console with the plugins'
    pages) built in this process for the test session, on its own SQLite instance.

    Attributes
    ----------
    app: fastapi.FastAPI
        The host's app.

    client: starlette.testclient.TestClient
        A client without credentials.

    instance: SQLConnector
        The API's instance (`sql:api_test`), where its users and sessions live.
    """

    def __init__(self, root: MrsmRoot, no_auth: bool = False):
        self.root = root
        self.no_auth = no_auth
        self.app: Any = None
        self.client: Any = None
        self.instance: Any = None
        self.passwords: Dict[str, str] = {}

    def start(self) -> 'MrsmApi':
        """Build the app (once)."""
        if self.app is not None:
            return self
        api = sys.modules.get('meerschaum.api', None)
        if api is not None and not getattr(api, '_host_mode', False):
            _purge_api_modules()
        self.instance = self.root.sqlite_instance(
            API_INSTANCE_LABEL, self.root.path / 'sqlite' / 'api_test.db',
        )
        os.environ['MRSM_API_HOST'] = '1'
        os.environ['MRSM_API_HOST_CONFIG'] = json.dumps({
            'mrsm_instance': f'sql:{API_INSTANCE_LABEL}',
            'no_auth': bool(self.no_auth),
        })
        from meerschaum.api.host import build_app
        from starlette.testclient import TestClient
        self.app = build_app()
        self.client = TestClient(self.app)
        return self

    @property
    def console_mounted(self) -> bool:
        """Whether the web console is served (`meerschaum[dash]` is installed)."""
        from meerschaum.api.host.dash import console_status
        return bool(console_status().get('mounted', False))

    ### Users, sessions and tokens

    def user(self, username: str = 'admin', password: Optional[str] = None, admin: Optional[bool] = None):
        """
        Return the user `username` of the API's instance, registering it first if needed (an admin
        when `admin`, which defaults to whether the name is `admin`).
        """
        from meerschaum.core import User
        conn = self.instance
        admin = (username == 'admin') if admin is None else admin
        probe = User(username, instance=conn)
        user_id = conn.get_user_id(probe)
        if user_id is None:
            password = password or self.passwords.get(username) or secrets.token_urlsafe(16)
            new_user = User(
                username,
                password,
                type=('admin' if admin else None),
                instance=conn,
            )
            success, message = conn.register_user(new_user)
            if not success:
                raise RuntimeError(f"Could not register the test user '{username}': {message}")
            self.passwords[username] = password
            user_id = conn.get_user_id(probe)
        user = User(username, instance=conn)
        user._user_id = user_id
        return user

    def session(self, username: str = 'admin', admin: Optional[bool] = None) -> str:
        """
        Sign `username` in to the web console (registering the user if needed); return the
        session ID (the `mrsm-session-id` cookie).
        """
        from meerschaum.api.dash.sessions import set_session
        self.user(username, admin=admin)
        session_id = str(uuid.uuid4())
        set_session(session_id, {'username': username})
        return session_id

    def token(self, username: str = 'admin', scopes: Optional[List[str]] = None, minutes: int = 60) -> str:
        """
        Return a JWT for `username` (as `/login` issues), for `Authorization: Bearer <token>`.
        """
        from meerschaum.api._oauth2 import manager
        self.user(username)
        return manager.create_access_token(
            data={'sub': username},
            expires=timedelta(minutes=minutes),
            scopes=list(scopes or ['*']),
        )

    def client_for(self, username: str = 'admin', admin: Optional[bool] = None):
        """
        Return a client signed in as `username`: a console session cookie and a bearer JWT.
        """
        from starlette.testclient import TestClient
        client = TestClient(self.app)
        client.cookies.set(SESSION_COOKIE, self.session(username, admin=admin))
        client.headers['Authorization'] = 'Bearer ' + self.token(username)
        return client


def _component_ids(node: Any, out: Set[str]) -> None:
    if isinstance(node, list):
        for item in node:
            _component_ids(item, out)
        return
    if not isinstance(node, dict):
        return
    props = node.get('props', None)
    if isinstance(props, dict):
        component_id = props.get('id', None)
        if isinstance(component_id, str):
            out.add(component_id)
        elif isinstance(component_id, dict):
            out.add(json.dumps(component_id, sort_keys=True, separators=(',', ':')))
        for value in props.values():
            _component_ids(value, out)
    else:
        for value in node.values():
            _component_ids(value, out)


def _texts(node: Any, out: List[str]) -> None:
    if isinstance(node, str):
        out.append(node)
        return
    if isinstance(node, (int, float)) and not isinstance(node, bool):
        out.append(str(node))
        return
    if isinstance(node, list):
        for item in node:
            _texts(item, out)
        return
    if isinstance(node, dict):
        props = node.get('props', None)
        if isinstance(props, dict):
            for key in ('children', 'label', 'placeholder', 'title', 'value'):
                if key in props:
                    _texts(props[key], out)
            for key, value in props.items():
                if key not in ('children', 'label', 'placeholder', 'title', 'value') and isinstance(value, (dict, list)):
                    _texts(value, out)


def _find(node: Any, component_id: Any) -> Optional[Dict[str, Any]]:
    if isinstance(node, list):
        for item in node:
            found = _find(item, component_id)
            if found is not None:
                return found
        return None
    if not isinstance(node, dict):
        return None
    props = node.get('props', None)
    if isinstance(props, dict):
        if props.get('id', None) == component_id:
            return node
        for value in props.values():
            found = _find(value, component_id)
            if found is not None:
                return found
    return None


def _output_parts(output: str) -> List[str]:
    """The `id.property` parts of a callback's output key (without `@<hash>` suffixes)."""
    parts = output.strip('.').split('...') if output.startswith('..') else [output]
    return [part.split('@')[0] for part in parts]


class RenderedPage:
    """
    A console or plugin page as a browser with the test's session sees it.

    Attributes
    ----------
    path: str
        The page's path (`/dash/...`).

    kind: str
        `'dash'` (a Dash layout) or `'html'` (a page-contract fragment).

    status: int
        The HTTP status of the page's request.

    layout: Any
        The Dash layout as JSON (`'dash'` pages).

    html: str
        The page's HTML (`'html'` pages).

    text: str
        The page's text, for easy assertions.

    ids: Set[str]
        The IDs of the page's components (`'dash'` pages).

    session_id: Optional[str]
        The console session the page was rendered for, if any.
    """

    def __init__(self, api: MrsmApi, path: str, client: Any, session_id: Optional[str]):
        self.api = api
        self.path = path
        self.client = client
        self.session_id = session_id
        self.kind = 'dash'
        self.status = 0
        self.layout: Any = None
        self.html: str = ''
        self.text: str = ''
        self.ids: Set[str] = set()
        self._dependencies: Optional[List[Dict[str, Any]]] = None

    def __repr__(self) -> str:
        return f"RenderedPage('{self.path}', kind='{self.kind}', status={self.status})"

    def __contains__(self, text: str) -> bool:
        return text in self.text or text in self.html

    def find(self, component_id: Any) -> Optional[Dict[str, Any]]:
        """Return the component (JSON) with this ID, or `None`."""
        return _find(self.layout, component_id)

    @property
    def dependencies(self) -> List[Dict[str, Any]]:
        """The console's callbacks (`/dash/_dash-dependencies`)."""
        if self._dependencies is None:
            self._dependencies = self.client.get('/dash/_dash-dependencies').json()
        return self._dependencies

    def _dependency(self, output: str) -> Dict[str, Any]:
        for dependency in self.dependencies:
            parts = _output_parts(dependency['output'])
            if output in parts or ('.' not in output and any(part.split('.')[0] == output for part in parts)):
                return dependency
        raise KeyError(f"No callback writes '{output}'.")

    def _values(
        self,
        specs: List[Dict[str, Any]],
        given: Union[Dict[str, Any], List[Dict[str, Any]], None],
    ) -> List[Dict[str, Any]]:
        if isinstance(given, list):
            return given
        given = dict(given or {})
        out = []
        for spec in specs:
            component_id, prop = spec['id'], spec['property']
            key = f"{component_id}.{prop}"
            if component_id.startswith('{'):
                component_id = json.loads(component_id)
            if key in given:
                value = given[key]
            elif component_id == 'session-store' and prop == 'data':
                value = {'session-id': self.session_id} if self.session_id else {}
            elif component_id == 'mrsm-location' and prop == 'pathname':
                value = self.path
            elif component_id == 'mrsm-location' and prop == 'href':
                value = 'http://localhost' + self.path
            else:
                value = None
            out.append({'id': component_id, 'property': prop, 'value': value})
        return out

    def callback(
        self,
        output: str,
        inputs: Union[Dict[str, Any], List[Dict[str, Any]], None] = None,
        state: Union[Dict[str, Any], List[Dict[str, Any]], None] = None,
        trigger: Optional[str] = None,
    ) -> Optional[Dict[str, Any]]:
        """
        Fire the callback that writes `output` (`'my-div.children'`, or a component ID), as the
        browser would, with this page's session.

        Parameters
        ----------
        output: str
            The output (`'id.property'`) or the component ID of the callback.

        inputs: Union[Dict[str, Any], List[Dict[str, Any]], None], default None
            Input values by `'id.property'` (others are `None`; the session's `session-store` and
            this page's location are filled in), or the raw `inputs` list Dash sends.

        state: Union[Dict[str, Any], List[Dict[str, Any]], None], default None
            State values, like `inputs`.

        trigger: Optional[str], default None
            The input that changed (`'id.property'`); defaults to the first one given.

        Returns
        -------
        The callback's response (`{component_id: {property: value}}`), or `None` when it did not
        update anything. Raises `CallbackError` when the console answered with an error (for
        example `401` without a session for a callback that is not public).
        """
        dependency = self._dependency(output)
        input_values = self._values(dependency.get('inputs', []), inputs)
        state_values = self._values(dependency.get('state', []), state)
        if trigger is None:
            if isinstance(inputs, dict) and inputs:
                trigger = next(iter(inputs))
            elif input_values:
                first = input_values[0]
                first_id = first['id'] if isinstance(first['id'], str) else json.dumps(first['id'])
                trigger = f"{first_id}.{first['property']}"
        outputs = []
        for part in _output_parts(dependency['output']):
            component_id, _, prop = part.rpartition('.')
            outputs.append({
                'id': json.loads(component_id) if component_id.startswith('{') else component_id,
                'property': prop,
            })
        payload = {
            'output': dependency['output'],
            'outputs': outputs if dependency['output'].startswith('..') else outputs[0],
            'inputs': input_values,
            'changedPropIds': [trigger] if trigger else [],
            'state': state_values,
        }
        response = self.client.post('/dash/_dash-update-component', json=payload)
        if response.status_code == 204:
            return None
        if response.status_code != 200:
            raise CallbackError(response.status_code, response.text)
        return response.json().get('response', None)


def render_page(
    api: MrsmApi,
    path: str,
    as_user: Optional[str] = None,
    admin: Optional[bool] = None,
) -> RenderedPage:
    """
    Render a console or plugin page for `as_user` (signed in; anonymous when `None`).
    """
    from starlette.testclient import TestClient
    console_path = path if path.startswith('/dash') else '/dash/' + path.lstrip('/')
    console_path = console_path.rstrip('/') or '/dash'
    client = TestClient(api.app)
    session_id = api.session(as_user, admin=admin) if as_user else None
    if session_id:
        client.cookies.set(SESSION_COOKIE, session_id)
    page = RenderedPage(api, console_path, client, session_id)

    from meerschaum.api.host.dash import get_page_contract_pages, page_route
    if console_path in get_page_contract_pages():
        response = client.get(page_route(console_path))
        page.kind = 'html'
        page.status = response.status_code
        page.html = response.text
        page.text = page.html
        return page

    try:
        response = page.callback(
            'page-layout-div.children',
            inputs={'mrsm-location.pathname': console_path},
            trigger='mrsm-location.pathname',
        )
        page.status = 200
    except CallbackError as e:
        page.status = e.status
        page.html = e.body
        return page
    page.layout = (response or {}).get('page-layout-div', {}).get('children', None)
    ids: Set[str] = set()
    _component_ids(page.layout, ids)
    page.ids = ids
    texts: List[str] = []
    _texts(page.layout, texts)
    page.text = '\n'.join(texts)
    return page
