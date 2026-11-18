"""Core-only adapter from existing console page URLs to native confined page frames.

No plugin module is imported or rendered by this adapter. Catalog data and source authority
come from Main; the browser then uses its existing console session at the native page URL.
"""

from __future__ import annotations


def remote_layout(pathname, location_href=None):
    """Return (handled, layout) without changing the console's global plugin registry."""
    import json
    import urllib.parse
    import urllib.request
    from flask import request
    from dash import html
    from meerschaum.api.dash.sessions import (
        get_session_id_from_request, get_username_from_session, get_session_data,
    )
    from meerschaum.api.dash.access import viewer_for
    from meerschaum.api import no_auth

    if no_auth:
        return False, None
    from meerschaum.api.dash.execute import server_url

    session_id = get_session_id_from_request(request)
    username = get_username_from_session(session_id) if session_id else None
    if not username:
        return False, None
    viewer = viewer_for(username)
    base = server_url()
    from datetime import timedelta
    from meerschaum.api._oauth2 import manager, CREDENTIAL_CLAIM, PRINCIPAL_CLAIM
    current = get_session_data(session_id, use_cache=False)
    if not isinstance(current, dict) or current.get('username') != username or not current.get('credential'):
        return True, html.P('Sign in again to view this plugin page.')
    # Bind the backchannel to this exact original console credential. Looking up a fresh
    # password binding by username could adopt a deleted/re-registered user during a wait.
    bearer = manager.create_access_token(
        data={'sub': username, PRINCIPAL_CLAIM: 'user', CREDENTIAL_CLAIM: current['credential']},
        expires=timedelta(seconds=60), scopes=sorted(viewer.permissions),
    )
    if not base or not bearer:
        return True, html.P('The confined plugin page service is unavailable.')
    try:
        class NoRedirect(urllib.request.HTTPRedirectHandler):
            def redirect_request(self, *args, **kwargs):
                return None

        # Classification comes only from Main's constructor-owned typed backend, never
        # from a Python isolation guess, environment marker or failed catalog response.
        opener = urllib.request.build_opener(urllib.request.ProxyHandler({}), NoRedirect())
        policy_request = urllib.request.Request(
            base + '/plugins/extensions/policy',
            headers={'accept': 'application/json', 'authorization': 'Bearer ' + bearer},
            method='GET',
        )
        with opener.open(policy_request, timeout=5) as response:
            policy_body = response.read(1025)
        if len(policy_body) > 1024:
            raise ValueError('The page policy exceeds its byte bound.')
        policy = json.loads(policy_body.decode('utf-8'))
        if not isinstance(policy, dict) or set(policy) != {'confined'} or type(policy['confined']) is not bool:
            raise ValueError('Invalid native page policy.')
        if not policy['confined']:
            return False, None
        if not viewer.can('plugins:read'):
            return True, html.P('This plugin page is not available to your account.')
        query = urllib.request.Request(
            base + '/plugins/extensions',
            headers={'accept': 'application/json', 'authorization': 'Bearer ' + bearer},
            method='GET',
        )
        # The destination is Main's own fixed host configuration, never a page-provided URL.
        # A corporate/environment proxy must not receive this local authentication.
        with opener.open(query, timeout=45) as response:
            encoded = response.read(2 * 1024 * 1024 + 1)
        if len(encoded) > 2 * 1024 * 1024:
            raise ValueError('The page catalog exceeds its byte bound.')
        catalog = json.loads(encoded.decode('utf-8'))
        path = str(pathname).removeprefix('/dash/').strip('/')
        matching = []
        for plugin in catalog['plugins'].values():
            registry = (plugin.get('metadata') or {}).get('registry')
            if not isinstance(registry, dict):
                continue
            pages = registry.get('pages') or {}
            if pages.get('state') != 'complete':
                continue
            matching.extend(
                entry for entry in pages.get('entries', ())
                if (entry.get('selector') or {}).get('kind') == 'page'
                and entry['selector'].get('path') == path
            )
        if len(matching) != 1:
            return True, html.P('This plugin page is not declared in your current source view.')
        uri = '/plugins/extensions/pages/' + urllib.parse.quote(path, safe='/')
        query = urllib.parse.urlsplit(location_href or '').query
        if len(query) > 8192:
            raise ValueError('The page query exceeds its byte bound.')
        if query:
            uri += '?' + query
        return True, html.Iframe(
            src=uri, title='Plugin page', className='mrsm-contract-frame',
            style={'width': '100%', 'height': 'calc(100vh - 80px)', 'border': '0'},
        )
    except Exception:
        # Never execute a global Main plugin page when the source-bound service failed.
        return True, html.P('The confined plugin page is unavailable. Try again shortly.')
