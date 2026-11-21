"""Core-only adapter from existing console page URLs to native confined page frames.

No plugin module is imported or rendered by this adapter. Catalog data and source authority
come from Main; the browser then uses its existing console session at the native page URL.
"""

from __future__ import annotations


def remote_layout(pathname, location_href=None):
    """Return (handled, layout) without changing the console's global plugin registry."""
    import urllib.parse
    from flask import current_app, request
    from dash import html
    from meerschaum.api.dash.sessions import (
        get_session_id_from_request, get_username_from_session,
    )
    from meerschaum.api.dash.access import viewer_for
    from meerschaum.api import no_auth
    from meerschaum.api.host._server_client import (
        SessionBoundBearerAuth, get_client, mint_session_bound_bearer,
    )

    if no_auth:
        return False, None
    client = get_client()
    if not client.available and not current_app.config.get('MRSM_SUPERVISED_HOST', True):
        # The in-process plugin SDK builds a standalone host with its own page registry.
        return False, None

    session_id = get_session_id_from_request(request)
    username = get_username_from_session(session_id) if session_id else None
    if not username:
        return False, None
    viewer = viewer_for(username)
    scopes = tuple(sorted(viewer.permissions))
    # Verify the live session credential before calling Main; never adopt a deleted/re-registered user.
    if not mint_session_bound_bearer(session_id, username=username, scopes=scopes, seconds=60):
        return True, html.P('Sign in again to view this plugin page.')
    if not client.available:
        return True, html.P('The confined plugin page service is unavailable.')
    auth = SessionBoundBearerAuth(session_id, username=username, scopes=scopes, seconds=60)
    try:
        # Classification comes only from Main's constructor-owned typed backend, never
        # from a Python isolation guess, environment marker or failed catalog response.
        policy = client.request_json(
            'GET',
            '/plugins/extensions/policy',
            auth=auth,
            timeout=5,
            max_bytes=1024,
        )
        if not isinstance(policy, dict) or set(policy) != {'confined'} or type(policy['confined']) is not bool:
            raise ValueError('Invalid native page policy.')
        if not policy['confined']:
            return False, None
        if not viewer.can('plugins:read'):
            return True, html.P('This plugin page is not available to your account.')
        # The destination is Main's own fixed host configuration, never a page-provided URL.
        # UrllibTransport bypasses environment proxies and refuses redirects.
        catalog = client.request_json(
            'GET',
            '/plugins/extensions',
            auth=auth,
            timeout=45,
            max_bytes=2 * 1024 * 1024,
        )
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
