"""Publish installed plugins with repository credentials private to a console session."""
from __future__ import annotations

import copy
import threading
import time
import uuid
from types import SimpleNamespace

from dash import dcc, html
import dash_bootstrap_components as dbc

from meerschaum.api.dash.plugin_sources import may_read_source, source_viewer

_AUTH_FIELDS = ('username', 'password', 'client_id', 'client_secret', 'api_key')
_CONNECTIONS = {}
_LOCK = threading.Lock()
_SESSION_SECONDS = 3600


class PublishError(ValueError):
    """A local validation error whose message is safe to show in the dialog."""


def repositories(session):
    """Configured API repositories visible to an administrator; no credentials go to Dash."""
    if not may_read_source('installed', session):
        return []
    from meerschaum.api.dash.connector_listing import list_connectors
    return [row['keys'] for row in list_connectors(source_viewer(session)) if row['type'] == 'api']


def _key(context, repository, session):
    if not isinstance(context, dict) or not may_read_source('installed', session):
        raise PermissionError('Only administrators can publish installed plugins.')
    from meerschaum.api.dash.plugins import _discover_plugin_files
    if context.get('name') not in _discover_plugin_files():
        raise PublishError('This plugin is not installed.')
    if repository not in repositories(session):
        raise PublishError('Select a configured API repository.')
    from meerschaum.api.dash.sessions import valid_session_id
    session_id = valid_session_id((session or {}).get('session-id'))
    # A no-auth console still keeps different dialogs' remote credentials separate.
    nonce = valid_session_id(context.get('nonce'))
    if not session_id and not (source_viewer(session).no_auth and nonce):
        raise PermissionError('Sign in before publishing a plugin.')
    return session_id or nonce, repository


def _connection(context, repository, session, credentials=None):
    import meerschaum as mrsm
    key = _key(context, repository, session)
    configured = mrsm.get_connector(repository)
    if configured is None:
        raise PublishError('The selected API repository is unavailable.')
    now = time.monotonic()
    with _LOCK:
        for expired in [k for k, (until, _, _) in _CONNECTIONS.items() if until <= now]:
            _CONNECTIONS.pop(expired, None)
        cached = _CONNECTIONS.get(key)
    if credentials is None and cached and cached[1] == configured.url:
        return key, cached[2], configured.url
    # Copying resets the native HTTP client. Never change the globally cached connector.
    conn = copy.copy(configured)
    conn.connect_timeout = 10
    conn.timeout = 30
    if credentials is not None:
        for field in _AUTH_FIELDS:
            conn.__dict__.pop(field, None)
        # The parsed host / port / protocol remain; the URI must not restore old credentials.
        conn.__dict__.pop('uri', None)
        conn.__dict__.update(credentials)
    elif configured._token:
        conn._token = configured._token
        conn._expires = configured._expires
    return key, conn, configured.url


def _remember(key, conn, url):
    with _LOCK:
        if len(_CONNECTIONS) >= 128:
            oldest = min(_CONNECTIONS, key=lambda k: _CONNECTIONS[k][0])
            _CONNECTIONS.pop(oldest, None)
        _CONNECTIONS[key] = (time.monotonic() + _SESSION_SECONDS, url, conn)


def _authenticated(conn):
    if conn._token:
        response = conn.get('/users/me', timeout=10)
        if response.ok:
            return True
    if any(conn.__dict__.get(field) for field in _AUTH_FIELDS):
        return bool(conn.login(warn=False)[0])
    return False


def repository_status(context, repository, session):
    try:
        key, conn, url = _connection(context, repository, session)
        if _authenticated(conn):
            _remember(key, conn, url)
            return True, f'Ready to publish to {repository}.'
        return False, f'Sign in to {repository} to publish.'
    except (PublishError, PermissionError) as exc:
        return False, str(exc)
    except Exception:
        return False, 'Could not connect to this repository. Check its connection settings and try again.'


def login_repository(context, repository, session, method, username, password,
                     client_id, client_secret, api_key):
    fields = {
        'password': {'username': username, 'password': password},
        'client_credentials': {'client_id': client_id, 'client_secret': client_secret},
        'api_key': {'api_key': api_key},
    }.get(method)
    if not fields or not all(isinstance(value, str) and value.strip() for value in fields.values()):
        return False, 'Fill in the credentials for the selected sign-in method.'
    try:
        key, conn, url = _connection(context, repository, session, fields)
        if not conn.login(warn=False)[0]:
            return False, 'Sign-in failed. Check your credentials and try again.'
        _remember(key, conn, url)
        return True, f'Ready to publish to {repository}.'
    except (PublishError, PermissionError) as exc:
        return False, str(exc)
    except Exception:
        return False, 'Could not sign in to this repository. Check its connection settings and try again.'


def publish(context, repository, session):
    """Package current source with the CLI's archiver, then upload using the API client."""
    try:
        key, conn, url = _connection(context, repository, session)
        if not _authenticated(conn):
            return False, 'Sign in to this repository before publishing.'
        from meerschaum import _core
        package = _core.plugin_archive(context['name'])
        plugin = SimpleNamespace(name=context['name'], version=package['version'], attributes={})
        previous = conn.get_plugin_attributes(plugin)
        description = (previous or {}).get('description') or package['description']
        if description:
            plugin.attributes['description'] = description
        ok, message = conn._rust_client().register_plugin(
            plugin, package['archive'], context['name'] + '.tar.gz',
        )
        _remember(key, conn, url)
        if ok:
            return True, f"Published '{context['name']}' to {repository}."
        from meerschaum.api.dash.connector_listing import collect_secrets, scrub
        secrets = collect_secrets(vars(conn))
        if conn._token:
            secrets.append(conn._token)
        return False, scrub(str(message), secrets)[:1000]
    except (PublishError, PermissionError) as exc:
        # These local errors contain no entered credentials.
        return False, str(exc)
    except Exception:
        return False, 'Publishing failed. Check the repository connection and your plugin permissions.'


def build_publish_dialog(name, origin, session):
    choices = repositories(session)
    selected = origin if origin in choices else (choices[0] if choices else None)
    def field(label, identifier, secret=False):
        return html.Div([
            dbc.Label(label, html_for=identifier),
            dbc.Input(id=identifier, type='password' if secret else 'text',
                      autoComplete='off', value='', maxLength=4096),
        ], className='mb-3')
    return dbc.Modal([
        dbc.ModalHeader(dbc.ModalTitle(f"Publish {name}")),
        dbc.ModalBody([
            html.P('Publish uploads the saved source. Save edits before publishing.'),
            dcc.Store(id='plugin-publish-context', data={'name': name, 'nonce': str(uuid.uuid4())}),
            dbc.Label('API repository', html_for='plugin-publish-repository'),
            dbc.Select(id='plugin-publish-repository',
                       options=[{'label': keys, 'value': keys} for keys in choices], value=selected),
            html.P('Configure an API connector on this server to add a repository.',
                   hidden=bool(choices), className='text-muted mt-2'),
            html.Div(id='plugin-publish-alert', className='mt-3', role='status'),
            html.Fieldset([
                dbc.Label('Sign in with'),
                dbc.RadioItems(id='plugin-publish-method', value='password', inline=True,
                    options=[{'label': 'Username and password', 'value': 'password'},
                             {'label': 'Client ID and secret', 'value': 'client_credentials'},
                             {'label': 'API key', 'value': 'api_key'}]),
                html.Div([field('Username', 'plugin-publish-username'),
                          field('Password', 'plugin-publish-password', True)],
                         id='plugin-publish-password-fields', className='mt-3'),
                html.Div([field('Client ID', 'plugin-publish-client-id'),
                          field('Client secret', 'plugin-publish-client-secret', True)],
                         id='plugin-publish-client-fields', hidden=True, className='mt-3'),
                html.Div(field('API key', 'plugin-publish-api-key', True),
                         id='plugin-publish-key-fields', hidden=True, className='mt-3'),
                html.P('Credentials are used only for this browser session.', className='text-muted'),
                dbc.Button('Sign in', id='plugin-publish-login', n_clicks=0, color='primary'),
            ], id='plugin-publish-auth', hidden=True),
        ]),
        dbc.ModalFooter(html.Fieldset([
            dbc.Button('Close', id='plugin-publish-close', n_clicks=0, color='secondary'),
            dbc.Button('Publish', id='plugin-publish-submit', n_clicks=0, color='primary', disabled=True),
        ], id='plugin-publish-controls', className='d-flex gap-2')),
    ], id='plugin-publish-modal', is_open=False)
