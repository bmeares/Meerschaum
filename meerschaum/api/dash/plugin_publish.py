"""Publish plugins with temporary browser credentials or saved API connector credentials."""
from __future__ import annotations

import copy
import threading
import uuid
from types import SimpleNamespace

from dash import dcc, html
import dash_bootstrap_components as dbc

from meerschaum.api.dash.plugin_sources import source_viewer

_AUTH_FIELDS = ('username', 'password', 'client_id', 'client_secret', 'api_key')
_LOCK = threading.Lock()


class PublishError(ValueError):
    """A local validation error whose message is safe to show in the dialog."""


#: Why a session may not publish an installed plugin.
PUBLISH_REFUSAL = 'Only administrators and team plugin managers can publish installed plugins.'


def may_publish(session, name=None):
    """
    Whether this session, read fresh, may publish the installed plugin `name` (those who may
    manage it: `plugins.may_manage_plugin`); without a name, whether it may publish at all.
    """
    from meerschaum.api.dash import plugins as pages
    try:
        who = source_viewer(session)
    except Exception:
        return False
    if who.no_auth:
        return True
    if not who.signed_in or not who.can('plugins:read'):
        return False
    if name is None:
        return pages.may_manage_plugins(who)
    return (pages.is_safe_plugin_name(name) and pages.can_see_plugin(who, name)
            and pages.may_manage_plugin(who, name))


def repositories(session, name=None):
    """
    Configured API repositories visible to who may publish (`may_publish`; the connectors their
    account may see); no credentials go to Dash.
    """
    if not may_publish(session, name):
        return []
    from meerschaum.api.dash.connector_listing import list_connectors
    return [row['keys'] for row in list_connectors(source_viewer(session)) if row['type'] == 'api']


def _key(context, repository, session):
    if not isinstance(context, dict) or not may_publish(session):
        raise PermissionError(PUBLISH_REFUSAL)
    from meerschaum.api.dash.plugins import _discover_plugin_files
    name = context.get('name')
    ### A plugin the session may not manage is not told apart from one that is not installed.
    if name not in _discover_plugin_files() or not may_publish(session, name):
        raise PublishError('This plugin is not installed.')
    if repository not in repositories(session, name):
        raise PublishError('Select a configured API repository.')
    from meerschaum.api.dash.sessions import valid_session_id
    session_id = valid_session_id((session or {}).get('session-id'))
    # A no-auth console still keeps different dialogs' remote credentials separate.
    nonce = valid_session_id(context.get('nonce'))
    if not session_id and not (source_viewer(session).no_auth and nonce):
        raise PermissionError('Sign in before publishing a plugin.')
    return session_id or nonce, repository


def browser_credentials(browser, key, url):
    """Accept only credentials entered for this login session and this repository address."""
    if not isinstance(browser, dict) or browser.get('session') != key[0]:
        return None
    entries = browser.get('repositories')
    entry = entries.get(key[1]) if isinstance(entries, dict) else None
    if not isinstance(entry, dict) or entry.get('url') != url:
        return None
    fields = entry.get('credentials')
    if not isinstance(fields, dict) or set(fields) not in (
        {'username', 'password'}, {'client_id', 'client_secret'}, {'api_key'},
    ):
        return None
    if not all(isinstance(value, str) and value.strip() and len(value) <= 4096
               for value in fields.values()):
        return None
    return fields


def _connection(context, repository, session, credentials=None, browser=None):
    import meerschaum as mrsm
    key = _key(context, repository, session)
    configured = mrsm.get_connector(repository)
    if configured is None:
        raise PublishError('The selected API repository is unavailable.')
    if credentials is None:
        credentials = browser_credentials(browser, key, configured.url)
    # Copying resets the native HTTP client. Validate entered credentials on a private copy.
    conn = copy.copy(configured)
    conn.connect_timeout = 10
    conn.timeout = 30
    if credentials is not None:
        for field in _AUTH_FIELDS:
            conn.__dict__.pop(field, None)
        conn.__dict__.pop('uri', None)
        conn.__dict__.update(credentials)
    elif configured._token:
        conn._token = configured._token
        conn._expires = configured._expires
    return key, conn, configured.url


def _browser_update(browser, key, url, fields):
    entries = (browser.get('repositories') or {}).copy() if (
        isinstance(browser, dict) and browser.get('session') == key[0]
        and isinstance(browser.get('repositories'), dict)
    ) else {}
    entries.pop(key[1], None)
    if fields is not None:
        while len(entries) >= 128:
            entries.pop(next(iter(entries)))
        entries[key[1]] = {'url': url, 'credentials': fields}
    return {'session': key[0], 'repositories': entries}


def _save_credentials(repository, conn, fields):
    """Change only the selected connector, preserving other files and environment settings."""
    import os
    import json
    from meerschaum.config import get_config, _config
    from meerschaum.config._read_config import read_config
    from meerschaum.config._edit import write_config
    from meerschaum.api.dash.connector_listing import _env_patches
    label = repository.split(':', 1)[1]
    overrides = [patch.get('api', {}).get(label, {}) for _, patch in _env_patches()]
    variable = os.environ.get('MRSM_API_' + label.upper())
    if variable:
        overrides.append(json.loads(variable) if variable.lstrip().startswith('{')
                         else {'uri': variable})
    for override in overrides:
        if not isinstance(override, dict):
            continue
        values = dict(override)
        if isinstance(values.get('uri'), str):
            values.update(conn.from_uri(values['uri'], as_dict=True))
        if any(values.get(field) and values[field] != fields.get(field) for field in _AUTH_FIELDS):
            raise PublishError('This connector has credentials in environment settings. '
                               'Update those settings or choose temporary browser storage.')
    with _LOCK:
        disk = read_config(keys=['meerschaum'], write_missing=False, substitute=False,
                           raise_parsing_errors=True)
        section = disk.setdefault('meerschaum', {})
        apis = section.setdefault('connectors', {}).setdefault('api', {})
        attributes = copy.deepcopy(apis.get(label) or {})
        # A URI may contain the previous credentials. Keep the same address without them.
        if 'uri' in attributes or not attributes.get('host'):
            attributes['uri'] = conn.url
        attributes.update({field: None for field in _AUTH_FIELDS})
        attributes.update(fields)
        apis[label] = attributes
        if not write_config({'meerschaum': section}):
            raise PublishError('Could not save credentials to this API connector.')
        # The live config is a shared dictionary; the native registry reads it on every call.
        live = get_config('meerschaum', 'connectors', 'api')
        updated = copy.deepcopy(live.get(label) or {})
        updated.update({field: None for field in _AUTH_FIELDS})
        updated.update(fields)
        if 'uri' in updated or not updated.get('host'):
            updated['uri'] = conn.url
        live[label] = updated
        links = _config().get('_symlinks', {}).get('meerschaum', {}).get('connectors', {}).get('api', {}).get(label, {})
        if isinstance(links, dict):
            for field in (*_AUTH_FIELDS, 'uri'):
                links.pop(field, None)
        import meerschaum as mrsm
        mrsm.get_connector(repository, refresh=True)


def _authenticated(conn):
    if conn._token:
        response = conn.get('/users/me', timeout=10)
        if response.ok:
            return True
    if any(conn.__dict__.get(field) for field in _AUTH_FIELDS):
        return bool(conn.login(warn=False)[0])
    return False


def repository_status(context, repository, session, browser=None):
    try:
        key, conn, url = _connection(context, repository, session, browser=browser)
        if _authenticated(conn):
            return True, f'Ready to publish to {repository}.'
        return False, f'Sign in to {repository} to publish.'
    except (PublishError, PermissionError) as exc:
        return False, str(exc)
    except Exception:
        return False, 'Could not connect to this repository. Check its connection settings and try again.'


def login_repository(context, repository, session, method, username, password,
                     client_id, client_secret, api_key, persist=False, browser=None):
    fields = {
        'password': {'username': username, 'password': password},
        'client_credentials': {'client_id': client_id, 'client_secret': client_secret},
        'api_key': {'api_key': api_key},
    }.get(method)
    if not fields or not all(isinstance(value, str) and value.strip() and len(value) <= 4096
                             for value in fields.values()):
        return False, 'Fill in the credentials for the selected sign-in method.', browser
    try:
        key, conn, url = _connection(context, repository, session, fields)
        if not conn.login(warn=False)[0]:
            return False, 'Sign-in failed. Check your credentials and try again.', browser
        if persist is True:
            try:
                _save_credentials(repository, conn, fields)
            except PublishError:
                raise
            except Exception:
                raise PublishError('Could not save credentials to this API connector. '
                                   'Check its configuration permissions or choose temporary browser storage.') from None
        stored = _browser_update(browser, key, url, None if persist is True else fields)
        destination = 'Saved to the API connector.' if persist is True else 'Stored for this browser session.'
        return True, f'Ready to publish to {repository}. {destination}', stored
    except (PublishError, PermissionError) as exc:
        return False, str(exc), browser
    except Exception:
        return False, 'Could not sign in to this repository. Check its connection settings and try again.', browser


def publish(context, repository, session, browser=None):
    """Package current source with the CLI's archiver, then upload using the API client."""
    try:
        key, conn, url = _connection(context, repository, session, browser=browser)
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
    choices = repositories(session, name)
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
            dcc.Store(id='plugin-publish-credentials', storage_type='session'),
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
                dbc.Switch(id='plugin-publish-persist', label='Save credentials to this API connector', value=False),
                html.P('Off: temporary browser storage, cleared when this browser session ends. '
                       'On: save on the server for future use of this connector.', className='text-muted mt-2'),
                dbc.Button('Sign in', id='plugin-publish-login', n_clicks=0, color='primary'),
            ], id='plugin-publish-auth', hidden=True),
        ]),
        dbc.ModalFooter(html.Fieldset([
            dbc.Button('Close', id='plugin-publish-close', n_clicks=0, color='secondary'),
            dbc.Button('Publish', id='plugin-publish-submit', n_clicks=0, color='primary', disabled=True),
        ], id='plugin-publish-controls', className='d-flex gap-2')),
    ], id='plugin-publish-modal', is_open=False)
