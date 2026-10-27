#! /usr/bin/env python
# -*- coding: utf-8 -*-
# vim:fenc=utf-8

"""
Custom components of the web console (v4's, on the v5 façade).
"""

from __future__ import annotations

import sys
import contextlib
import contextvars

from meerschaum.utils.packages import attempt_import, import_dcc, import_html
from meerschaum.utils.typing import SuccessTuple, List, Optional, Any, Dict, Tuple
from meerschaum._internal.static import STATIC_CONFIG
from meerschaum.utils.misc import remove_ansi
from meerschaum.api.dash._support import get_shell_intro
from meerschaum.api import endpoints, CHECK_UPDATE, docs_enabled, get_api_connector
from meerschaum.connectors import instance_types, _load_builtin_custom_connectors
from meerschaum.utils.misc import get_connector_labels
from meerschaum.config import __doc__ as doc
dbc = attempt_import('dash_bootstrap_components', lazy=False, check_update=CHECK_UPDATE)
html, dcc = import_html(check_update=CHECK_UPDATE), import_dcc(check_update=CHECK_UPDATE)
dex = attempt_import('dash_extensions', lazy=False, check_update=CHECK_UPDATE)
dash_ace = attempt_import('dash_ace', lazy=False, check_update=CHECK_UPDATE)
_load_builtin_custom_connectors()

go_button = dbc.Button('Exec', id='go-button', color='primary', style={'width': '100%'})
test_button = dbc.Button('Test', id='test-button', color='danger', style={'display': 'none'})
show_pipes_button = dbc.Button(
    'Pipes',
    id='get-pipes-button',
    color='info',
    style={'width': '100%'},
)
show_jobs_button = dbc.Button(
    'Jobs',
    id='get-jobs-button',
    color='success',
    style={'width': '100%'},
)
cancel_button = dbc.Button(
    html.Span('>_', role='img', **{'aria-label': 'Terminal'}),
    id='cancel-button',
    color='dark',
    style={'width': '100%', 'background-color': 'black', 'display': 'none'},
)
show_webterm_button = dbc.Button(
    html.Span('>_', role='img', **{'aria-label': 'Terminal'}),
    id='show-webterm-button',
    color='dark',
    title='Terminal',
    style={'background-color': 'black'},
)
show_pipelines_button = dbc.Button('Pipelines', id='get-pipelines-button',
                                    color='warning', style={'width': '100%'})
### A grid sized by its card (`.console-actions` in `dash.css`): four columns, then two, then one,
### so a label never wraps and the terminal button stays beside Exec. (Plugins, Users and Runs,
### once under a "More" menu here, are pages of the top navigation.)
bottom_buttons_content = dbc.Card(
    dbc.CardBody(
        html.Div(
            [
                dbc.ButtonGroup(
                    [go_button, show_webterm_button],
                    className='console-actions-exec',
                ),
                show_pipes_button,
                show_jobs_button,
                show_pipelines_button,
            ],
            className='console-actions',
        ),
        className='console-actions-card',
    )
)
console_div = html.Div(
    id='console-div',
    children=[html.Pre(get_shell_intro(), id='console-pre')],
)

location = dcc.Location(id='mrsm-location', refresh=False)

search_parameters_editor = dash_ace.DashAceEditor(
    id='search-parameters-editor',
    theme='monokai',
    mode='json',
    tabSize=2,
    placeholder=(
        'Additional search parameters. ' +
        'Simple dictionary format or JSON accepted.'
    ),
    style={'height': 100},
)

download_dataframe = dcc.Download(id='download-dataframe-csv')
download_logs = dcc.Download(id='download-logs')


def build_icon(name: str, class_name: str = '') -> Any:
    """
    An icon drawn in the text's color (a CSS mask over an inline SVG, see `assets/nav.css`), so it
    renders with any font: `refresh`, `fullscreen`, `fullscreen-exit`, `user`, `plus`, `terminal`,
    `check`, `x`. It is hidden from screen readers: label its button with `title`.
    """
    return html.Span(
        className=f'mrsm-icon mrsm-icon-{name}' + (f' {class_name}' if class_name else ''),
        **{'aria-hidden': 'true'},
    )


### The console's top navigation (design §8.1): the logo (the Overview) and the console's
### pages on the left; the global instance selector and the user menu on the right. On a phone
### it collapses behind the toggler.
###
### NOTE: These navbar pieces are FACTORIES, not module-level singletons. Reusing
### the same component object across multiple page layouts is a Dash anti-pattern:
### React keeps the shared subtree mounted and mis-reconciles its siblings on
### navigation, so a previous page's components linger (and their callbacks keep
### firing). Building a fresh instance per page makes navigation fully remount.

#: The links of the top navigation, in order: `(section, label, path)`.
CONSOLE_NAV_LINKS: Tuple[Tuple[str, str, str], ...] = (
    ('pipes', 'Pipes', '/dash/pipes'),
    ('pipelines', 'Pipelines', '/dash/pipelines'),
    ('jobs', 'Jobs', '/dash/jobs'),
    ('connectors', 'Connectors', '/dash/connectors'),
    ('plugins', 'Plugins', '/dash/plugins'),
    ('terminal', 'Terminal', '/dash/terminal'),
)

#: The pages of the user menu (above Sign out): `(section, label, path)`.
CONSOLE_USER_LINKS: Tuple[Tuple[str, str, str], ...] = (
    ('users', 'Users', '/dash/users'),
    ('tokens', 'Tokens', '/dash/tokens'),
    ('settings', 'Settings', '/dash/password-reset'),
)

#: Console paths of a section that do not start with its path.
SECTION_ALIASES: Dict[str, str] = {
    '/dash': 'overview',
    '/dash/runs': 'jobs',
    '/dash/webterm': 'terminal',
}

#: The console's own pages: never listed under Pages.
CONSOLE_OWN_PATHS = frozenset(
    [href for _, _, href in CONSOLE_NAV_LINKS + CONSOLE_USER_LINKS]
    + list(SECTION_ALIASES)
    + ['/dash/login', '/dash/register']
)

_nav_context: contextvars.ContextVar = contextvars.ContextVar('mrsm_console_nav', default=None)


@contextlib.contextmanager
def console_nav_context(pathname: Optional[str] = None, session_id: Optional[str] = None):
    """
    Build the navbars inside this block for the page at `pathname`, seen by the user of
    `session_id`. The console's router (`callbacks/dashboard.py`) renders every page inside it;
    elsewhere (a callback) the navbar reads the request's session cookie and referrer instead.
    """
    token = _nav_context.set({'pathname': pathname, 'session_id': session_id})
    try:
        yield
    finally:
        _nav_context.reset(token)


def section_of_path(pathname: Optional[str]) -> Optional[str]:
    """
    The navigation section a console path belongs to (`overview`, `pipes`, `jobs`, `users`, ...),
    or `None` for another page (a plugin's).
    """
    path = '/' + str(pathname or '').strip().strip('/')
    if path in SECTION_ALIASES:
        return SECTION_ALIASES[path]
    for section, _, href in CONSOLE_NAV_LINKS + CONSOLE_USER_LINKS:
        if path == href or path.startswith(href + '/'):
            return section
    return None


def _current_request() -> Any:
    try:
        import flask
        return flask.request if flask.has_request_context() else None
    except Exception:
        return None


def _console_tables() -> Tuple[Optional[Dict[str, Any]], Dict[str, str]]:
    """
    The router's `_paths` and `_pages` (`callbacks/dashboard.py`), or `(None, {})` before it is
    imported: a page built at import time gets every link, and the router rebuilds its navbar for
    each request (`personalize_navbar`).
    """
    module = sys.modules.get('meerschaum.api.dash.callbacks.dashboard', None)
    if module is None:
        return None, {}
    return getattr(module, '_paths', None), (getattr(module, '_pages', None) or {})


def _is_served(path: str, paths: Optional[Dict[str, Any]]) -> bool:
    return paths is None or path in paths


def get_nav_state(pathname: Optional[str] = None, session_id: Optional[str] = None) -> Dict[str, Any]:
    """
    Who is looking at which page, for the navbar: `pathname`, `session_id`, `username`,
    `signed_in`, `may_act` (the user may run actions: every user, or only an administrator under
    `--secure`) and `terminal` (whether this user may use the web terminal: it is enabled and the
    user may run actions).
    """
    from meerschaum.api import no_auth
    context = _nav_context.get() or {}
    pathname = pathname or context.get('pathname', None)
    session_id = session_id or context.get('session_id', None)
    request = _current_request()
    if request is not None:
        if not session_id:
            from meerschaum.api.dash.sessions import SESSION_COOKIE_NAME
            session_id = request.cookies.get(SESSION_COOKIE_NAME, None) or None
        if not pathname and getattr(request, 'referrer', None):
            from urllib.parse import urlparse
            pathname = urlparse(request.referrer).path

    username, signed_in, may_act = None, bool(no_auth), bool(no_auth)
    if session_id and not no_auth:
        try:
            from meerschaum.api.dash.sessions import (
                get_username_from_session,
                is_session_authenticated,
            )
            username = get_username_from_session(str(session_id))
            signed_in = username is not None
            may_act = signed_in and is_session_authenticated(str(session_id))
        except Exception:
            username, signed_in, may_act = None, False, False

    terminal = False
    if may_act:
        try:
            from meerschaum.api.dash.webterm import webterm_enabled
            terminal = webterm_enabled()
        except Exception:
            terminal = False

    return {
        'pathname': pathname,
        'session_id': session_id,
        'username': username,
        'signed_in': signed_in,
        'may_act': may_act,
        'terminal': terminal,
    }


def _group_label(group: Optional[str]) -> str:
    if not group:
        return 'Pages'
    group = str(group)
    return group.capitalize() if not group[0].isupper() else group


def _page_label(label: str) -> str:
    """`Reports/daily` (a page key) as `Reports / Daily`."""
    parts = [part.strip() for part in str(label).split('/') if part.strip()]
    return ' / '.join((part[0].upper() + part[1:]) for part in parts) or str(label)


def get_plugin_nav_pages() -> Dict[str, List[Tuple[str, str]]]:
    """
    The pages plugins add to the console (`@web_page` pages, native plugin pages and layout
    pages), grouped by their page group: `{group: [(label, path), ...]}`. These are the
    navbar's Pages menu.
    """
    paths, pages = _console_tables()
    if paths is None:
        return {}
    from meerschaum.plugins import _plugin_endpoints_to_pages
    custom = sys.modules.get('meerschaum.api.dash.callbacks.custom', None)
    contract_pages = getattr(custom, 'contract_pages', None) or {}
    dash_prefix = endpoints['dash']

    def _console_path(endpoint: str) -> str:
        endpoint = str(endpoint)
        if endpoint.lstrip('/').startswith('dash'):
            return '/' + endpoint.lstrip('/')
        return dash_prefix + '/' + endpoint.lstrip('/')

    groups: Dict[str, List[Tuple[str, str]]] = {}
    seen = set()
    for group, pages_dicts in list(_plugin_endpoints_to_pages.items()):
        for endpoint, page in list(pages_dicts.items()):
            path = _console_path(endpoint).rstrip('/')
            if path in CONSOLE_OWN_PATHS or path in seen or path not in paths:
                continue
            seen.add(path)
            groups.setdefault(_group_label(group), []).append(
                (_page_label(page.get('page_key') or path), path)
            )

    for label, path in list(pages.items()):
        path = str(path).rstrip('/')
        if path in CONSOLE_OWN_PATHS or path in seen or path not in paths:
            continue
        seen.add(path)
        group = (contract_pages.get(path, None) or {}).get('page_group', None)
        groups.setdefault(_group_label(group), []).append((_page_label(label), path))
    return groups


def build_logo_link(paths: Optional[Dict[str, Any]] = None) -> Any:
    """
    The Meerschaum logo, a link to the Overview (`/dash`) when the console's pages are served.
    """
    if paths is None:
        paths, _ = _console_tables()
    logo = html.Img(
        src=endpoints['dash'] + "/assets/logo_48x48.png",
        title=doc,
        alt="Meerschaum",
        id="logo-img",
        className='console-nav-logo',
    )
    if not _is_served('/dash', paths):
        return html.Span(logo, className='navbar-brand console-nav-brand')
    return dcc.Link(logo, href='/dash', className='navbar-brand console-nav-brand', title='Overview')


def build_sign_out_button():
    """Sign out (the bar above a plugin's page)."""
    return dbc.Button(
        "Sign out",
        color='link',
        id='sign-out-button',
        class_name='console-nav-sign-out',
    )


def build_logo_row():
    """The logo in a row (kept for pages that build their own navbar)."""
    return dbc.Row(
        [dbc.Col(build_logo_link())],
        align='center',
        className='g-0 navbar-logo-row',
    )


def _instance_options(selected: Optional[str] = None) -> List[Dict[str, str]]:
    from meerschaum.api.dash.connector_listing import is_internal_connector
    labels = [
        keys for keys in get_connector_labels(*instance_types)
        if not is_internal_connector(*keys.split(':', 1))
    ]
    if selected and selected not in labels:
        labels = [selected] + labels
    return [
        {'label': (i[:32] + '…') if len(i) > 32 else i, 'value': i}
        for i in labels
    ]


def _global_instance_value() -> Optional[str]:
    """The console's global instance (the `mrsm-instance` cookie, if allowed), else the API's."""
    try:
        from meerschaum.api.dash.engine import get_instance
        return str(get_instance())
    except Exception:
        try:
            return str(get_api_connector())
        except Exception:
            return None


def build_pages_instance_select(
    select_id: str = 'pages-instance-select',
    value: Optional[str] = None,
) -> Any:
    """
    The console's global instance selector. Choosing an instance rewrites the `mrsm-instance`
    cookie (`assets/global_instance.js`, read by `meerschaum.api.dash.engine.global_instance_keys`),
    and the page reloads (the Pipelines pages return to their list).

    The Terminal page's selector is `instance-select`, which its own callbacks follow without a
    reload; it starts empty so they fill it with the global instance.
    """
    ### Outside a request (a page module's layout, built on import) the value is left for the
    ### router to fill per request (`personalize_navbar`), so importing builds no connector.
    if select_id != 'instance-select' and not value and _current_request() is not None:
        value = _global_instance_value()
    return html.Div(
        [
            html.Label('Instance', htmlFor=select_id, className='visually-hidden'),
            dbc.Select(
                id=select_id,
                size='sm',
                options=_instance_options(value),
                value=value,
                class_name='dbc_dark custom-select custom-select-sm instance-select',
            ),
        ],
        className='console-nav-instance',
        title='Instance',
    )


def _build_pages_menu(pathname: Optional[str]) -> Any:
    groups = get_plugin_nav_pages()
    if not groups:
        return None
    current = '/' + str(pathname or '').strip('/')
    items = []
    active = False
    for group, group_pages in groups.items():
        if len(groups) > 1:
            items.append(dbc.DropdownMenuItem(group, header=True))
        for label, path in group_pages:
            is_current = current == path
            active = active or is_current
            items.append(dbc.DropdownMenuItem(label, href=path, active=is_current))
    return dbc.DropdownMenu(
        items,
        label='Pages',
        nav=True,
        in_navbar=True,
        id='console-nav-pages',
        toggle_class_name='console-nav-link' + (' active' if active else ''),
        menu_variant='dark',
    )


def _build_user_menu(state: Dict[str, Any], paths: Optional[Dict[str, Any]], active: Optional[str]):
    from meerschaum.api import no_auth
    if not state['signed_in']:
        return dbc.NavItem(dbc.NavLink(
            'Sign in',
            href=endpoints['dash'] + '/login',
            id='console-nav-sign-in',
            class_name='console-nav-link',
        ))

    items = []
    for section, label, href in CONSOLE_USER_LINKS:
        if not _is_served(href, paths):
            continue
        items.append(dbc.DropdownMenuItem(
            label,
            href=href,
            active=(section == active),
            id=f'console-nav-{section}',
        ))
    if not no_auth:
        if items:
            items.append(dbc.DropdownMenuItem(divider=True))
        items.append(dbc.DropdownMenuItem('Sign out', id='sign-out-button', n_clicks=0))
    username = state['username'] or ('Guest' if no_auth else 'Account')
    return dbc.DropdownMenu(
        items,
        label=[
            build_icon('user'),
            html.Span(username, className='console-nav-username'),
        ],
        nav=True,
        in_navbar=True,
        align_end=True,
        id='console-nav-user',
        toggle_class_name='console-nav-link console-nav-user-toggle'
        + (' active' if active in ('users', 'tokens', 'settings') else ''),
        menu_variant='dark',
    )


def build_pages_navbar(
    instance_select: bool = True,
    instance_keys: Optional[str] = None,
    instance_select_id: str = 'pages-instance-select',
    active: Optional[str] = None,
    pathname: Optional[str] = None,
    session_id: Optional[str] = None,
) -> Any:
    """
    Return a fresh top navigation for a console page: the logo (the Overview), Pipes, Pipelines,
    Jobs, Connectors, Plugins, Terminal and the plugins' Pages, then the global instance selector
    and the user menu (Users, Tokens, Settings, Sign out). Links to pages the server does not
    serve (`api:permissions:routes`) are left out, Terminal is shown only to a user who may use
    the web terminal, and Connectors is left out for a user who may not run actions (a
    non-administrator under `--secure`).

    Parameters
    ----------
    instance_select: bool, default True
        Include the global instance selector.

    instance_keys: Optional[str], default None
        The selector's value (the console's global instance by default).

    instance_select_id: str, default 'pages-instance-select'
        The selector's id (`instance-select` on the Terminal page).

    active: Optional[str], default None
        The highlighted section (`pipes`, `jobs`, ...); by default the current page's.

    pathname: Optional[str], default None
        The current page (the router's, else the request's referrer).

    session_id: Optional[str], default None
        The viewer's session (the router's, else the request's session cookie).
    """
    state = get_nav_state(pathname=pathname, session_id=session_id)
    paths, _ = _console_tables()
    active = active or section_of_path(state['pathname'])

    links = []
    for section, label, href in CONSOLE_NAV_LINKS:
        if not _is_served(href, paths):
            continue
        if section == 'terminal' and not state['terminal']:
            continue
        ### Under `--secure` the connectors (hosts, users, databases) are for administrators.
        if section == 'connectors' and state['signed_in'] and not state['may_act']:
            continue
        links.append(dbc.NavItem(dbc.NavLink(
            label,
            href=href,
            active=(section == active),
            id=f'console-nav-{section}',
            class_name='console-nav-link',
        )))
    pages_menu = _build_pages_menu(state['pathname'])
    if pages_menu is not None:
        links.append(pages_menu)

    right = []
    if instance_select:
        right.append(build_pages_instance_select(
            select_id=instance_select_id,
            value=instance_keys,
        ))
    right.append(dbc.Nav(
        [_build_user_menu(state, paths, active)],
        navbar=True,
        class_name='console-nav-user-nav',
    ))

    return html.Div(
        dbc.Navbar(
            dbc.Container(
                [
                    build_logo_link(paths),
                    dbc.NavbarToggler(id="navbar-toggler", n_clicks=0),
                    dbc.Collapse(
                        [
                            dbc.Nav(links, navbar=True, class_name='console-nav-links'),
                            html.Div(right, className='console-nav-right'),
                        ],
                        id='navbar-collapse',
                        is_open=False,
                        navbar=True,
                    ),
                ],
                fluid=True,
                class_name='console-nav-container',
            ),
            dark=True,
            color='dark',
            expand='lg',
            class_name='console-nav',
        ),
        id='pages-navbar-div',
        className='console-nav-root',
        **{
            'data-mrsm-nav': 'console',
            'data-mrsm-nav-select': (instance_select_id if instance_select else ''),
        },
    )


def build_plugin_navbar() -> Any:
    """
    The bar above a plugin's `@web_page` page: the logo (a link to the Overview) and Sign out.
    A page registered with `@web_page(..., navbar=True)` gets the console's navigation
    (`build_pages_navbar`) instead, and `skip_navbar=True` gets no bar.
    """
    paths, _ = _console_tables()
    return html.Div(
        dbc.Navbar(
            dbc.Container(
                [
                    build_logo_link(paths),
                    dbc.NavbarToggler(id="navbar-toggler", n_clicks=0),
                    dbc.Collapse(
                        html.Div(build_sign_out_button(), className='console-nav-right'),
                        id='navbar-collapse',
                        is_open=False,
                        navbar=True,
                    ),
                ],
                fluid=True,
                class_name='console-nav-container',
            ),
            dark=True,
            color='dark',
            expand='lg',
            class_name='console-nav console-nav-plugin',
        ),
        id='pages-navbar-div',
        className='console-nav-root',
        **{'data-mrsm-nav': 'plugin'},
    )


def _walk_components(node: Any):
    if isinstance(node, (list, tuple)):
        for child in node:
            yield from _walk_components(child)
        return
    if node is None or isinstance(node, (str, int, float)):
        return
    yield node
    yield from _walk_components(getattr(node, 'children', None))


def personalize_navbar(layout: Any) -> Any:
    """
    Return a page layout (a list) whose first item is a console navbar built when the page's
    module was imported with that navbar rebuilt for this request (the viewer, the current page,
    the plugins' pages), keeping its instance selector. The layout itself is not modified.
    """
    if not isinstance(layout, (list, tuple)) or not layout:
        return layout
    first = layout[0]
    if getattr(first, 'data-mrsm-nav', None) != 'console':
        return layout
    select_id = getattr(first, 'data-mrsm-nav-select', '') or ''
    value = None
    if select_id:
        for component in _walk_components(first):
            if getattr(component, 'id', None) == select_id:
                value = getattr(component, 'value', None)
                break
    navbar_ = build_pages_navbar(
        instance_select=bool(select_id),
        instance_select_id=(select_id or 'pages-instance-select'),
        instance_keys=(value if select_id != 'instance-select' else None),
    )
    return [navbar_] + list(layout[1:])


def __getattr__(name: str) -> Any:
    """
    `navbar` and `instance_select` (v4's module-level navbar and the Terminal page's selector)
    are built on access, so each use is a fresh component.
    """
    if name == 'navbar':
        return build_pages_navbar(instance_select_id='instance-select')
    if name == 'instance_select':
        return build_pages_instance_select(select_id='instance-select')
    raise AttributeError(f"module {__name__!r} has no attribute {name!r}")


refresh_jobs_interval = dcc.Interval(
    id='refresh-jobs-interval',
    interval=(3 * 1000),
    n_intervals=0,
    disabled=False,
)

def alert_from_success_tuple(success: SuccessTuple) -> dbc.Alert:
    """
    Return a `dbc.Alert` from a `SuccessTuple`.
    """
    return dbc.Alert('', is_open=False) if not isinstance(success, tuple) else (
        dbc.Alert(
            remove_ansi(success[1]),
            id='success-alert',
            dismissable=True,
            fade=True,
            is_open=(success[1] not in STATIC_CONFIG['system']['success']['ignore']),
            color='success' if success[0] else 'danger',
        )
    )


def build_cards_grid(cards: List[dbc.Card], num_columns: int = 3) -> html.Div:
    """
    Because `CardColumns` were removed in Bootstrap 5, this function recreates a similar effect.
    """
    rows_childrens = []
    for i, card in enumerate(cards):
        if i % num_columns == 0:
            rows_childrens.append([])
        rows_childrens[-1].append(dbc.Col(card, sm=12, md=12, lg=int(12/num_columns)))
    ### Append mising columns to keep the grid shape.
    if rows_childrens and len(rows_childrens[-1]) != num_columns:
        for i in range(num_columns - len(rows_childrens[-1])):
            rows_childrens[-1].append(dbc.Col(sm=12, md=12, lg=int(12/num_columns)))
    _rows = [dbc.Row(children) for children in rows_childrens]
    rows = []
    for r in _rows:
        rows.append(r)
        rows.append(html.Br())
    return html.Div(rows)

