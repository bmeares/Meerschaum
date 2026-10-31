#! /usr/bin/env python3
# -*- coding: utf-8 -*-
# vim:fenc=utf-8

"""
Callbacks for the plugins page: the Installed and Repository tabs, a published plugin's page,
and the administrators' Update, Uninstall and Install actions (run as jobs and watched until
they finish).
"""

from __future__ import annotations
import json
from urllib.parse import parse_qs
from meerschaum.utils.typing import Optional, Dict, Any
from meerschaum.api.dash import dash_app, debug
from meerschaum.api import get_api_connector, CHECK_UPDATE
from meerschaum.core import Plugin
from meerschaum.utils.packages import attempt_import, import_dcc, import_html
dash = attempt_import('dash', lazy=False, check_update=CHECK_UPDATE)
from dash.exceptions import PreventUpdate
from dash.dependencies import Input, Output, State, ALL, MATCH
from dash import no_update
html, dcc = import_html(check_update=CHECK_UPDATE), import_dcc(check_update=CHECK_UPDATE)
import dash_bootstrap_components as dbc
from meerschaum.api.dash.components import alert_from_success_tuple
from meerschaum.api.dash.users import viewer
from meerschaum.api.dash.plugins import (
    build_plugins_listing,
    build_plugin_detail,
    build_installed_plugin_detail,
    build_installed_plugins_tab,
    get_installed_plugins,
    is_plugin_owner,
    plugin_action_sysargs,
    start_plugin_run,
)

PLUGINS_TABS = ('installed', 'repository')
HIDDEN = {'display': 'none'}


def _parse_plugin_name_from_pathname(pathname: Optional[str]) -> Optional[str]:
    """
    Return the plugin name from ``/dash/plugins/<name>`` or ``None`` for the listing.
    """
    if not pathname or not str(pathname).startswith('/dash/plugins'):
        return None
    suffix = str(pathname)[len('/dash/plugins'):].strip('/')
    if suffix.startswith('installed/'):
        suffix = suffix[len('installed/'):]
    return suffix or None


@dash_app.callback(
    Output('plugins-tabs', 'active_tab'),
    Input('plugins-location', 'search'),
    Input('plugins-browse-repository-button', 'n_clicks', allow_optional=True),
)
def choose_plugins_tab(search: Optional[str], browse_clicks: Optional[int]):
    """`?tab=repository` (or the Installed tab's "Browse the repository") opens the Repository tab."""
    if dash.callback_context.triggered_id == 'plugins-browse-repository-button':
        if not browse_clicks:
            raise PreventUpdate
        return 'repository'
    tab = parse_qs(str(search or '').lstrip('?')).get('tab', ['installed'])[0]
    return tab if tab in PLUGINS_TABS else 'installed'


@dash_app.callback(
    Output('plugins-content-div', 'children'),
    Output('plugins-search-wrapper', 'style'),
    Output('plugins-pagination', 'max_value'),
    Output('plugins-pagination', 'active_page'),
    Output('plugins-pagination', 'style'),
    Output('plugins-installed-div', 'children'),
    Output('plugins-installed-div', 'hidden'),
    Output('plugins-repository-div', 'hidden'),
    Output('plugins-tabs-div', 'hidden'),
    Output('plugins-header', 'style'),
    Input('plugins-location', 'pathname'),
    Input('plugins-tabs', 'active_tab'),
    Input('search-plugins-input', 'value'),
    Input('plugins-pagination', 'active_page'),
    Input('plugins-refresh', 'data'),
    State('session-store', 'data'),
)
def render_plugins_page(
    pathname: Optional[str] = None,
    active_tab: Optional[str] = None,
    search_term: Optional[str] = None,
    active_page: Optional[int] = None,
    refresh: Any = None,
    session_data: Optional[Dict[str, Any]] = None,
):
    """
    Render the active tab (Installed or Repository) or a single published plugin's page.
    """
    who = viewer(session_data)
    if not who['active'] or not who['viewer'].can('plugins:read'):
        message = ('Plugins are not available to your account.' if who['active']
                   else 'Sign in to see the plugins')
        return (
            html.Div(
                [html.H5(message)] + ([] if who['active'] else [
                    dbc.Button('Sign in', href='/dash/login', color='primary')]),
                className='console-empty',
            ),
            HIDDEN, 1, 1, HIDDEN, [], True, False, True, HIDDEN,
        )
    admin = who['admin']
    plugin_name = _parse_plugin_name_from_pathname(pathname)
    if plugin_name:
        return (
            (build_installed_plugin_detail(plugin_name, session_data)
             if str(pathname).startswith('/dash/plugins/installed/')
             else build_plugin_detail(plugin_name, session_data, admin=admin)),
            HIDDEN, 1, 1, HIDDEN, [], True, False, True, HIDDEN,
        )

    if active_tab != 'repository':
        return (
            no_update, no_update, no_update, no_update, no_update,
            build_installed_plugins_tab(admin=admin),
            False, True, False, {},
        )

    triggered_id = getattr(dash.callback_context, 'triggered_id', None)
    if triggered_id == 'search-plugins-input':
        page = 1
    else:
        page = int(active_page or 1)

    content, total_pages, total = build_plugins_listing(
        search_term=search_term,
        session_data=session_data,
        page=page,
        admin=admin,
    )
    show_search = bool(search_term) or total > 0
    return (
        content,
        {} if show_search else HIDDEN,
        total_pages,
        page,
        {
            'justify-content': 'center',
            'display': 'flex' if total_pages > 1 else 'none',
        },
        no_update,
        True,
        False,
        False,
        {},
    )


@dash_app.callback(
    Output('plugin-source-content', 'children'),
    Input('plugin-source-file', 'value'),
    Input('plugin-source-context', 'data'),
    State('session-store', 'data'),
)
def show_plugin_source(selected, context, session):
    """Every file selection repeats authentication, membership, and bounded path checks."""
    if not selected or not isinstance(context, dict):
        raise PreventUpdate
    from meerschaum.api.dash.plugin_sources import read_source, source_preview
    try:
        return source_preview(selected,read_source(context.get('kind'), context.get('name'), selected, session))
    except PermissionError:
        return 'Plugin source is not available to your account.'
    except Exception:
        return 'This file cannot be previewed. Choose another source file.'


dash_app.clientside_callback(
    """
    function(clicks, ids) {
        const ctx = dash_clientside.callback_context;
        const target = ctx.triggered_id;
        const keep = dash_clientside.no_update;
        if (!target || typeof target.path !== 'string') return [keep, keep];
        const position = (ids || []).findIndex(item => item.path === target.path);
        if (position < 0 || !(clicks || [])[position]) return [keep, keep];
        return [target.path, ids.map(item => 'plugin-source-file' +
            (item.path === target.path ? ' active' : ''))];
    }
    """,
    Output('plugin-source-file','value'),
    Output({'type':'plugin-source-select','path':ALL},'className'),
    Input({'type':'plugin-source-select','path':ALL},'n_clicks'),
    State({'type':'plugin-source-select','path':ALL},'id'),
    prevent_initial_call=True,
)


def _run_alert(message: str, job_name: Optional[str], success: bool = True, running: bool = False):
    """An alert naming what runs (or ran), with a button to its output."""
    children = [html.Span(message)]
    if job_name:
        children += [
            ' ',
            dbc.Button(
                'View output',
                id={'type': 'job-view-logs-button', 'index': job_name},
                color='link',
                size='sm',
                class_name='p-0 align-baseline',
            ),
        ]
    return dbc.Alert(
        children,
        color=('info' if running else ('success' if success else 'danger')),
        dismissable=True,
        id='plugins-run-alert',
    )


ACTION_WORDS = {
    'install': ('Installing', 'Installed', 'install'),
    'reinstall': ('Reinstalling', 'Reinstalled', 'reinstall'),
    'update': ('Updating', 'Updated', 'update'),
    'uninstall': ('Uninstalling', 'Uninstalled', 'uninstall'),
}


def run_plugin_action(action: str, name: str, session_data: Optional[Dict[str, Any]]):
    """
    Start a plugin action for an administrator. Returns `(alert, run, interval_disabled)`.
    """
    who = viewer(session_data)
    if not who['admin']:
        return alert_from_success_tuple((False, "Only administrators can manage plugins.")), no_update, no_update
    origin = None
    if action == 'update':
        installed = {p['name']: p for p in get_installed_plugins(check_packages=False)}
        if name not in installed:
            return alert_from_success_tuple((False, f"Plugin '{name}' is not installed.")), no_update, no_update
        origin = installed[name].get('origin')
    sysargs, env, message = plugin_action_sysargs(action, name, origin=origin, username=who['username'])
    if sysargs is None:
        return alert_from_success_tuple((False, message)), no_update, no_update
    (success, message), job_name = start_plugin_run(sysargs, who['username'], env=env)
    if not success:
        return _run_alert(message, job_name, success=False), no_update, no_update
    doing = ACTION_WORDS[action][0]
    return (
        _run_alert(f"{doing} plugin '{name}'…", job_name, running=True),
        {'job': job_name, 'action': action, 'plugin': name},
        False,
    )


@dash_app.callback(
    Output('plugins-uninstall-modal', 'is_open'),
    Output('plugins-uninstall-title', 'children'),
    Output('plugins-uninstall-target', 'data'),
    Output('plugins-alert-div', 'children'),
    Output('plugins-run', 'data'),
    Output('plugins-run-interval', 'disabled'),
    Input({'type': 'plugin-action', 'action': ALL, 'index': ALL}, 'n_clicks'),
    Input('plugins-uninstall-cancel', 'n_clicks'),
    Input('plugins-uninstall-confirm', 'n_clicks'),
    State('plugins-uninstall-target', 'data'),
    State('plugins-run', 'data'),
    State('session-store', 'data'),
    prevent_initial_call=True,
)
def plugin_action_click(action_clicks, cancel_clicks, confirm_clicks, target, run, session_data):
    """Update or install right away; Uninstall asks first."""
    ctx = dash.callback_context
    if not ctx.triggered or not ctx.triggered[0]['value']:
        raise PreventUpdate
    trigger = ctx.triggered_id
    if trigger == 'plugins-uninstall-cancel':
        return False, no_update, None, no_update, no_update, no_update
    if run and run.get('job'):
        return (
            no_update, no_update, no_update,
            alert_from_success_tuple((False, "Wait for the running plugin action to finish.")),
            no_update, no_update,
        )
    if trigger == 'plugins-uninstall-confirm':
        if not target:
            raise PreventUpdate
        alert, new_run, disabled = run_plugin_action('uninstall', target, session_data)
        return False, no_update, None, alert, new_run, disabled
    if not isinstance(trigger, dict) or trigger.get('type') != 'plugin-action':
        raise PreventUpdate
    action, name = trigger.get('action'), trigger.get('index')
    if action == 'uninstall':
        if not viewer(session_data)['admin']:
            raise PreventUpdate
        return True, ['Uninstall plugin ', html.B(name), '?'], name, no_update, no_update, no_update
    if action not in ('install', 'reinstall', 'update'):
        raise PreventUpdate
    alert, new_run, disabled = run_plugin_action(action, name, session_data)
    return no_update, no_update, no_update, alert, new_run, disabled


@dash_app.callback(
    Output('plugins-alert-div', 'children', allow_duplicate=True),
    Output('plugins-run', 'data', allow_duplicate=True),
    Output('plugins-run-interval', 'disabled', allow_duplicate=True),
    Output('plugins-refresh', 'data'),
    Input('plugins-run-interval', 'n_intervals'),
    State('plugins-run', 'data'),
    State('plugins-refresh', 'data'),
    State('session-store', 'data'),
    prevent_initial_call=True,
)
def watch_plugin_run(n_intervals, run, refresh, session_data):
    """When the plugin action's job finishes, say how it went and refresh the lists."""
    if not run or not run.get('job'):
        return no_update, no_update, True, no_update
    if not viewer(session_data)['admin']:
        return no_update, None, True, no_update
    from meerschaum.api.dash._support import get_job
    from meerschaum.api.dash.jobs import job_visible_to_session
    try:
        job = get_job(run['job'])
        exists = job.exists() and job_visible_to_session(job, (session_data or {}).get('session-id'))
        status = job.status if exists else 'stopped'
        result = job.result if exists else (False, 'The job went away.')
    except Exception as e:
        status, result = 'stopped', (False, str(e))
    if status in ('running', 'paused'):
        raise PreventUpdate
    action = run.get('action', 'install')
    done, verb = ACTION_WORDS.get(action, ACTION_WORDS['install'])[1:]
    success = bool(result and result[0])
    message = (
        f"{done} plugin '{run.get('plugin')}'."
        if success
        else f"Failed to {verb} plugin '{run.get('plugin')}'."
    )
    return _run_alert(message, run['job'], success=success), None, True, (refresh or 0) + 1


@dash_app.callback(
    Output({'type': 'edit-alert-div', 'index': MATCH}, 'children'),
    Input({'type': 'edit-button', 'index': MATCH}, 'n_clicks'),
    State({'type': 'description-textarea', 'index': MATCH}, 'value'),
    State('session-store', 'data'),
)
def edit_plugin_description(
    n_clicks: Optional[int] = None,
    description: Optional[str] = None,
    session_data: Optional[Dict[str, Any]] = None,
):
    """
    Edit a plugin's description and set the alert.
    """
    if n_clicks is None:
        raise PreventUpdate
    ctx = dash.callback_context
    prop_id = ctx.triggered[0]['prop_id']
    j = '.'.join(prop_id.split('.')[:-1])
    plugin_name = json.loads(j)['index']
    if not is_plugin_owner(plugin_name, session_data):
        success, msg = (
            False,
            f"Failed to update description for plugin '{plugin_name}'"
            + " due to insufficient permissions."
        )
    else:
        plugin = Plugin(plugin_name)
        plugin.user_id = get_api_connector().get_plugin_user_id(plugin, debug=debug)
        plugin.attributes = get_api_connector().get_plugin_attributes(plugin, debug=debug)
        plugin.attributes.update({'description': description})
        success, _msg = get_api_connector().register_plugin(plugin, debug=debug, force=True)
        msg = _msg if not success else "Successfully updated description."
    return [alert_from_success_tuple((success, msg))]
