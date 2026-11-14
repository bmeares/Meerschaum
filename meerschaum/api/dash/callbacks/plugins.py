#! /usr/bin/env python3
# -*- coding: utf-8 -*-
# vim:fenc=utf-8

"""
Callbacks for the plugins page: the Installed and Repository tabs, a published plugin's page,
the Update, Uninstall and Install actions of administrators and of a team's plugin managers
(run as jobs and watched until they finish), New plugin (from a template or uploaded files), and
editing an installed plugin's files. Every callback reads the session's viewer again and asks
the policy (`plugins.plugin_action_refusal`, `plugins.may_manage_plugin`,
`plugin_sources.authoring_refusal`): nothing the browser sends decides who may do what.
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
from dash.dependencies import Input, Output, State, ALL, MATCH, ClientsideFunction
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
    may_manage_plugin,
    plugin_action_refusal,
)
from meerschaum.api.dash.plugin_sources import may_author

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
    Output('plugins-header-actions', 'children'),
    Input('plugins-location', 'pathname'),
    Input('plugins-tabs', 'active_tab'),
    Input('search-plugins-input', 'value'),
    Input('plugins-pagination', 'active_page'),
    Input('plugins-refresh', 'data'),
    State('session-store', 'data'),
    State('plugins-location', 'search'),
)
def render_plugins_page(
    pathname: Optional[str] = None,
    active_tab: Optional[str] = None,
    search_term: Optional[str] = None,
    active_page: Optional[int] = None,
    refresh: Any = None,
    session_data: Optional[Dict[str, Any]] = None,
    search: Optional[str] = None,
):
    """
    Render the active tab (Installed or Repository) or a single plugin's page, with the
    plugins the viewer may see. New plugin (the header's circled plus) is shown to those who may
    author plugins (`plugin_sources.may_author`).
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
            HIDDEN, 1, 1, HIDDEN, [], True, False, True, HIDDEN, [],
        )
    admin, person = who['admin'], who['viewer']
    plugin_name = _parse_plugin_name_from_pathname(pathname)
    if plugin_name:
        if str(pathname).startswith('/dash/plugins/installed/'):
            query = parse_qs(str(search or '').lstrip('?'))
            content = build_installed_plugin_detail(
                plugin_name,
                session_data,
                selected=(query.get('file') or [None])[0],
                edit=(query.get('edit') or [''])[0] == '1',
                created=(query.get('created') or [''])[0] == '1',
            )
        else:
            content = build_plugin_detail(plugin_name, session_data, admin=admin, viewer=person)
        return (content, HIDDEN, 1, 1, HIDDEN, [], True, False, True, HIDDEN, [])

    header_actions = _new_plugin_button() if may_author(session_data) else []
    if active_tab != 'repository':
        return (
            no_update, no_update, no_update, no_update, no_update,
            build_installed_plugins_tab(admin=admin, viewer=person),
            False, True, False, {}, header_actions,
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
        viewer=person,
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
        header_actions,
    )


def _new_plugin_button():
    """The header's New plugin: a circled plus."""
    return html.Button(
        [html.I(className='fa-solid fa-circle-plus', **{'aria-hidden': 'true'}),
         html.Span('New plugin', className='visually-hidden')],
        id='plugins-new-button', type='button', n_clicks=0,
        className='btn plugin-new-button', title='New plugin',
        **{'aria-label': 'New plugin'},
    )


@dash_app.callback(
    Output('plugin-source-content', 'children'),
    Input('plugin-source-file', 'value'),
    Input('plugin-source-context', 'data'),
    State('session-store', 'data'),
)
def show_plugin_source(selected, context, session):
    """
    Every file selection repeats authentication, membership, and bounded path checks. An
    installed plugin's file has Edit for authors; the context's `edit` naming the file opens it
    in the editor (its text as read now, and the digest a save is checked against).
    """
    if not selected or not isinstance(context, dict):
        raise PreventUpdate
    from meerschaum.api.dash.plugin_sources import read_source_with_digest, source_preview
    kind = context.get('kind')
    try:
        source, digest = read_source_with_digest(kind, context.get('name'), selected, session)
        editable = kind == 'installed' and may_author(session, context.get('name'))
        editing = context.get('edit')
        in_editor = bool(editable and isinstance(editing, dict) and editing.get('path') == selected)
        return source_preview(selected, source, digest=digest, editable=editable, editing=in_editor)
    except PermissionError:
        return 'Plugin source is not available to your account.'
    except Exception:
        return 'This file cannot be previewed. Choose another source file.'


@dash_app.callback(
    Output('plugin-source-context', 'data'),
    Output('plugin-source-alert', 'children'),
    Input({'type': 'plugin-source-action', 'action': ALL}, 'n_clicks'),
    State('plugin-source-file', 'value'),
    State('plugin-source-context', 'data'),
    State('plugin-source-data', 'data', allow_optional=True),
    State('plugin-source-draft', 'data', allow_optional=True),
    State('session-store', 'data'),
    prevent_initial_call=True,
)
def plugin_source_action(clicks, selected, context, data, text, session):
    """
    Edit opens the file in the editor (the context's `edit`, which `show_plugin_source` reads),
    Cancel closes it, Save writes it (`plugin_sources.save_source`: inside the plugin's own
    directory, atomically, refused if the file changed on disk since it was opened) and closes
    it. Permission is checked again for each.
    """
    ctx = dash.callback_context
    trigger = ctx.triggered_id
    if not isinstance(trigger, dict) or not ctx.triggered or not ctx.triggered[0].get('value'):
        raise PreventUpdate
    if not isinstance(context, dict) or context.get('kind') != 'installed' or not selected:
        raise PreventUpdate
    from meerschaum.api.dash.plugin_sources import authoring_refusal, save_source
    action = trigger.get('action')
    closed = {**context, 'edit': None}
    refusal = authoring_refusal(session, context.get('name'))
    if refusal:
        return (closed if context.get('edit') else no_update), _source_alert(False, refusal)
    if action == 'edit':
        import uuid
        return {**context, 'edit': {'path': selected, 'nonce': uuid.uuid4().hex}}, []
    if action == 'cancel':
        return closed, []
    if action != 'save':
        raise PreventUpdate
    if not isinstance(data, dict) or data.get('path') != selected or not data.get('editing'):
        return no_update, _source_alert(False, 'Open the file for editing again before saving.')
    success, message, _ = save_source(context.get('name'), selected, text, data.get('digest'), session)
    if not success:
        return no_update, _source_alert(False, message)
    return closed, _source_alert(True, message)


def _source_alert(success: bool, message: str):
    return dbc.Alert(
        message, color=('success' if success else 'danger'), dismissable=True,
        id='plugin-source-save-alert', class_name='plugin-source-save-alert',
    )


### The file's text, highlighting and line numbers, drawn as text (`assets/plugin_source.js`).
dash_app.clientside_callback(
    ClientsideFunction(namespace='mrsmPluginSource', function_name='render'),
    Output('plugin-source-rendered', 'children'),
    Input('plugin-source-data', 'data'),
)


dash_app.clientside_callback(
    """
    function(clicks, ids) {
        const ctx = dash_clientside.callback_context;
        const target = ctx.triggered_id;
        const keep = dash_clientside.no_update;
        if (!target || typeof target.path !== 'string') return [keep, keep];
        // The file being edited stays open until it is saved or cancelled.
        if (document.querySelector('.plugin-source-browser.plugin-source-editing')) return [keep, keep];
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


@dash_app.callback(
    Output({'type': 'plugin-update-slot', 'index': ALL}, 'hidden'),
    Output({'type': 'plugin-update-slot', 'index': ALL}, 'className'),
    Output({'type': 'plugin-action', 'action': 'update', 'index': ALL}, 'title'),
    Input('plugins-update-check', 'data'),
    State({'type': 'plugin-update-slot', 'index': ALL}, 'id'),
    State({'type': 'plugin-action', 'action': 'update', 'index': ALL}, 'id'),
    State('session-store', 'data'),
)
def show_plugin_updates(names, slots, buttons, session_data):
    """
    After the installed plugins show, reveal Update only for the plugins whose repository has a
    newer version, among the plugins the viewer may manage (`plugins.plugin_update_versions`).
    """
    if not slots:
        raise PreventUpdate
    from meerschaum.api.dash.plugins import plugin_update_versions
    who = viewer(session_data)
    updates = plugin_update_versions(names or [], viewer=who['viewer']) if who['active'] else {}
    hidden = [slot.get('index') not in updates for slot in slots]
    titles = [
        (f"Update {button.get('index')} to {updates[button.get('index')]}"
         if button.get('index') in updates else f"Update {button.get('index')}")
        for button in buttons
    ]
    ### `plugin-update-checked`: the check has answered (what browser tests wait for).
    checked = ['plugin-update-slot plugin-update-checked' for _ in slots]
    return hidden, checked, titles


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
    Start a plugin action for who may run it (`plugins.plugin_action_refusal`: administrators,
    and a team's plugin managers for their team's plugins). Returns `(alert, run,
    interval_disabled)`.
    """
    who = viewer(session_data)
    if action not in ACTION_WORDS:
        return alert_from_success_tuple((False, f"Unknown plugin action '{action}'.")), no_update, no_update
    refusal = (
        plugin_action_refusal(who['viewer'], action, name) if who['active']
        else "Sign in to manage plugins."
    )
    if refusal:
        return alert_from_success_tuple((False, refusal)), no_update, no_update
    ### Its versions change: the next update check asks the repository again.
    from meerschaum.api.dash.plugins import forget_remote_versions
    forget_remote_versions(name)
    origin = None
    if action == 'update':
        installed = {p['name']: p for p in get_installed_plugins(check_packages=False)}
        if name not in installed:
            return alert_from_success_tuple((False, f"Plugin '{name}' is not installed.")), no_update, no_update
        origin = installed[name].get('origin')
    sysargs, env, message = plugin_action_sysargs(action, name, origin=origin, username=who['username'])
    if sysargs is None:
        return alert_from_success_tuple((False, message)), no_update, no_update
    ### The job itself records what it is for: `watch_plugin_run` reads it back from the job.
    (success, message), job_name = start_plugin_run(
        sysargs, who['username'], env=env, execution={'plugin': name, 'action': action},
    )
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
        who = viewer(session_data)
        if not who['active'] or not may_manage_plugin(who['viewer'], name):
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
    """
    When the plugin action's job finishes, say how it went and refresh the lists. A
    non-administrator's install is then recorded as their team's plugin, and their uninstall
    taken off its list (`plugins.claim_plugin`, `plugins.release_plugin`). The browser only
    names the job: whose it is, and the plugin and action it ran, are read from the job
    (`plugins.plugin_run_record`), and the viewer's rights are read again.
    """
    if not run or not isinstance(run, dict) or not run.get('job'):
        return no_update, no_update, True, no_update
    from meerschaum.api.dash.plugins import (
        may_manage_plugins, plugin_run_record, claim_plugin, release_plugin,
    )
    who = viewer(session_data)
    if not who['active'] or not may_manage_plugins(who['viewer']):
        return no_update, None, True, no_update
    from meerschaum.api.dash._support import get_job
    from meerschaum.api.dash.jobs import job_visible_to_session
    record = None
    try:
        job = get_job(str(run['job']))
        exists = job.exists() and job_visible_to_session(job, (session_data or {}).get('session-id'))
        ### Only a plugin run this console started for this user is watched.
        record = plugin_run_record(job, who['username']) if exists else None
        exists = exists and record is not None
        status = job.status if exists else 'stopped'
        result = job.result if exists else (False, 'The job went away.')
    except Exception as e:
        status, result = 'stopped', (False, str(e))
    if status in ('running', 'paused'):
        raise PreventUpdate
    record = record or {}
    action = record.get('action') or 'install'
    plugin = record.get('plugin') or run.get('plugin')
    done, verb = ACTION_WORDS.get(action, ACTION_WORDS['install'])[1:]
    success = bool(result and result[0])
    message = (
        f"{done} plugin '{plugin}'."
        if success
        else f"Failed to {verb} plugin '{plugin}'."
    )
    if success and record and not who['admin']:
        recorded, note = (
            release_plugin(who['viewer'], plugin) if action == 'uninstall'
            else claim_plugin(who['viewer'], plugin)
        )
        if not recorded:
            message = f"{message} {note}"
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


########################################################################################
# New plugin (`bootstrap plugins`).
########################################################################################

def _feature_options(choices):
    return [
        {'label': html.Span(label, title=(example or None)), 'value': key}
        for key, label, example in choices.get('features', [])
    ]


def _language_options(choices):
    from meerschaum.api.dash.plugin_sources import language_logo
    return [
        {'label': html.Span([language_logo(language), html.Span(language, **{'aria-hidden': 'true'})],
                            className='plugin-new-language'),
         'value': language}
        for language in choices.get('languages', [])
    ]


@dash_app.callback(
    Output('plugins-new-modal', 'is_open'),
    Output('plugins-new-language', 'options'),
    Output('plugins-new-language', 'value'),
    Output('plugins-new-features', 'options'),
    Output('plugins-new-features', 'value'),
    Output('plugins-new-directory', 'options'),
    Output('plugins-new-directory', 'value'),
    Output('plugins-new-directory-field', 'hidden'),
    Output('plugins-new-name', 'value'),
    Output('plugins-new-action', 'value'),
    Output('plugins-new-dt', 'value'),
    Output('plugins-new-packaging', 'value'),
    Output('plugins-new-alert', 'children'),
    Input('plugins-new-button', 'n_clicks', allow_optional=True),
    Input('plugins-new-cancel', 'n_clicks'),
    State('session-store', 'data'),
    prevent_initial_call=True,
)
def open_new_plugin(new_clicks, cancel_clicks, session_data):
    """New plugin opens a blank form (`bootstrap plugins`'s choices) for authors; Cancel closes it."""
    ctx = dash.callback_context
    if not ctx.triggered or not ctx.triggered[0].get('value'):
        raise PreventUpdate
    keep = [no_update] * 12
    if ctx.triggered_id == 'plugins-new-cancel':
        return [False] + keep
    from meerschaum.api.dash.plugin_sources import authoring_refusal
    refusal = authoring_refusal(session_data)
    if refusal:
        return [False] + keep[:-1] + [no_update]
    from meerschaum.api.dash.plugins import new_plugin_choices
    try:
        choices = new_plugin_choices()
    except Exception as e:
        return [True] + keep[:-1] + [_new_plugin_alert(f"New plugin is unavailable: {e}")]
    directories = choices.get('directories', [])
    return (
        True,
        _language_options(choices),
        (choices.get('languages') or ['Python'])[0],
        _feature_options(choices),
        list(choices.get('default_features', [])),
        [{'label': directory, 'value': directory} for directory in directories],
        (directories[0] if directories else None),
        len(directories) <= 1,
        '', '', '', 'file',
        [],
    )


@dash_app.callback(
    Output('plugins-new-packaging', 'options'),
    Output('plugins-new-action-field', 'hidden'),
    Output('plugins-new-dt-field', 'hidden'),
    Input('plugins-new-name', 'value'),
    Input('plugins-new-language', 'value'),
    Input('plugins-new-features', 'value'),
    prevent_initial_call=True,
)
def new_plugin_form(name, language, features):
    """
    The packaging choices worded for the name and language; Action and Datetime column as
    needed. Nothing here is privileged (Create checks the session).
    """
    from meerschaum.api.dash.plugins import new_plugin_choices
    try:
        packaging = new_plugin_choices(name)['packaging'].get(language or 'Python')
    except Exception:
        packaging = None
    features = features or []
    options = (
        [{'label': packaging[0], 'value': 'file'}, {'label': packaging[1], 'value': 'package'}]
        if packaging else no_update
    )
    return options, 'action' not in features, 'fetch' not in features


@dash_app.callback(
    Output('plugins-new-alert', 'children', allow_duplicate=True),
    Output('plugins-new-modal', 'is_open', allow_duplicate=True),
    Output('plugins-navigate', 'data'),
    Input('plugins-new-create', 'n_clicks'),
    State('plugins-new-name', 'value'),
    State('plugins-new-language', 'value'),
    State('plugins-new-packaging', 'value'),
    State('plugins-new-features', 'value'),
    State('plugins-new-action', 'value'),
    State('plugins-new-dt', 'value'),
    State('plugins-new-directory', 'value'),
    State('session-store', 'data'),
    prevent_initial_call=True,
)
def create_plugin(clicks, name, language, packaging, features, action, dt_col, directory, session_data):
    """Create the plugin (`plugins.create_new_plugin`) and open its source for editing."""
    if not clicks:
        raise PreventUpdate
    from meerschaum.api.dash.plugins import create_new_plugin
    success, message, url = create_new_plugin(
        name, language, packaging, features, action, dt_col, directory, session_data,
    )
    if not success:
        return _new_plugin_alert(message), no_update, no_update
    return [], False, url


def _new_plugin_alert(message: str):
    return dbc.Alert(message, color='danger', id='plugins-new-error', class_name='plugin-new-error')


### Start from a template or Upload files: each shows its own fields and its own button.
dash_app.clientside_callback(
    """function(mode) {
        const upload = mode === 'upload';
        const shown = {}, gone = {display: 'none'};
        return [!upload, upload, !upload, upload ? gone : shown, upload ? shown : gone];
    }""",
    Output('plugins-new-upload-fields', 'hidden'),
    Output('plugins-new-template-fields', 'hidden'),
    Output('plugins-upload-replace-field', 'hidden'),
    Output('plugins-new-create', 'style'),
    Output('plugins-new-upload', 'style'),
    Input('plugins-new-mode', 'value'),
)


@dash_app.callback(
    Output('plugins-new-mode', 'value'),
    Output('plugins-upload-files', 'contents'),
    Output('plugins-upload-files', 'filename'),
    Output('plugins-upload-replace', 'value'),
    Input('plugins-new-modal', 'is_open'),
    prevent_initial_call=True,
)
def reset_new_plugin_upload(is_open):
    """New plugin opens on the template form with no files chosen."""
    if not is_open:
        raise PreventUpdate
    return 'template', None, None, False


@dash_app.callback(
    Output('plugins-upload-chosen', 'children'),
    Output('plugins-new-name', 'value', allow_duplicate=True),
    Input('plugins-upload-files', 'filename'),
    prevent_initial_call=True,
)
def show_chosen_upload(filenames):
    """
    Name the chosen files in the drop zone, and fill Name with the name one file suggests (its
    stem). Nothing here is privileged (Upload checks the session).
    """
    from meerschaum.api.dash.pages.plugins import UPLOAD_PROMPT
    from meerschaum.api.dash.plugin_upload import suggested_name
    if isinstance(filenames, str):
        filenames = [filenames]
    names = [str(name) for name in (filenames or []) if name]
    if not names:
        return UPLOAD_PROMPT, no_update
    chosen = names[0] if len(names) == 1 else f"{len(names)} files: " + ', '.join(names[:3]) + (
        '…' if len(names) > 3 else '')
    suggestion = suggested_name(names)
    return chosen, (suggestion if suggestion else no_update)


@dash_app.callback(
    Output('plugins-new-alert', 'children', allow_duplicate=True),
    Output('plugins-new-modal', 'is_open', allow_duplicate=True),
    Output('plugins-navigate', 'data', allow_duplicate=True),
    Input('plugins-new-upload', 'n_clicks'),
    State('plugins-upload-files', 'contents'),
    State('plugins-upload-files', 'filename'),
    State('plugins-new-name', 'value'),
    State('plugins-new-directory', 'value'),
    State('plugins-upload-replace', 'value'),
    State('session-store', 'data'),
    prevent_initial_call=True,
    running=[(Output('plugins-new-upload', 'disabled'), True, False)],
)
def upload_plugin(clicks, contents, filenames, name, directory, replace, session_data):
    """
    Install the uploaded files as a plugin (`plugin_upload.upload_plugin`, which checks the
    session's authoring rights and every path) and open the installed plugin's page.
    """
    if not clicks:
        raise PreventUpdate
    from meerschaum.api.dash.plugin_upload import upload_plugin as install_upload
    success, message, url = install_upload(
        contents, filenames, name, directory, replace is True, session_data,
    )
    if not success:
        return _new_plugin_alert(message), no_update, no_update
    return [], False, url


dash_app.clientside_callback(
    """function(url) {
        // New plugin lands on the plugin's source, the way links move between pages.
        if (url && window.mrsmNavigate) window.mrsmNavigate(url);
        else if (url) window.location.assign(url);
        return dash_clientside.no_update;
    }""",
    Output('plugins-navigate', 'modified_timestamp'),
    Input('plugins-navigate', 'data'),
    prevent_initial_call=True,
)


@dash_app.callback(
    Output('plugin-publish-modal', 'is_open'),
    Input('plugin-publish-open', 'n_clicks'),
    Input('plugin-publish-close', 'n_clicks'),
    prevent_initial_call=True,
)
def toggle_plugin_publish(open_clicks, close_clicks):
    return dash.callback_context.triggered_id == 'plugin-publish-open' and bool(open_clicks)


dash_app.clientside_callback(
    """function(method) {
        return [method !== 'password', method !== 'client_credentials', method !== 'api_key'];
    }""",
    Output('plugin-publish-password-fields', 'hidden'),
    Output('plugin-publish-client-fields', 'hidden'),
    Output('plugin-publish-key-fields', 'hidden'),
    Input('plugin-publish-method', 'value'),
)


@dash_app.callback(
    Output('plugin-publish-alert', 'children'),
    Output('plugin-publish-auth', 'hidden'),
    Output('plugin-publish-submit', 'disabled'),
    Output('plugin-publish-password', 'value'),
    Output('plugin-publish-client-secret', 'value'),
    Output('plugin-publish-api-key', 'value'),
    Output('plugin-publish-credentials', 'data'),
    Input('plugin-publish-modal', 'is_open'),
    Input('plugin-publish-repository', 'value'),
    Input('plugin-publish-login', 'n_clicks'),
    Input('plugin-publish-submit', 'n_clicks'),
    State('plugin-publish-context', 'data'),
    State('session-store', 'data'),
    State('plugin-publish-method', 'value'),
    State('plugin-publish-username', 'value'),
    State('plugin-publish-password', 'value'),
    State('plugin-publish-client-id', 'value'),
    State('plugin-publish-client-secret', 'value'),
    State('plugin-publish-api-key', 'value'),
    State('plugin-publish-persist', 'value'),
    State('plugin-publish-credentials', 'data'),
    prevent_initial_call=True,
    running=[(Output('plugin-publish-auth', 'disabled'), True, False),
             (Output('plugin-publish-controls', 'disabled'), True, False),
             (Output('plugin-publish-repository', 'disabled'), True, False)],
)
def plugin_publish_action(is_open, repository, login_clicks, publish_clicks, context, session,
                          method, username, password, client_id, client_secret, api_key, persist, browser):
    from meerschaum.api.dash import plugin_publish as publishing
    if not is_open:
        return no_update, True, True, '', '', '', no_update
    trigger = dash.callback_context.triggered_id
    if trigger == 'plugin-publish-submit' and publish_clicks:
        ok, message = publishing.publish(context, repository, session, browser=browser)
        # A permission error is not a request to overwrite the repository's authentication.
        ready, _ = publishing.repository_status(context, repository, session, browser=browser)
        return dbc.Alert(message, color='success' if ok else 'danger'), ready, not ready, '', '', '', no_update
    if trigger == 'plugin-publish-login' and login_clicks:
        ready, message, stored = publishing.login_repository(
            context, repository, session, method, username, password, client_id, client_secret, api_key,
            persist=persist, browser=browser,
        )
        return dbc.Alert(message, color='success' if ready else 'danger'), ready, not ready, '', '', '', stored
    ready, message = publishing.repository_status(context, repository, session, browser=browser)
    identity = (session or {}).get('session-id') or (context or {}).get('nonce')
    stored = no_update if isinstance(browser, dict) and browser.get('session') == identity else {}
    return dbc.Alert(message, color='success' if ready else 'secondary'), ready, not ready, '', '', '', stored
