#! /usr/bin/env python3
# vim:fenc=utf-8

"""
Callbacks of the Pipes pages: the table (`/dash/pipes`), a pipe's page
(`/dash/pipes/<connector>/<metric>[/<location>]`) and New pipe (`/dash/pipes/new`).
"""

import json
import time
from typing import List, Optional, Dict, Any

import dash
from dash.dependencies import Input, Output, State, ALL, MATCH
from dash.exceptions import PreventUpdate
import dash_bootstrap_components as dbc

import meerschaum as mrsm
from meerschaum.api.dash import dash_app
from meerschaum.api.dash.components import alert_from_success_tuple
from meerschaum.api.dash.pipes import (
    build_relations_body,
    add_pipe_relation,
    remove_pipe_relation,
)
from meerschaum.api.dash import pipes_table, pipe_detail, new_pipe, pipes_access
from meerschaum.api import CHECK_UPDATE
from meerschaum.utils.packages import import_html, import_dcc
from meerschaum.api.dash.sessions import is_state_authenticated
html, dcc = import_html(check_update=CHECK_UPDATE), import_dcc(check_update=CHECK_UPDATE)

#: The keys filters' dropdowns, in order.
KEY_FILTERS = (
    ('connector_keys', 'pipes-connector-keys-dropdown'),
    ('metric_keys', 'pipes-metric-keys-dropdown'),
    ('location_keys', 'pipes-location-keys-dropdown'),
    ('tags', 'pipes-tags-dropdown'),
)

#: The direction a column sorts in when its header is first clicked.
DEFAULT_ASCENDING = {'pipe': True, 'rows': False, 'sync_time': False, 'status': True, 'team': True}


def _is_table_path(pathname: Optional[str]) -> bool:
    return str(pathname or '').rstrip('/') == '/dash/pipes'


# ------------------------------------------------------------------------------------------------
# The table
# ------------------------------------------------------------------------------------------------

@dash_app.callback(
    *[Output(dropdown_id, 'options') for _, dropdown_id in KEY_FILTERS],
    *[Output(dropdown_id, 'value') for _, dropdown_id in KEY_FILTERS],
    Input('pipes-location', 'pathname'),
    State('pipes-location', 'search'),
    State('pipes-page-meta', 'data'),
    State('session-store', 'data'),
)
def init_pipes_filters(pathname, search, page_meta, session_data):
    """Fill the key and tag filters from the URL and the instance's keys."""
    if not _is_table_path(pathname) or not pipes_access.can_read(session_data):
        raise PreventUpdate
    filters = pipes_table.filters_from_search(search)
    instance_keys = (page_meta or {}).get('instance') or pipes_table.global_instance_keys()
    from meerschaum.api.dash import engine
    try:
        options = pipes_table.filter_options(
            engine.get_instance(instance_keys, strict=True),
            viewer=pipes_access.viewer(session_data),
        )
    except Exception:
        options = {key: [] for key, _ in KEY_FILTERS}
    out_options, out_values = [], []
    for key, _ in KEY_FILTERS:
        selected = [str(value) for value in filters.get(key, [])]
        out_options.append(sorted(set(options.get(key, [])) | set(selected), key=str.lower))
        out_values.append(selected)
    return (*out_options, *out_values)


@dash_app.callback(
    Output('pipes-location', 'search'),
    Input('pipes-connector-keys-dropdown', 'value', allow_optional=True),
    Input('pipes-metric-keys-dropdown', 'value', allow_optional=True),
    Input('pipes-location-keys-dropdown', 'value', allow_optional=True),
    Input('pipes-tags-dropdown', 'value', allow_optional=True),
    Input('pipes-status-select', 'value', allow_optional=True),
    Input('pipes-clear-all-button', 'n_clicks', allow_optional=True),
    Input('pipes-scope-select', 'value', allow_optional=True),
    State('pipes-location', 'search'),
    State('pipes-page-meta', 'data'),
    State('pipes-search', 'value', allow_optional=True),
    prevent_initial_call=True,
)
def update_location_on_pipes_filter_change(
    connector_keys: Optional[List[str]],
    metric_keys: Optional[List[str]],
    location_keys: Optional[List[str]],
    tags: Optional[List[str]],
    status: Optional[str],
    clear_all_button_n_clicks: Optional[int],
    scope: Optional[str],
    current_search: Optional[str],
    page_meta: Optional[Dict[str, Any]],
    search_text: Optional[str] = None,
):
    """
    Keep the filters in the URL (`?connector_keys=...&tags=...&status=...&scope=mine&q=...`), so
    a filtered table can be shared; a deep link's `instance` stays. Typing in the search box
    updates `q` in place (below), without a history entry per word.
    """
    if dash.callback_context.triggered_id == 'pipes-clear-all-button':
        if not clear_all_button_n_clicks:
            raise PreventUpdate
        connector_keys, metric_keys, location_keys, tags, status = [], [], [], [], 'any'
        search_text = ''
        scope = 'all'
    page_meta = page_meta or {}
    instance = page_meta.get('instance', None)
    if instance == page_meta.get('global', None):
        instance = None
    search = pipes_table.search_from_filters(
        connector_keys, metric_keys, location_keys, tags, status, instance=instance, q=search_text,
        scope=scope,
    )
    if search == (current_search or ''):
        raise PreventUpdate
    return search


### The search is in the URL (`?q=`) so a searched table can be shared. Typing replaces the
### current history entry (the filters above add one per change). `dcc.Location` compares its
### props with the address bar and pushes its own `href` back when they differ, so its props are
### set to the new address too (the rows are not read again: `load_pipes_rows`).
dash_app.clientside_callback(
    """
    function(text) {
        if (window.location.pathname.replace(/\\/+$/, '') !== '/dash/pipes') {
            return dash_clientside.no_update;
        }
        const q = String(text || '').trim();
        const parts = window.location.search.replace(/^\\?/, '').split('&')
            .filter(part => part && part.split('=')[0] !== 'q');
        if (q) parts.push('q=' + encodeURIComponent(q).replace(/%20/g, '+'));
        const search = parts.length ? '?' + parts.join('&') : '';
        if (search === window.location.search) {
            return dash_clientside.no_update;
        }
        window.history.replaceState(
            window.history.state, '', window.location.pathname + search + window.location.hash,
        );
        dash_clientside.set_props(
            'pipes-location', {search: window.location.search, href: window.location.href},
        );
        return dash_clientside.no_update;
    }
    """,
    Output('pipes-search-url', 'children'),
    Input('pipes-search', 'value'),
)


@dash_app.callback(
    *[Output(dropdown_id, 'value', allow_duplicate=True) for _, dropdown_id in KEY_FILTERS],
    Output('pipes-status-select', 'value'),
    Output('pipes-search', 'value'),
    Output('pipes-scope-select', 'value'),
    Input('pipes-clear-all-button', 'n_clicks'),
    prevent_initial_call=True,
)
def clear_pipes_filters(n_clicks):
    """Clear all: no keys, tags, status, owner filter or search."""
    if not n_clicks:
        raise PreventUpdate
    return [], [], [], [], 'any', '', 'all'


@dash_app.callback(
    Output('pipes-rows-store', 'data'),
    Output('pipes-alert', 'children'),
    Input('pipes-location', 'search'),
    Input('pipes-refresh-button', 'n_clicks'),
    State('pipes-location', 'pathname'),
    State('pipes-page-meta', 'data'),
    State('session-store', 'data'),
    State('pipes-rows-store', 'data'),
)
def load_pipes_rows(search, refresh_clicks, pathname, page_meta, session_data, rows_store=None):
    """
    List the pipes matching the URL's filters (keys, tags and targets only: fast). Refresh starts
    a new `generation`, which drops the statistics computed so far. A URL that changed only its
    search or status keeps the rows (those narrow the table without a new read).
    """
    if not _is_table_path(pathname) or not pipes_access.can_read(session_data):
        raise PreventUpdate
    refreshed = dash.callback_context.triggered_id == 'pipes-refresh-button'
    if refreshed:
        if not refresh_clicks:
            raise PreventUpdate
        pipes_table.clear_cache()
    filters = pipes_table.filters_from_search(search)
    instance_keys = (page_meta or {}).get('instance') or pipes_table.global_instance_keys()
    rows_key = pipes_table.rows_key(filters, instance_keys)
    if (
        not refreshed
        and isinstance(rows_store, dict)
        and rows_store.get('rows_key') == rows_key
    ):
        raise PreventUpdate
    from meerschaum.api.dash import engine
    viewer = pipes_access.viewer(session_data)
    instance = None
    try:
        instance = engine.get_instance(instance_keys, strict=True)
        rows = pipes_table.list_rows(
            instance,
            connector_keys=filters['connector_keys'],
            metric_keys=filters['metric_keys'],
            location_keys=filters['location_keys'],
            tags=filters['tags'],
            viewer=viewer,
        )
        alert = []
    except Exception as e:
        rows = []
        alert = alert_from_success_tuple((
            False,
            f"Failed to get pipes for instance '{instance_keys}':\n{pipes_table.error_text(e, instance)}",
        ))
    store = {
        'instance': instance_keys,
        'rows': rows,
        'search': search or '',
        ### A failed read is tried again on the next change of the URL.
        'rows_key': (None if alert else rows_key),
        'loaded': time.time(),
        'generation': int(refresh_clicks or 0),
        ### Mine / My teams, and whether the Team column has anything to say.
        'me': pipes_table.viewer_summary(viewer),
        'teams': bool(any(row.get('team') for row in rows) or viewer.own_teams),
    }
    return store, alert


def _sort_args(sort: Optional[Dict[str, Any]]):
    sort = sort or {}
    column = sort.get('column', 'pipe')
    if column not in dict(pipes_table.SORT_COLUMNS):
        column = 'pipe'
    return column, bool(sort.get('ascending', True))


### After each render of the table, ask for the statistics it still needs in a new chain of
### callbacks: chained to the rows, the render would wait for the statistics (Dash holds a callback
### while another one writing its inputs is pending), and chained through an output of the render,
### the statistics would not render again (a cycle of callbacks runs once per chain).
dash_app.clientside_callback(
    """
    function(rendered) {
        if (!rendered) return dash_clientside.no_update;
        setTimeout(function () {
            dash_clientside.set_props('pipes-stats-request', {data: rendered});
        }, 0);
        return dash_clientside.no_update;
    }
    """,
    Output('pipes-stats-dummy', 'children'),
    Input('pipes-render-done', 'data'),
)


@dash_app.callback(
    Output('pipes-stats-store', 'data'),
    Input('pipes-stats-request', 'data'),
    State('pipes-rows-store', 'data'),
    State('pipes-sort-store', 'data'),
    State('pipes-search', 'value'),
    State('pipes-status-select', 'value'),
    State('pipes-stats-store', 'data'),
    State('session-store', 'data'),
    State('pipes-scope-select', 'value', allow_optional=True),
    prevent_initial_call=True,
)
def fill_pipes_stats(rendered, rows_store, sort, search, status, stats, session_data, scope=None):
    """
    After the table renders (its keys first), fill the slow columns (rows, sync time, status,
    jobs) of the rows on screen, or of every matching row when sorting by one of them or
    filtering by status; the table renders again with them. Only pipes the viewer may see are
    read (`pipes_table.compute_stats` checks the store's rows again).
    """
    if not rows_store or not isinstance(rendered, dict) or not pipes_access.can_read(session_data):
        raise PreventUpdate
    generation = rows_store.get('generation', 0)
    if rendered.get('generation', 0) != generation:
        raise PreventUpdate
    current = _stats_of(stats, rows_store)
    column, ascending = _sort_args(sort)
    needed = pipes_table.rows_needing_stats(
        rows_store.get('rows') or [], current, search, status, column, ascending,
        int(rendered.get('page') or 1), scope=scope, me=rows_store.get('me'),
    )
    if not needed:
        raise PreventUpdate
    current = dict(current)
    current.update(pipes_table.compute_stats(
        rows_store.get('instance'), needed, viewer=pipes_access.viewer(session_data),
    ))
    return {'generation': generation, 'stats': current}


def _stats_of(stats_store, rows_store) -> Dict[str, Any]:
    """The statistics of the rows' generation (none after a Refresh)."""
    if not isinstance(stats_store, dict) or not rows_store:
        return {}
    if stats_store.get('generation', 0) != rows_store.get('generation', 0):
        return {}
    return stats_store.get('stats', None) or {}


@dash_app.callback(
    Output('pipes-table-markdown', 'children'),
    Output('pipes-summary', 'children'),
    Output('pipes-pagination', 'max_value'),
    Output('pipes-pagination', 'active_page'),
    Output('pipes-pagination-div', 'hidden'),
    Output('pipes-render-done', 'data'),
    Input('pipes-rows-store', 'data'),
    Input('pipes-stats-store', 'data'),
    Input('pipes-search', 'value'),
    Input('pipes-status-select', 'value'),
    Input('pipes-sort-store', 'data'),
    Input('pipes-pagination', 'active_page'),
    Input('pipes-scope-select', 'value', allow_optional=True),
    State('pipes-page-meta', 'data'),
)
def render_pipes_table(rows_store, stats, search, status, sort, page, scope, page_meta):
    """
    Search, filter, sort and page the rows (the table's HTML); `pipes-render-done` then asks for
    the statistics the page still needs (above).
    """
    if not rows_store:
        raise PreventUpdate
    if dash.callback_context.triggered_id in (
        'pipes-search', 'pipes-status-select', 'pipes-sort-store', 'pipes-scope-select',
    ):
        page = 1
    rows = rows_store.get('rows') or []
    stats = _stats_of(stats, rows_store)
    column, ascending = _sort_args(sort)
    me = rows_store.get('me')
    page_rows, page, num_pages, num_matching = pipes_table.visible_rows(
        rows, stats, search, status, column, ascending, page or 1, scope=scope, me=me,
    )
    filtering = status not in (None, '', 'any')
    show_team = bool(rows_store.get('teams', True))
    table = pipes_table.render_table_html(
        page_rows, stats, (page_meta or {}).get('global'),
        sort={'column': column, 'ascending': ascending},
        empty_text=('No pipes match.' if rows else 'No pipes yet.'),
        loading=(filtering and any(
            row['ref'] not in stats
            for row in pipes_table.filter_scope(pipes_table.search_rows(rows, search), scope, me)
        )),
        show_team=show_team,
    )
    summary = pipes_table.summary_text(num_matching, len(rows), page, num_pages)
    rendered = {'generation': rows_store.get('generation', 0), 'page': page, 'at': time.time()}
    return table, summary, num_pages, page, num_pages <= 1, rendered


@dash_app.callback(
    Output('pipes-sort-store', 'data'),
    Output('pipes-sort-select', 'value'),
    Input('pipes-sort-click', 'data'),
    Input('pipes-sort-select', 'value'),
    State('pipes-sort-store', 'data'),
    prevent_initial_call=True,
)
def sort_pipes(click, select_value, sort):
    """
    Sort by a header (`pipes-sort-click`, from `assets/pipes.js`; again: the other direction) or
    by the phone's Sort select.
    """
    trigger = dash.callback_context.triggered_id
    column, ascending = _sort_args(sort)
    if trigger == 'pipes-sort-select':
        try:
            new_column, direction = str(select_value).split(':', 1)
        except ValueError:
            raise PreventUpdate
        if new_column not in dict(pipes_table.SORT_COLUMNS):
            raise PreventUpdate
        new_sort = {'column': new_column, 'ascending': direction != 'desc'}
        if new_sort == {'column': column, 'ascending': ascending}:
            raise PreventUpdate
        return new_sort, dash.no_update
    if trigger != 'pipes-sort-click' or not isinstance(click, dict):
        raise PreventUpdate
    new_column = str(click.get('column') or '')
    if new_column not in dict(pipes_table.SORT_COLUMNS):
        raise PreventUpdate
    new_ascending = (not ascending) if new_column == column else DEFAULT_ASCENDING.get(new_column, True)
    return (
        {'column': new_column, 'ascending': new_ascending},
        f"{new_column}:{'asc' if new_ascending else 'desc'}",
    )


# ------------------------------------------------------------------------------------------------
# A pipe's page
# ------------------------------------------------------------------------------------------------

def _meta_pipe(meta: Optional[Dict[str, Any]], session_data: Optional[Dict[str, Any]] = None) -> mrsm.Pipe:
    """
    The pipe of a page's `pipe-detail-meta` (the browser's store) if the session's viewer may
    see it on an instance the API allows, else no update.
    """
    if not meta:
        raise PreventUpdate
    from meerschaum.api.dash import access
    pipe = access.visible_pipe_from_meta(meta, viewer=pipes_access.viewer(session_data))
    if pipe is None:
        raise PreventUpdate
    return pipe


@dash_app.callback(
    Output('pipe-detail-result', 'data'),
    Input('pipe-detail-tabs', 'active_tab'),
    Input('pipe-detail-edit', 'data'),
    Input('pipe-detail-refresh', 'data'),
    State('pipe-detail-meta', 'data'),
    State('session-store', 'data'),
)
def render_pipe_detail_tab(active_tab, edit, refresh, meta, session_data):
    """
    The active tab (re-read after an action finishes), tagged with the tab and Edit mode it was
    built for: a slow tab answered after the next one was chosen is not shown (see below). It
    also carries whether the pipe is registered (`None`: its instance could not be read).
    """
    if not pipes_access.can_read(session_data) or not meta:
        raise PreventUpdate
    from meerschaum.api.dash import access
    viewer = pipes_access.viewer(session_data)
    try:
        pipe, error = access.resolve_visible_pipe(
            meta['connector_keys'], meta['metric_key'], meta.get('location_key', None),
            meta.get('instance_keys', None), viewer=viewer,
        )
    except (KeyError, TypeError):
        raise PreventUpdate
    tab = active_tab or 'overview'
    if pipe is None:
        ### A pipe the viewer may not see reads as one that is not registered; an instance that
        ### could not be read to tell says so (that names no pipe).
        pipe = pipe_detail.pipe_from_meta(meta)
        state = (None, pipe_detail.read_error(pipe, error)) if error is not None else (False, '')
        rights = {}
    else:
        if dash.callback_context.triggered_id == 'pipe-detail-refresh':
            pipes_table.clear_cache(pipe_detail.pipe_ref(pipe))
        state = pipe_detail.registration(pipe)
        rights = (
            {
                **pipe_detail.pipe_rights(viewer, pipe),
                'read': True,
                'define_sql': viewer.can('pipes:define_sql'),
            }
            if state[0] else {}
        )
    registered = state[0]
    can_register = registered is False and pipes_access.can_register(session_data)[0]
    try:
        children = pipe_detail.build_tab(
            tab, pipe, edit=bool(edit), state=state, can_register=can_register, rights=rights,
        )
    except Exception as e:
        children = alert_from_success_tuple((False, f"Could not load this tab: {e}"))
    return {'tab': tab, 'edit': bool(edit), 'children': children, 'registered': registered}


### Responses may arrive out of order (the Overview reads the database; Lineage plans a graph):
### only the answer for the tab and mode on screen replaces the content. Any answer says whether
### the pipe is registered: Sync, Verify, Edit and Manage show only for a pipe that is.
dash_app.clientside_callback(
    """
    function(result, tab, edit) {
        if (!result) {
            return [dash_clientside.no_update, dash_clientside.no_update];
        }
        const hidden = result.registered !== true;
        if (result.tab !== (tab || 'overview') || result.edit !== !!edit) {
            return [dash_clientside.no_update, hidden];
        }
        return [result.children, hidden];
    }
    """,
    Output('pipe-detail-content', 'children'),
    Output('pipe-detail-actions', 'hidden'),
    Input('pipe-detail-result', 'data'),
    State('pipe-detail-tabs', 'active_tab'),
    State('pipe-detail-edit', 'data'),
)


@dash_app.callback(
    Output('pipe-detail-tab-runs', 'tab_style'),
    Input('pipe-detail-meta', 'data'),
    State('session-store', 'data'),
)
def show_runs_tab(meta, session_data):
    """Runs appears only on instances that keep engine runs."""
    if not pipes_access.can_read(session_data):
        raise PreventUpdate
    pipe = _meta_pipe(meta, session_data)
    return {} if pipe_detail.runs_available(pipe.instance_connector) else {'display': 'none'}


dash_app.clientside_callback(
    """
    function(tab) {
        // The active tab is in the URL (`?tab=`), so a reload or a shared link opens it.
        if (!tab) return dash_clientside.no_update;
        const url = new URL(window.location.href);
        if (tab === 'overview') url.searchParams.delete('tab');
        else url.searchParams.set('tab', tab);
        if (url.href !== window.location.href) window.history.replaceState(window.history.state, '', url.href);
        const page = document.getElementById('pipe-detail-page');
        if (page) page.dataset.tab = tab;
        return dash_clientside.no_update;
    }
    """,
    Output('pipe-detail-dummy', 'children'),
    Input('pipe-detail-tabs', 'active_tab'),
)


@dash_app.callback(
    Output('pipe-detail-edit', 'data'),
    Output('pipe-edit-toggle', 'children'),
    Output('pipe-edit-toggle', 'color'),
    Output('pipe-detail-tabs', 'active_tab'),
    Output('pipe-detail-page', 'className'),
    Input('pipe-edit-toggle', 'n_clicks', allow_optional=True),
    State('pipe-detail-edit', 'data'),
    State('pipe-detail-tabs', 'active_tab'),
    State('session-store', 'data'),
    State('pipe-detail-meta', 'data'),
    prevent_initial_call=True,
)
def toggle_pipe_edit(n_clicks, edit, active_tab, session_data, meta=None):
    """Edit makes the parameters, definition, lineage and rows editable; Done ends it."""
    if not n_clicks:
        raise PreventUpdate
    from meerschaum.api.dash import access
    viewer = pipes_access.viewer(session_data)
    if not access.can_edit_pipe(viewer, _meta_pipe(meta, session_data)):
        raise PreventUpdate
    edit = not bool(edit)
    tab = active_tab
    if edit and active_tab not in pipe_detail.EDITABLE_TABS:
        tab = pipe_detail.EDIT_TAB
    return (
        edit,
        ('Done' if edit else 'Edit'),
        ('warning' if edit else 'secondary'),
        (tab if tab != active_tab else dash.no_update),
        'pipes-page pipes-detail-page' + (' pipes-editing' if edit else ''),
    )


def _start_action(sysargs: List[str], action: str, session_data, ref: str):
    """Run `sysargs` on the server; return `(store, alert)`."""
    from meerschaum.api.dash.execute import run_action
    import shlex
    try:
        result = run_action(sysargs, pipes_access.username(session_data))
    except Exception as e:
        result = {'success': False, 'msg': str(e), 'job': None, 'stream': None}
    if not result.get('success'):
        return dash.no_update, alert_from_success_tuple((False, result.get('msg') or f"Could not {action}."))
    return {
        'job': result.get('job'),
        'stream': result.get('stream'),
        'command': shlex.join(sysargs),
        'action': action,
        'ref': ref,
        'started': time.time(),
    }, []


@dash_app.callback(
    Output('pipe-action-store', 'data'),
    Output('pipe-detail-alert', 'children'),
    Output('pipe-confirm-modal', 'is_open', allow_duplicate=True),
    Input('pipe-action-sync', 'n_clicks', allow_optional=True),
    Input('pipe-action-verify', 'n_clicks', allow_optional=True),
    Input('pipe-confirm-button', 'n_clicks'),
    State('pipe-confirm-store', 'data'),
    State('pipe-confirm-input', 'value'),
    State('pipe-confirm-begin', 'value'),
    State('pipe-confirm-end', 'value'),
    State('pipe-detail-meta', 'data'),
    State('session-store', 'data'),
    prevent_initial_call=True,
)
def run_pipe_action(sync_clicks, verify_clicks, confirm_clicks, confirm, typed, begin, end, meta, session_data):
    """Sync, Verify, or the confirmed Clear, Drop or Delete, run on the server with streamed output."""
    trigger = dash.callback_context.triggered_id
    clicks = {
        'pipe-action-sync': sync_clicks,
        'pipe-action-verify': verify_clicks,
        'pipe-confirm-button': confirm_clicks,
    }
    if not clicks.get(trigger):
        raise PreventUpdate
    pipe = _meta_pipe(meta, session_data)
    ref = pipe_detail.pipe_ref(pipe)
    if trigger == 'pipe-confirm-button':
        action = (confirm or {}).get('action')
        if action not in pipe_detail.DESTRUCTIVE_ACTIONS or (confirm or {}).get('ref') != ref:
            raise PreventUpdate
        if str(typed or '').strip() != str(pipe.metric_key):
            raise PreventUpdate
    else:
        action = 'sync' if trigger == 'pipe-action-sync' else 'verify'
    ### Its permission, edit rights on this pipe and its locks (the server checks them again).
    from meerschaum.api.dash import access
    allowed, reason = access.may_run_pipe_action(pipes_access.viewer(session_data), pipe, action)
    if not allowed:
        return dash.no_update, alert_from_success_tuple((False, reason)), False
    sysargs, message = pipe_detail.action_sysargs(pipe_detail.meta_of(pipe), action, begin=begin, end=end)
    if sysargs is None:
        return dash.no_update, alert_from_success_tuple((False, message)), dash.no_update
    store, alert = _start_action(sysargs, action, session_data, ref)
    return store, alert, (False if trigger == 'pipe-confirm-button' else dash.no_update)


@dash_app.callback(
    Output('pipe-confirm-modal', 'is_open'),
    Output('pipe-confirm-store', 'data'),
    Output('pipe-confirm-title', 'children'),
    Output('pipe-confirm-consequence', 'children'),
    Output('pipe-confirm-bounds', 'hidden'),
    Output('pipe-confirm-input', 'value'),
    Output('pipe-confirm-button', 'children'),
    Output('pipe-detail-alert', 'children', allow_duplicate=True),
    Input('pipe-manage-clear', 'n_clicks', allow_optional=True),
    Input('pipe-manage-drop', 'n_clicks', allow_optional=True),
    Input('pipe-manage-delete', 'n_clicks', allow_optional=True),
    Input('pipe-confirm-cancel', 'n_clicks'),
    State('pipe-detail-meta', 'data'),
    State('session-store', 'data'),
    prevent_initial_call=True,
)
def open_pipe_confirm(clear_clicks, drop_clicks, delete_clicks, cancel_clicks, meta, session_data):
    """
    Ask before Clear, Drop or Delete: the pipe's keys, what happens, and its metric typed. A
    locked pipe (or one the viewer may not change) says why instead.
    """
    no = dash.no_update
    trigger = dash.callback_context.triggered_id
    if trigger == 'pipe-confirm-cancel':
        if not cancel_clicks:
            raise PreventUpdate
        return False, None, no, no, no, '', no, no
    clicks = {
        'pipe-manage-clear': clear_clicks,
        'pipe-manage-drop': drop_clicks,
        'pipe-manage-delete': delete_clicks,
    }
    if not clicks.get(trigger):
        raise PreventUpdate
    pipe = _meta_pipe(meta, session_data)
    action = trigger[len('pipe-manage-'):]
    from meerschaum.api.dash import access
    allowed, reason = access.may_run_pipe_action(pipes_access.viewer(session_data), pipe, action)
    if not allowed:
        return no, no, no, no, no, no, no, alert_from_success_tuple((False, reason))
    title, button, consequence = pipe_detail.DESTRUCTIVE_ACTIONS[action]
    return (
        True,
        {'action': action, 'ref': pipe_detail.pipe_ref(pipe)},
        title,
        consequence,
        action != 'clear',
        '',
        button,
        [],
    )


dash_app.clientside_callback(
    """
    function(typed, meta) {
        // The destructive button waits for the pipe's metric key.
        return !(meta && typed !== undefined && typed !== null && typed.trim() === String(meta.metric_key));
    }
    """,
    Output('pipe-confirm-button', 'disabled'),
    Input('pipe-confirm-input', 'value'),
    State('pipe-detail-meta', 'data'),
)


@dash_app.callback(
    Output('pipe-copy-modal', 'is_open'),
    Output('pipe-copy-alert', 'children'),
    Input('pipe-manage-copy', 'n_clicks', allow_optional=True),
    Input('pipe-copy-cancel', 'n_clicks'),
    State('session-store', 'data'),
    prevent_initial_call=True,
)
def open_pipe_copy(copy_clicks, cancel_clicks, session_data):
    trigger = dash.callback_context.triggered_id
    if trigger == 'pipe-copy-cancel':
        if not cancel_clicks:
            raise PreventUpdate
        return False, []
    if not copy_clicks or not pipes_access.can_register(session_data)[0]:
        raise PreventUpdate
    return True, []


dash_app.clientside_callback(
    """
    function(connector, metric, location, instance, meta) {
        // Rows are copied with the registration to another instance under the same keys.
        if (!meta) return dash_clientside.no_update;
        const same = (connector || '').trim() === meta.connector_keys
            && (metric || '').trim() === meta.metric_key
            && (((location || '').trim() || null) === (meta.location_key || null)
                || ((location || '').trim() === 'None' && !meta.location_key));
        return (same && instance && instance !== meta.instance_keys) ? {} : {display: 'none'};
    }
    """,
    Output('pipe-copy-rows', 'style'),
    Input('pipe-copy-connector', 'value'),
    Input('pipe-copy-metric', 'value'),
    Input('pipe-copy-location', 'value'),
    Input('pipe-copy-instance', 'value'),
    State('pipe-detail-meta', 'data'),
)


@dash_app.callback(
    Output('pipe-copy-alert', 'children', allow_duplicate=True),
    Output('pipe-copy-modal', 'is_open', allow_duplicate=True),
    Output('pipe-copy-store', 'data'),
    Output('pipe-detail-alert', 'children', allow_duplicate=True),
    Input('pipe-copy-button', 'n_clicks'),
    State('pipe-copy-connector', 'value'),
    State('pipe-copy-metric', 'value'),
    State('pipe-copy-location', 'value'),
    State('pipe-copy-instance', 'value'),
    State('pipe-copy-rows', 'value'),
    State('pipe-detail-meta', 'data'),
    State('session-store', 'data'),
    prevent_initial_call=True,
)
def copy_pipe_click(n_clicks, connector_keys, metric_key, location_key, instance_keys, copy_rows, meta, session_data):
    """
    Copy: to another instance under the same keys, `copy pipes <instance>` (rows too, streamed);
    under new keys, a new registration with the same parameters.
    """
    if not n_clicks:
        raise PreventUpdate
    allowed, reason = pipes_access.can_register(session_data)
    if not allowed:
        return alert_from_success_tuple((False, reason)), dash.no_update, dash.no_update, dash.no_update
    pipe = _meta_pipe(meta, session_data)
    from meerschaum.api.dash import engine
    try:
        engine.get_instance(instance_keys, strict=True)
    except Exception as e:
        return alert_from_success_tuple((False, str(e))), dash.no_update, dash.no_update, dash.no_update
    destination, message = pipe_detail.copy_destination(
        pipe, connector_keys, metric_key, location_key, instance_keys,
    )
    if destination is None:
        return alert_from_success_tuple((False, message)), dash.no_update, dash.no_update, dash.no_update
    if destination['same_keys']:
        sysargs = pipe_detail.copy_sysargs(pipe, destination['instance_keys'], copy_rows=bool(copy_rows))
        store, alert = _start_action(sysargs, 'copy', session_data, pipe_detail.pipe_ref(pipe))
        if store is dash.no_update:
            return alert, dash.no_update, dash.no_update, dash.no_update
        return [], False, store, []
    (success, message), new_pipe = pipe_detail.copy_pipe(
        pipe, destination, viewer=pipes_access.viewer(session_data),
    )
    if not success:
        return alert_from_success_tuple((False, message)), dash.no_update, dash.no_update, dash.no_update
    link = dcc.Link(
        pipes_table.pipe_title(new_pipe.connector_keys, new_pipe.metric_key, new_pipe.location_key),
        href=pipes_table.pipe_path(new_pipe.connector_keys, new_pipe.metric_key, new_pipe.location_key,
                                   str(new_pipe.instance_keys)),
        className='pipes-table-link',
    )
    alert = dbc.Alert(['Copied to ', link], color='success', dismissable=True, id='pipe-copy-success')
    return [], False, dash.no_update, alert


### Each action's store has one writer: a store written by two callbacks (`allow_duplicate`)
### does not trigger the callbacks listening to it under the console's `MultiplexerTransform`.
dash_app.clientside_callback(
    """
    function(store, copy) {
        const triggered = (dash_clientside.callback_context.triggered || []).map(t => t.prop_id);
        const latest = triggered.includes('pipe-copy-store.data') ? copy : store;
        if (window.mrsmPipes) window.mrsmPipes.follow(latest);
        return dash_clientside.no_update;
    }
    """,
    Output('pipe-action-panel', 'className'),
    Input('pipe-action-store', 'data'),
    Input('pipe-copy-store', 'data'),
)


dash_app.clientside_callback(
    """
    function(n_clicks) {
        if (!n_clicks) return dash_clientside.no_update;
        if (window.mrsmPipes) window.mrsmPipes.close();
        return true;
    }
    """,
    Output('pipe-action-panel', 'hidden'),
    Input('pipe-action-close', 'n_clicks'),
    prevent_initial_call=True,
)


@dash_app.callback(
    Output('pipes-navigate-store', 'data'),
    Input('pipe-detail-refresh', 'data'),
    State('pipe-action-store', 'data'),
    State('pipe-detail-meta', 'data'),
    State('session-store', 'data'),
    prevent_initial_call=True,
)
def after_pipe_action(refresh, action_store, meta, session_data):
    """
    After Delete finishes, return to the table once the pipe is gone (read as unregistered: a
    failed read or a failed Delete stays on the page, which says why).
    """
    if not refresh or not action_store or action_store.get('action') != 'delete':
        raise PreventUpdate
    if not pipes_access.can_read(session_data):
        raise PreventUpdate
    pipe = _meta_pipe(meta, session_data)
    registered, _ = pipe_detail.registration(pipe)
    if registered is not False:
        raise PreventUpdate
    pipes_table.clear_cache()
    return '/dash/pipes'


dash_app.clientside_callback(
    """
    function(follow, meta) {
        // Register and sync lands here: follow the sync it started (or say why it did not start).
        if (!follow || !meta) return dash_clientside.no_update;
        const ref = [meta.instance_keys, meta.connector_keys, meta.metric_key,
                     meta.location_key === null || meta.location_key === undefined ? 'None' : meta.location_key].join('|');
        if (follow.ref !== ref) return dash_clientside.no_update;
        dash_clientside.set_props('pipes-follow-store', {data: null});
        if (follow.error) {
            dash_clientside.set_props('pipe-detail-alert', {children: {
                namespace: 'dash_bootstrap_components', type: 'Alert',
                props: {children: follow.error, color: 'danger', dismissable: true, id: 'pipe-follow-alert'},
            }});
        } else if (window.mrsmPipes) {
            window.mrsmPipes.follow(follow);
        }
        return dash_clientside.no_update;
    }
    """,
    Output('pipe-detail-dummy', 'title'),
    Input('pipes-follow-store', 'data'),
    State('pipe-detail-meta', 'data'),
)


_NAVIGATE = """
    function(url) {
        if (url && window.mrsmNavigate) window.mrsmNavigate(url);
        else if (url) window.location.assign(url);
        return dash_clientside.no_update;
    }
"""
dash_app.clientside_callback(
    _NAVIGATE,
    Output('pipes-navigate-store', 'modified_timestamp'),
    Input('pipes-navigate-store', 'data'),
    prevent_initial_call=True,
)
dash_app.clientside_callback(
    _NAVIGATE,
    Output('new-pipe-navigate-store', 'modified_timestamp'),
    Input('new-pipe-navigate-store', 'data'),
    prevent_initial_call=True,
)


@dash_app.callback(
    Output('pipe-fetch-alert', 'children'),
    Input('pipe-fetch-update', 'n_clicks'),
    State('pipe-fetch-editor', 'value'),
    State('pipe-detail-meta', 'data'),
    State('session-store', 'data'),
    prevent_initial_call=True,
)
def update_pipe_fetch(n_clicks, text, meta, session_data):
    """Update a non-SQL pipe's `fetch` parameters (Definition, in Edit mode)."""
    if not n_clicks:
        raise PreventUpdate
    allowed, reason = pipes_access.can_write_pipes(session_data)
    if not allowed:
        return alert_from_success_tuple((False, reason))
    pipe = _meta_pipe(meta, session_data)
    fetch, message = new_pipe.parse_parameters_text(text)
    if fetch is None:
        return alert_from_success_tuple((False, message.replace('parameters', '`fetch` parameters')))
    from copy import deepcopy
    from meerschaum.api.dash import pipe_writes
    parameters = deepcopy(pipe.get_parameters(apply_symlinks=False, refresh=True) or {})
    if fetch:
        parameters['fetch'] = fetch
    else:
        parameters.pop('fetch', None)
    return alert_from_success_tuple(
        pipe_writes.save_parameters(pipe, parameters, viewer=pipes_access.viewer(session_data))
    )


# ------------------------------------------------------------------------------------------------
# Manage > Access… and Locks…
# ------------------------------------------------------------------------------------------------

#: The Owner select's "No owner" (a select shows an empty value as no choice at all).
NO_OWNER = '__no_owner__'


def owner_options(viewer: Any, current_owner: Optional[str], team: Optional[str]) -> List[Dict[str, str]]:
    """
    Who the Access dialog may name as the owner: any user for administrators and the team's
    managers; otherwise the owner may only keep or give up ownership.
    """
    from meerschaum.api.dash import access
    from meerschaum.api import get_api_connector
    none = {'label': 'No owner', 'value': NO_OWNER}
    if viewer.is_admin or viewer.no_auth or viewer.manages(team):
        try:
            names = sorted(str(name) for name in (get_api_connector().get_users() or []))
        except Exception:
            names = []
        if current_owner and current_owner not in names:
            names.insert(0, current_owner)
        return [none] + [{'label': name, 'value': name} for name in names]
    keep = [current_owner] if current_owner else []
    if viewer.username and viewer.username not in keep and not current_owner:
        keep.append(viewer.username)
    return [none] + [{'label': name, 'value': name} for name in keep]


@dash_app.callback(
    Output('pipe-access-modal', 'is_open'),
    Output('pipe-access-owner', 'options'),
    Output('pipe-access-owner', 'value'),
    Output('pipe-access-team', 'options'),
    Output('pipe-access-team', 'value'),
    Output('pipe-access-visibility', 'value'),
    Output('pipe-access-edit', 'value'),
    Output('pipe-access-alert', 'children'),
    Input('pipe-manage-access', 'n_clicks', allow_optional=True),
    Input('pipe-access-cancel', 'n_clicks'),
    State('pipe-detail-meta', 'data'),
    State('session-store', 'data'),
    prevent_initial_call=True,
)
def open_pipe_access(access_clicks, cancel_clicks, meta, session_data):
    """Access…: the pipe's owner, team, visibility and edit mode (for those who manage it)."""
    from meerschaum.api.dash import access
    no = dash.no_update
    trigger = dash.callback_context.triggered_id
    if trigger == 'pipe-access-cancel':
        if not cancel_clicks:
            raise PreventUpdate
        return False, no, no, no, no, no, no, []
    if not access_clicks:
        raise PreventUpdate
    pipe = _meta_pipe(meta, session_data)
    viewer = pipes_access.viewer(session_data)
    parameters = pipe.get_parameters(apply_symlinks=False, refresh=True) or {}
    if not access.can_manage_access(viewer, parameters):
        raise PreventUpdate
    fields = access.access_fields(parameters)
    team = fields.get('team') or access.EVERYONE
    teams = access.assignable_teams(viewer)
    if team not in teams:
        teams = [team] + teams
    return (
        True,
        owner_options(viewer, fields.get('owner'), fields.get('team')),
        fields.get('owner') or NO_OWNER,
        [{'label': name, 'value': name} for name in teams],
        team,
        fields.get('visibility') or 'team',
        fields.get('edit') or 'team',
        [],
    )


@dash_app.callback(
    Output('pipe-access-modal', 'is_open', allow_duplicate=True),
    Output('pipe-access-alert', 'children', allow_duplicate=True),
    Output('pipe-detail-badges', 'children'),
    Output('pipe-detail-alert', 'children', allow_duplicate=True),
    Input('pipe-access-save', 'n_clicks'),
    State('pipe-access-owner', 'value'),
    State('pipe-access-team', 'value'),
    State('pipe-access-visibility', 'value'),
    State('pipe-access-edit', 'value'),
    State('pipe-detail-meta', 'data'),
    State('session-store', 'data'),
    prevent_initial_call=True,
)
def save_pipe_access(n_clicks, owner, team, visibility, edit, meta, session_data):
    """Save the pipe's access (checked again here: `pipe_writes.set_access`)."""
    if not n_clicks:
        raise PreventUpdate
    from meerschaum.api.dash import access, pipe_writes
    pipe = _meta_pipe(meta, session_data)
    viewer = pipes_access.viewer(session_data)
    owner = None if owner in (None, '', NO_OWNER) else owner
    success, message = pipe_writes.set_access(pipe, owner, team, visibility, edit, viewer=viewer)
    if not success:
        return dash.no_update, alert_from_success_tuple((False, message)), dash.no_update, dash.no_update
    access.forget()
    pipes_table.clear_cache()
    parameters = pipe.get_parameters(apply_symlinks=False, refresh=True) or {}
    return (
        False, [], pipe_detail.build_badges(parameters),
        alert_from_success_tuple((True, f"Saved the access of {pipe}.")),
    )


@dash_app.callback(
    Output('pipe-locks-modal', 'is_open'),
    Output('pipe-locks-value', 'value'),
    Output('pipe-locks-alert', 'children'),
    Input('pipe-manage-locks', 'n_clicks', allow_optional=True),
    Input('pipe-locks-cancel', 'n_clicks'),
    State('pipe-detail-meta', 'data'),
    State('session-store', 'data'),
    prevent_initial_call=True,
)
def open_pipe_locks(locks_clicks, cancel_clicks, meta, session_data):
    """Locks…: what the pipe is locked against (for its owner, team managers and admins)."""
    from meerschaum.api.dash import access
    trigger = dash.callback_context.triggered_id
    if trigger == 'pipe-locks-cancel':
        if not cancel_clicks:
            raise PreventUpdate
        return False, dash.no_update, []
    if not locks_clicks:
        raise PreventUpdate
    pipe = _meta_pipe(meta, session_data)
    parameters = pipe.get_parameters(apply_symlinks=False, refresh=True) or {}
    if not access.can_lock(pipes_access.viewer(session_data), parameters):
        raise PreventUpdate
    return True, access.pipe_locks(parameters), []


@dash_app.callback(
    Output('pipe-locks-modal', 'is_open', allow_duplicate=True),
    Output('pipe-locks-alert', 'children', allow_duplicate=True),
    Output('pipe-detail-badges', 'children', allow_duplicate=True),
    Output('pipe-detail-alert', 'children', allow_duplicate=True),
    Input('pipe-locks-save', 'n_clicks'),
    State('pipe-locks-value', 'value'),
    State('pipe-detail-meta', 'data'),
    State('session-store', 'data'),
    prevent_initial_call=True,
)
def save_pipe_locks(n_clicks, locks, meta, session_data):
    """Lock and unlock the pipe (checked again here: `pipe_writes.set_locks`)."""
    if not n_clicks:
        raise PreventUpdate
    from meerschaum.api.dash import pipe_writes
    pipe = _meta_pipe(meta, session_data)
    success, message = pipe_writes.set_locks(pipe, list(locks or []), viewer=pipes_access.viewer(session_data))
    if not success:
        return dash.no_update, alert_from_success_tuple((False, message)), dash.no_update, dash.no_update
    pipes_table.clear_cache()
    parameters = pipe.get_parameters(apply_symlinks=False, refresh=True) or {}
    return False, [], pipe_detail.build_badges(parameters), alert_from_success_tuple((True, message))


# ------------------------------------------------------------------------------------------------
# New pipe
# ------------------------------------------------------------------------------------------------

dash_app.clientside_callback(
    """
    function(connector) {
        // The query and Preview are for `sql:` connectors.
        return !String(connector || '').trim().startsWith('sql:');
    }
    """,
    Output('new-pipe-sql', 'hidden'),
    Input('new-pipe-connector', 'value'),
)


@dash_app.callback(
    Output('new-pipe-preview-result', 'children'),
    Output('new-pipe-columns', 'children'),
    Output('new-pipe-datetime', 'value'),
    Input('new-pipe-preview', 'n_clicks'),
    State('new-pipe-connector', 'value'),
    State('new-pipe-query', 'value'),
    State('new-pipe-instance', 'value'),
    State('new-pipe-datetime', 'value'),
    State('session-store', 'data'),
    prevent_initial_call=True,
)
def preview_new_pipe(n_clicks, connector_keys, query, instance_keys, datetime_column, session_data):
    """Run the query (first rows only); its columns fill the Datetime and ID pickers."""
    if not n_clicks:
        raise PreventUpdate
    allowed, reason = pipes_access.can_register(session_data)
    if not allowed:
        return alert_from_success_tuple((False, reason)), dash.no_update, dash.no_update
    from meerschaum.api.dash import engine
    try:
        instance_keys = str(engine.get_instance(instance_keys, strict=True))
    except Exception as e:
        return alert_from_success_tuple((False, str(e))), dash.no_update, dash.no_update
    ### The query runs with the connector's credentials: the same checks as a SQL definition.
    refusal = new_pipe.preview_refusal(
        pipes_access.viewer(session_data), connector_keys, query, instance_keys,
    )
    if refusal:
        return (
            dbc.Alert(refusal, color='danger', id='new-pipe-preview-error', className='new-pipe-preview-error'),
            dash.no_update,
            dash.no_update,
        )
    df, message = new_pipe.preview_query(connector_keys, query, instance_keys)
    if df is None:
        return (
            dbc.Alert(message, color='danger', id='new-pipe-preview-error', className='new-pipe-preview-error'),
            dash.no_update,
            dash.no_update,
        )
    options = [html.Option(value=str(col)) for col in df.columns]
    guess = new_pipe.guess_datetime_column(df)
    return (
        new_pipe.preview_table(df),
        options,
        (guess if guess and not str(datetime_column or '').strip() else dash.no_update),
    )


@dash_app.callback(
    Output('new-pipe-alert', 'children'),
    Output('new-pipe-connector', 'invalid'),
    Output('new-pipe-metric', 'invalid'),
    Output('new-pipe-location-key', 'invalid'),
    Output('new-pipe-navigate-store', 'data'),
    Output('pipes-follow-store', 'data'),
    Input('new-pipe-register', 'n_clicks', allow_optional=True),
    Input('new-pipe-register-sync', 'n_clicks', allow_optional=True),
    State('new-pipe-connector', 'value'),
    State('new-pipe-metric', 'value'),
    State('new-pipe-location-key', 'value'),
    State('new-pipe-instance', 'value'),
    State('new-pipe-query', 'value'),
    State('new-pipe-parameters', 'value'),
    State('new-pipe-datetime', 'value'),
    State('new-pipe-id', 'value'),
    State('session-store', 'data'),
    State('new-pipe-team', 'value', allow_optional=True),
    State('new-pipe-visibility', 'value', allow_optional=True),
    prevent_initial_call=True,
)
def register_new_pipe(
    register_clicks, register_sync_clicks, connector_keys, metric_key, location_key, instance_keys,
    query, parameters_text, datetime_column, id_column, session_data, team=None, visibility=None,
):
    """
    Register the pipe (and with Register and sync, start its first sync), then open its page.
    The viewer owns it, in the chosen team (`pipe_writes.register_pipe` checks it all again).
    """
    trigger = dash.callback_context.triggered_id
    clicks = {'new-pipe-register': register_clicks, 'new-pipe-register-sync': register_sync_clicks}
    if not clicks.get(trigger):
        raise PreventUpdate
    no = dash.no_update
    allowed, reason = pipes_access.can_register(session_data)
    if not allowed:
        return alert_from_success_tuple((False, reason)), no, no, no, no, no
    keys, errors = new_pipe.validate_keys(connector_keys, metric_key, location_key)
    if keys is None:
        return (
            dbc.Alert(' '.join(errors.values()), color='danger', id='new-pipe-error'),
            'connector' in errors, 'metric' in errors, 'location' in errors, no, no,
        )
    connector_keys, metric_key, location_key = keys
    from meerschaum.api.dash import engine
    try:
        instance_keys = str(engine.get_instance(instance_keys, strict=True))
    except Exception as e:
        return alert_from_success_tuple((False, str(e))), False, False, False, no, no
    parameters, message = new_pipe.build_parameters(
        connector_keys, query, datetime_column, id_column, parameters_text,
    )
    if parameters is None:
        return alert_from_success_tuple((False, message)), False, False, False, no, no
    (success, message), pipe = new_pipe.register_pipe(
        connector_keys, metric_key, location_key, instance_keys, parameters,
        viewer=pipes_access.viewer(session_data), team=team, visibility=visibility,
    )
    if not success:
        return (
            dbc.Alert(message, color='danger', id='new-pipe-error'),
            False, ('already registered' in message), False, no, no,
        )
    pipes_table.clear_cache()
    ref = pipes_table.make_ref(instance_keys, connector_keys, metric_key, location_key)
    follow = None
    from meerschaum.api.dash import access
    if trigger == 'new-pipe-register-sync' and pipes_access.viewer(session_data).can('pipes:write'):
        sysargs, _ = pipe_detail.action_sysargs(pipe_detail.meta_of(pipe), 'sync')
        store, alert = _start_action(sysargs, 'sync', session_data, ref)
        if store is dash.no_update:
            error = getattr(alert, 'children', None) or 'The sync did not start.'
            follow = {'ref': ref, 'error': f"Registered {pipe}, but the sync did not start: {error}"}
        else:
            follow = store
    href = pipes_table.pipe_path(connector_keys, metric_key, location_key, instance_keys)
    return [], False, False, False, href, follow


# ------------------------------------------------------------------------------------------------
# Parents and children (the pipe card and the Lineage tab)
# ------------------------------------------------------------------------------------------------

@dash_app.callback(
    Output({'type': 'pipe-relations-div', 'index': MATCH}, 'children'),
    Input({'type': 'pipe-relation-add-dropdown', 'index': MATCH, 'relation': ALL}, 'value'),
    Input(
        {'type': 'pipe-relation-remove-button', 'index': MATCH, 'relation': ALL, 'other': ALL},
        'n_clicks',
    ),
    State({'type': 'pipe-relations-tabs', 'index': MATCH}, 'active_tab'),
    State('session-store', 'data'),
    State('pipe-detail-meta', 'data', allow_optional=True),
    prevent_initial_call=True,
)
def edit_pipe_relations(
    add_values: List[Optional[str]],
    remove_n_clicks: List[Optional[int]],
    active_tab: Optional[str],
    session_store_data: Optional[Dict[str, Any]],
    detail_meta: Optional[Dict[str, Any]] = None,
):
    """
    Add or remove a parent or child from a pipe card's Parents & Children tabs (or a pipe page's
    Lineage tab, whose links open in place).
    """
    triggered = dash.callback_context.triggered
    triggered_id = dash.callback_context.triggered_id
    if not triggered or not triggered_id or not any(trigger.get('value') for trigger in triggered):
        raise PreventUpdate

    from meerschaum.api.dash import access
    viewer = pipes_access.viewer(session_store_data)
    try:
        pipe = access.visible_pipe_from_meta(json.loads(triggered_id['index']), viewer=viewer)
    except (TypeError, ValueError):
        pipe = None
    if pipe is None:
        raise PreventUpdate
    relation = triggered_id['relation']
    ### Changing a pipe's parents or children edits its parameters: edit rights, no lock.
    authenticated = access.can_edit_pipe(viewer, pipe)
    refusal = access.lock_refusal(pipe, 'edit', pipe=pipe) if authenticated else None
    if not authenticated:
        feedback = (False, "You may not change this pipe.")
    elif refusal:
        feedback = (False, refusal)
    else:
        try:
            if triggered_id['type'] == 'pipe-relation-add-dropdown':
                value = next(
                    (trigger['value'] for trigger in triggered if trigger.get('value')),
                    None,
                )
                other = access.visible_pipe_from_meta(json.loads(value), viewer=viewer)
                feedback = (
                    add_pipe_relation(pipe, relation, other)
                    if other is not None else (False, "That pipe is not available.")
                )
            else:
                other = access.visible_pipe_from_meta(json.loads(triggered_id['other']), viewer=viewer)
                feedback = (
                    remove_pipe_relation(
                        pipe, relation, other,
                        edit_other=(access.can_edit_pipe(viewer, other)
                                    and not access.lock_refusal(other, 'edit', pipe=other)),
                    )
                    if other is not None else (False, "That pipe is not available.")
                )
        except Exception as e:
            feedback = (False, f"Failed to edit the {relation} of {pipe}:\n{e}")

    body = build_relations_body(
        pipe,
        authenticated=authenticated,
        active_tab=(active_tab or relation),
        feedback=feedback,
        new_tab=(not detail_meta),
    )
    return getattr(body, 'children', [body])
