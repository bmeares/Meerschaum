#! /usr/bin/env python3
# -*- coding: utf-8 -*-
# vim:fenc=utf-8

"""
Callbacks of the Jobs page: the jobs table (its rows' actions, its delete dialog and its refresh
in place), the job cards (Start, Pause, Stop, Restart, Delete, logs), the filter and the New job
form.
"""

from __future__ import annotations

import json
import time
import traceback
from datetime import datetime, timezone

import meerschaum as mrsm
from meerschaum.jobs import get_jobs
from meerschaum.utils.typing import Optional, Dict, Any, List, SuccessTuple, Tuple
from meerschaum.api import CHECK_UPDATE
from meerschaum.api.dash import dash_app
from meerschaum.api.dash.sessions import get_username_from_session
from meerschaum.utils.packages import attempt_import, import_dcc, import_html
from meerschaum.api.dash.components import alert_from_success_tuple, build_cards_grid
from meerschaum.api.dash.jobs import (
    STATUS_EMOJI,
    JOBS_FILTER_THRESHOLD,
    JOBS_REFRESH_MS,
    JOB_GROUPS,
    CONSOLE_EXECUTOR_KEYS,
    build_job_card,
    build_jobs_table,
    build_jobs_delete_modal,
    build_job_row_menu,
    build_row_status,
    build_archived_run_card,
    get_job_snapshots,
    invalidate_job_snapshots,
    is_automatic_job,
    job_visible_to_session,
    jobs_route_group_allowed,
    may_manage_jobs,
    build_manage_job_buttons_div_children,
    build_status_children,
    build_process_timestamps_children,
    build_new_job_sysargs,
    job_command_preview,
    job_action_options,
    action_selects_pipes,
    action_selects_pipeline,
    pipes_keys_options,
    validate_new_job_name,
    build_new_job_modal,
    JOB_FILTER_STATES,
    job_filter_state,
    job_group,
    job_health,
    job_last_run,
    actions_class_name,
    group_class_name,
    row_class_name,
    job_rights,
)
from meerschaum.api.dash._support import get_job as _get_job
from meerschaum.api.dash.sessions import is_session_authenticated, is_state_authenticated
dash = attempt_import('dash', lazy=False, check_update=CHECK_UPDATE)
html, dcc = import_html(check_update=CHECK_UPDATE), import_dcc(check_update=CHECK_UPDATE)
from dash.exceptions import PreventUpdate
from dash.dependencies import Input, Output, State, ALL, MATCH
import dash_bootstrap_components as dbc
from dash import no_update


@dash_app.callback(
    Output("download-logs", "data"),
    Input({'type': 'job-download-logs-button', 'index': ALL}, 'n_clicks'),
    State('session-store', 'data'),
    prevent_initial_call=True,
)
def download_job_logs(n_clicks, session_store_data):
    """
    When the download logs button is clicked, download the logs as one text file.

    It would have been more efficient to return the existing files on disk,
    but because the rotating log will keep file sizes down, this should not grow too large.
    """
    if not n_clicks:
        raise PreventUpdate

    ### Job logs may contain connector URIs and query text: `logs:read`, on a job the viewer sees.
    from meerschaum.api.dash.access import get_viewer
    if not get_viewer(session_store_data).can('logs:read'):
        raise PreventUpdate

    ctx = dash.callback_context.triggered
    if ctx[0]['value'] is None:
        raise PreventUpdate

    component_dict = json.loads(ctx[0]['prop_id'].split('.' + 'n_clicks')[0])
    job_name = component_dict['index']
    try:
        job = _get_job(job_name)
    except Exception:
        job = None
    if job is None or not job.exists() or not job_visible_to_session(job, (session_store_data or {}).get('session-id')):
        raise PreventUpdate

    now = datetime.now(timezone.utc)
    filename = job_name + '_' + str(int(now.timestamp())) + '.log'
    return {
        'content': job.get_logs(),
        'filename': filename,
    }


def restart_job(job) -> SuccessTuple:
    """Stop a running or paused job, then start it again."""
    if job.status in ('running', 'paused'):
        stopped = job.stop()
        if not stopped[0]:
            return stopped
    started = job.start()
    if not started[0]:
        return started
    return True, f"Restarted job '{job.name}'."


def delete_job(job) -> SuccessTuple:
    """Delete a job; a paused job is stopped first (deleting it would leave its process behind)."""
    if job.status == 'paused':
        stopped = job.stop()
        if not stopped[0]:
            return stopped
    return job.delete()


def _buttons_variant(class_name: Optional[str]) -> str:
    """`row` for the actions of a table row, else `card`."""
    return 'row' if 'job-row-actions' in str(class_name or '') else 'card'


def build_job_actions(job, variant: str):
    """The actions of a job: a card's buttons or a table row's menu."""
    if job is None:
        return []
    return build_job_row_menu(job) if variant == 'row' else build_manage_job_buttons_div_children(job)


@dash_app.callback(
    Output({'type': 'manage-job-alert-div', 'index': MATCH}, 'children'),
    Output({'type': 'manage-job-buttons-div', 'index': MATCH}, 'children'),
    Output({'type': 'manage-job-buttons-div', 'index': MATCH}, 'className'),
    Output({'type': 'manage-job-status-div', 'index': MATCH}, 'children'),
    Output({'type': 'process-timestamps-div', 'index': MATCH}, 'children'),
    Input({'type': 'manage-job-button', 'action': ALL, 'index': MATCH}, 'n_clicks'),
    State('session-store', 'data'),
    State({'type': 'job-label-p', 'index': MATCH}, 'children'),
    State({'type': 'manage-job-buttons-div', 'index': MATCH}, 'className'),
    prevent_initial_call=True,
)
def manage_job_button_click(
    n_clicks: Optional[List[Optional[int]]] = None,
    session_data: Optional[Dict[str, Any]] = None,
    job_label: Optional[str] = None,
    buttons_class_name: Optional[str] = None,
):
    """
    Start, pause, stop or restart the given job.
    """
    if not n_clicks or not any(n_clicks):
        raise PreventUpdate

    ctx = dash.callback_context.triggered
    if not ctx or ctx[0]['value'] is None:
        raise PreventUpdate

    session_id = (session_data or {}).get('session-id', None)
    component_dict = json.loads(ctx[0]['prop_id'].split('.' + 'n_clicks')[0])
    job_name = component_dict['index']
    manage_job_action = component_dict['action']
    from meerschaum.api.dash.access import get_viewer
    from meerschaum.api.dash.jobs import job_rights
    if not job_rights(get_viewer(session_data)).get(manage_job_action, False):
        username = get_username_from_session(session_id)
        return (
            alert_from_success_tuple((False, f"User '{username}' may not {manage_job_action} jobs.")),
            no_update,
            no_update,
            no_update,
            no_update,
        )
    try:
        job = _get_job(job_name, job_label.replace('\n', ' ') if job_label else None)
    except Exception:
        job = None
    if job is None or not job.exists() or not job_visible_to_session(job, session_id):
        raise PreventUpdate

    manage_functions = {
        'start': job.start,
        'stop': job.stop,
        'pause': job.pause,
        'restart': lambda: restart_job(job),
    }
    if manage_job_action not in manage_functions:
        return (
            alert_from_success_tuple((False, f"Invalid action '{manage_job_action}'.")),
            no_update,
            no_update,
            no_update,
            no_update,
        )

    old_status = job.status
    try:
        success, msg = manage_functions[manage_job_action]()
    except Exception:
        success, msg = False, traceback.format_exc()

    invalidate_job_snapshots()
    ### Wait for a status change before building the elements.
    timeout_seconds = 1.0
    check_interval_seconds = mrsm.get_config('system', 'cli', 'refresh_seconds')
    begin = time.perf_counter()
    while manage_job_action != 'restart' and (time.perf_counter() - begin) < timeout_seconds:
        if job.status != old_status:
            break
        time.sleep(check_interval_seconds)

    variant = _buttons_variant(buttons_class_name)
    return (
        alert_from_success_tuple((success, msg)),
        build_job_actions(job, variant),
        actions_class_name(job, variant),
        build_status_children(job),
        build_process_timestamps_children(job),
    )


@dash_app.callback(
    Output({'type': 'job-delete-confirm-button', 'index': MATCH}, 'disabled'),
    Input({'type': 'job-delete-name-input', 'index': MATCH}, 'value'),
    State({'type': 'job-delete-name-input', 'index': MATCH}, 'id'),
    prevent_initial_call=True,
)
def enable_job_delete_confirm(typed_name: Optional[str], input_id: Dict[str, str]):
    """Delete is enabled once the job's name has been typed."""
    return (typed_name or '').strip() != input_id.get('index')


@dash_app.callback(
    Output({'type': 'job-delete-modal', 'index': MATCH}, 'is_open'),
    Output({'type': 'job-delete-alert-div', 'index': MATCH}, 'children'),
    Output({'type': 'job-delete-name-input', 'index': MATCH}, 'value'),
    Input({'type': 'job-delete-button', 'index': MATCH}, 'n_clicks'),
    Input({'type': 'job-delete-cancel-button', 'index': MATCH}, 'n_clicks'),
    Input({'type': 'job-delete-confirm-button', 'index': MATCH}, 'n_clicks'),
    State({'type': 'job-delete-name-input', 'index': MATCH}, 'value'),
    State('session-store', 'data'),
    State({'type': 'job-label-p', 'index': MATCH}, 'children'),
    prevent_initial_call=True,
)
def job_delete_modal(
    open_clicks: Optional[int] = None,
    cancel_clicks: Optional[int] = None,
    confirm_clicks: Optional[int] = None,
    typed_name: Optional[str] = None,
    session_data: Optional[Dict[str, Any]] = None,
    job_label: Optional[str] = None,
):
    """
    Open the delete-job confirmation modal, cancel it, or delete the job once its name was typed.
    """
    ctx = dash.callback_context.triggered
    if not ctx or ctx[0]['value'] is None:
        raise PreventUpdate

    prop_id = ctx[0]['prop_id']
    if 'job-delete-button' in prop_id:
        return True, [], ''
    if 'job-delete-cancel-button' in prop_id:
        return False, [], ''

    ### Confirm button: actually delete the job (`jobs:delete`).
    session_id = (session_data or {}).get('session-id', None)
    from meerschaum.api.dash.access import get_viewer
    from meerschaum.api.dash.jobs import job_rights
    if not job_rights(get_viewer(session_data)).get('delete', False):
        username = get_username_from_session(session_id)
        return True, alert_from_success_tuple(
            (False, f"User '{username}' may not delete jobs.")
        ), no_update

    component_dict = json.loads(prop_id.split('.' + 'n_clicks')[0])
    job_name = component_dict['index']
    if (typed_name or '').strip() != job_name:
        return True, alert_from_success_tuple((False, "Type the job's name to delete it.")), no_update
    try:
        job = _get_job(job_name, job_label.replace('\n', ' ') if job_label else None)
    except Exception:
        job = None
    if job is None or not job.exists() or not job_visible_to_session(job, session_id):
        return False, [], ''

    try:
        success, msg = delete_job(job)
    except Exception:
        success, msg = False, traceback.format_exc()
    invalidate_job_snapshots()

    if not success:
        return True, alert_from_success_tuple((success, msg)), no_update

    return False, [], ''


### "Follow logs" is handled by `meerschaum.api.dash.callbacks.execute` (the output pane).


@dash_app.callback(
    Output({'type': 'manage-job-buttons-div', 'index': ALL}, 'children'),
    Output({'type': 'manage-job-buttons-div', 'index': ALL}, 'className'),
    Output({'type': 'manage-job-status-div', 'index': ALL}, 'children'),
    Output({'type': 'process-timestamps-div', 'index': ALL}, 'children'),
    Input('refresh-jobs-interval', 'n_intervals'),
    State('session-store', 'data'),
    State({'type': 'manage-job-buttons-div', 'index': ALL}, 'className'),
    prevent_initial_call=True,
)
def refresh_jobs_on_interval(
    n_intervals: Optional[int] = None,
    session_data: Optional[Dict[str, Any]] = None,
    class_names: Optional[List[Optional[str]]] = None,
):
    """
    When the jobs refresh interval fires, refresh the job cards' status lines, and rebuild a
    job's actions and timestamps only when its status changed (a button being replaced loses a
    click). Every card is read from one listing (`get_job_snapshots`).
    """
    session_id = (session_data or {}).get('session-id', None)
    may_manage = _signed_in(session_id)

    job_names = [
        component_dict['id']['index']
        for component_dict in dash.callback_context.outputs_grouping[0]
    ]
    if not job_names:
        raise PreventUpdate
    class_names = list(class_names or [None] * len(job_names))

    try:
        snapshots = get_job_snapshots()
    except Exception:
        snapshots = {}

    ### NOTE: The job may have been deleted, but the card may still exist.
    jobs = []
    for name in job_names:
        try:
            ### A job the listing does not have (another executor's) is read on its own.
            job = snapshots.get(name) or _get_job(name)
            job = job if job_visible_to_session(job, session_id) else None
        except Exception:
            job = None
        jobs.append(job)

    buttons, buttons_classes, statuses, timestamps = [], [], [], []
    for job, class_name in zip(jobs, class_names):
        variant = _buttons_variant(class_name)
        new_class_name = actions_class_name(job, variant)
        changed = new_class_name.split() != str(class_name or '').split()
        statuses.append(build_status_children(job))
        timestamps.append(build_process_timestamps_children(job))
        if not changed:
            buttons.append(no_update)
            buttons_classes.append(no_update)
            continue
        buttons.append(build_job_actions(job, variant) if may_manage else [])
        buttons_classes.append(new_class_name)

    return buttons, buttons_classes, statuses, timestamps


#: The tabs of the Jobs page (`/dash/runs` opens `runs`).
JOBS_PAGE_TABS = ('jobs', 'runs')


@dash_app.callback(
    Output('job-output-div', 'children'),
    Input('job-location', 'pathname'),
    State('session-store', 'data'),
    State('job-location', 'search'),
)
def render_job_page_from_url(
    pathname: str,
    session_data: Optional[Dict[str, Any]],
    search: Optional[str] = None,
):
    """
    Load the Jobs page (`/dash/jobs`, or `/dash/runs` for its Runs tab; `?status=stalled` shows
    the jobs in one state, as the Overview's Jobs tile links) or one job's card
    (`/dash/jobs/{name}`).
    """
    is_runs = str(pathname).startswith('/dash/runs')
    if not (str(pathname).startswith('/dash/jobs') or is_runs):
        return no_update

    session_id = (session_data or {}).get('session-id', None)
    authenticated = _signed_in(session_id)

    job_name = pathname.replace('/dash/runs' if is_runs else '/dash/jobs', '').lstrip('/').rstrip('/')
    if not job_name:
        from urllib.parse import parse_qs
        status = parse_qs(str(search or '').lstrip('?')).get('status', [None])[0]
        status = status if status in JOB_FILTER_STATES else None
        return [
            html.Div(
                [
                    dbc.Tabs(
                        [dbc.Tab(label='Jobs', tab_id='jobs'), dbc.Tab(label='Runs', tab_id='runs')],
                        id='jobs-page-tabs',
                        active_tab=('runs' if is_runs else 'jobs'),
                    ),
                    html.Div(
                        dbc.Button('New job', id='jobs-new-button', color='primary'),
                        id='jobs-new-button-div',
                        hidden=(is_runs or not job_rights().get('create', False)),
                        className='jobs-toolbar-actions',
                    ),
                ],
                className='jobs-toolbar',
            ),
            html.Div(
                [
                    dbc.Input(
                        id='jobs-filter-input',
                        type='search',
                        placeholder='Filter jobs',
                        debounce=True,
                        autoComplete='off',
                    ),
                    dbc.Button(
                        [JOB_FILTER_STATES.get(status, ''), ' ', html.Span('✕', **{'aria-hidden': 'true'})],
                        id='jobs-status-filter-clear',
                        color='secondary',
                        outline=True,
                        size='sm',
                        title='Show every job',
                        style=({} if status else {'display': 'none'}),
                    ),
                ],
                id='jobs-filter-div',
                hidden=not status,
                className='jobs-filter',
            ),
            dcc.Store(id='jobs-status-filter', data=status),
            html.Div(id='jobs-alert-div'),
            html.Div(id='jobs-page-tab-content'),
            html.Br(),
            build_new_job_modal(),
            build_jobs_delete_modal(),
            ### The refresh (`refresh_jobs_table`): the interval ticks, `gate_jobs_refresh`
            ### forwards a tick unless the viewer is using the table (`jobs-refresh-tick`), and a
            ### job that appeared rebuilds the table (`jobs-structure-changed`, then
            ### `jobs-list-refresh`, when the viewer is not using it either).
            dcc.Interval(id='jobs-list-interval', interval=JOBS_REFRESH_MS, n_intervals=0),
            dcc.Store(id='jobs-refresh-tick', data=None),
            dcc.Store(id='jobs-action-done', data=None),
            dcc.Store(id='jobs-created', data=None),
            dcc.Store(id='jobs-structure-changed', data=None),
            dcc.Store(id='jobs-list-refresh', data=0),
            dcc.Store(id='jobs-rendered-names', data=None),
        ]

    job = _get_job(job_name)
    if not job.exists() or not job_visible_to_session(job, session_id):
        return [
            html.Br(),
            html.H2("404: Job does not exist."),
        ]

    return [
        html.Br(),
        build_job_card(job, authenticated=authenticated, include_follow=False),
        html.Br(),
    ]


def _viewer_of(session_id: Optional[str]):
    from meerschaum.api.dash.access import get_viewer
    return get_viewer(session_id=(str(session_id) if session_id else None))


def _signed_in(session_id: Optional[str]) -> bool:
    """Whether the session is a signed-in viewer (or `--no-auth`)."""
    viewer = _viewer_of(session_id)
    return bool(viewer.no_auth or viewer.signed_in)


def _listed_jobs(is_runs: bool, session_id: Optional[str]) -> Dict[str, Any]:
    """The jobs of a tab the session may see, by name (from one listing: `get_job_snapshots`)."""
    jobs = get_job_snapshots()
    return {
        name: job
        for name, job in sorted(jobs.items())
        if is_automatic_job(job) == is_runs and job_visible_to_session(job, session_id)
    }


def _matches(job, filter_text: Optional[str], status: Optional[str] = None) -> bool:
    """
    Whether a job's name or command contains every word of the filter (and the job is in the
    `status` state, when one is given).
    """
    if status and job_filter_state(job) != status:
        return False
    words = str(filter_text or '').lower().split()
    if not words:
        return True
    try:
        haystack = (job.name + ' ' + ' '.join(job.sysargs)).lower()
    except Exception:
        haystack = str(getattr(job, 'name', '')).lower()
    return all(word in haystack for word in words)


@dash_app.callback(
    Output('jobs-page-tab-content', 'children'),
    Output('jobs-rendered-names', 'data'),
    Output('jobs-filter-div', 'hidden'),
    Output('jobs-new-button-div', 'hidden'),
    Output('refresh-jobs-interval', 'disabled'),
    Input('jobs-page-tabs', 'active_tab'),
    Input('jobs-filter-input', 'value'),
    Input('jobs-list-refresh', 'data'),
    Input('jobs-status-filter', 'data'),
    State('session-store', 'data'),
)
def render_jobs_page_tab(
    active_tab: Optional[str],
    filter_text: Optional[str],
    refresh: Any,
    status: Optional[str],
    session_data: Optional[Dict[str, Any]],
):
    """
    Build the active tab of the Jobs page: the jobs table (Jobs), or the runs' cards (Runs, which
    the cards' own refresh, `refresh-jobs-interval`, keeps current).
    """
    if active_tab not in JOBS_PAGE_TABS:
        raise PreventUpdate
    session_id = (session_data or {}).get('session-id', None)
    is_runs = active_tab == 'runs'
    status = status if status in JOB_FILTER_STATES and not is_runs else None
    content, names, total = build_jobs_tab(
        is_runs, session_id, filter_text=filter_text, status=status, with_total=True,
    )
    return (
        content,
        {'tab': active_tab, 'names': names, 'filter': filter_text or '', 'status': status},
        total <= JOBS_FILTER_THRESHOLD and not filter_text and not status,
        is_runs or not job_rights(_viewer_of(session_id)).get('create', False),
        not is_runs,
    )


@dash_app.callback(
    Output('jobs-status-filter', 'data'),
    Output('jobs-status-filter-clear', 'style'),
    Input('jobs-status-filter-clear', 'n_clicks'),
    prevent_initial_call=True,
)
def clear_jobs_status_filter(n_clicks: Optional[int]):
    """Show every job again (the state filter of an Overview link goes away)."""
    if not n_clicks:
        raise PreventUpdate
    return None, {'display': 'none'}


def build_jobs_tab(
    is_runs: bool,
    session_id: Optional[str],
    filter_text: Optional[str] = None,
    with_total: bool = False,
    status: Optional[str] = None,
):
    """
    The jobs of a tab: persistent jobs (the jobs table), or the console's runs (cards of the
    automatic execution workers and of the summaries retained after their output expired).
    With `with_total`, return `(content, names, total)`: the names shown and how many jobs the
    tab has before filtering.
    """
    ### Each control follows the viewer's job permissions (`job_rights`).
    authenticated = _signed_in(session_id)
    rights = job_rights(_viewer_of(session_id))
    listed = _listed_jobs(is_runs, session_id)
    total = len(listed)
    shown = [job for job in listed.values() if _matches(job, filter_text, status)]
    names = [job.name for job in shown]

    archived = []
    if is_runs and _viewer_of(session_id).can('jobs:read'):
        from meerschaum.api.dash.execute import execution_request
        try:
            records = execution_request('/execute/runs', get_username_from_session(session_id)) or []
            archived = [build_archived_run_card(record) for record in records]
        except Exception as exc:
            archived = [alert_from_success_tuple((False, f'Unable to load retained summaries: {exc}'))]

    if not shown and not archived:
        if filter_text:
            content = html.P(f"No jobs match '{filter_text}'.", className='text-muted jobs-empty')
        elif status:
            content = html.Div(html.H5(f"No {JOB_FILTER_STATES[status].lower()} jobs"), className='jobs-empty')
        elif is_runs:
            content = html.Div(html.H5('No runs'), className='jobs-empty')
        else:
            content = html.Div(
                [html.H5('No jobs')] + (
                    [dbc.Button('New job', id='jobs-empty-new-button', color='primary')]
                    if rights.get('create') else []
                ),
                className='jobs-empty',
            )
    elif is_runs:
        cards = [
            build_job_card(job, authenticated=authenticated, include_follow=False)
            for job in shown
        ] + archived
        content = build_cards_grid(cards, 3)
    else:
        content = build_jobs_table(shown, authenticated=authenticated)
    if with_total:
        return content, names, total
    return content


### Forward a refresh tick unless the viewer is using the table: a menu of it is open, or text
### in it is selected (a refresh would close the menu or lose the selection), or the page is not
### visible. The next tick tries again. Without a table (no jobs yet, or none matching the
### filter) the tick goes on too: a job that appears rebuilds the tab.
JOBS_TABLE_BUSY_JS = """
    function jobsTableBusy(table){
        if (!table) return false;
        if (table.querySelector('.dropdown-menu.show')) return true;
        const selection = window.getSelection ? window.getSelection() : null;
        return Boolean(selection && !selection.isCollapsed && (
            table.contains(selection.anchorNode) || table.contains(selection.focusNode)
        ));
    }
"""

dash_app.clientside_callback(
    "function(n){" + JOBS_TABLE_BUSY_JS + """
        if (document.hidden || jobsTableBusy(document.getElementById('jobs-table'))) {
            return dash_clientside.no_update;
        }
        return n;
    }""",
    Output('jobs-refresh-tick', 'data'),
    Input('jobs-list-interval', 'n_intervals'),
    prevent_initial_call=True,
)

### A job appeared (or came back into the filter): rebuild the table, unless the viewer is
### using it (then the next refresh asks again).
dash_app.clientside_callback(
    "function(changed, refresh){" + JOBS_TABLE_BUSY_JS + """
        if (!changed) return dash_clientside.no_update;
        if (jobsTableBusy(document.getElementById('jobs-table'))) return dash_clientside.no_update;
        return (refresh || 0) + 1;
    }""",
    Output('jobs-list-refresh', 'data'),
    Input('jobs-structure-changed', 'data'),
    State('jobs-list-refresh', 'data'),
    prevent_initial_call=True,
)


def table_refresh_outputs(
    row_names: List[str],
    row_classes: List[Optional[str]],
    status_classes: List[Optional[str]],
    last_runs: List[Any],
    last_run_titles: List[Any],
    actions_classes: List[Optional[str]],
    group_names: List[str],
    group_classes: List[Optional[str]],
    group_counts: List[Any],
    rendered: Dict[str, Any],
    session_id: Optional[str],
    snapshots: Optional[Dict[str, Any]] = None,
    now: Optional[datetime] = None,
) -> Tuple[Any, ...]:
    """
    The outputs of `refresh_jobs_table` from the table's current state: for each row its group
    class (a job that went away, or out of the filter, is hidden), Status, Last Run and (when the
    job's status changed) menu, `no_update` where nothing changed; the groups' counts and
    classes; and `jobs-structure-changed` when a job that is not in the table appeared.
    """
    snapshots = get_job_snapshots() if snapshots is None else snapshots
    filter_text, status = rendered.get('filter'), rendered.get('status')
    shown = {
        name: job
        for name, job in snapshots.items()
        if not is_automatic_job(job)
        and job_visible_to_session(job, session_id)
        and _matches(job, filter_text, status)
    }
    may_manage = _signed_in(session_id)
    rights = job_rights(_viewer_of(session_id)) if may_manage else {}

    def changed(new, old):
        return no_update if new == old else new

    out_rows, out_status, out_status_class, out_last, out_title, out_menu, out_menu_class = (
        [], [], [], [], [], [], []
    )
    counts = {group: 0 for group, _ in JOB_GROUPS}
    for index, name in enumerate(row_names):
        job = shown.get(name)
        if job is None:
            ### Gone (deleted, or out of the filter): hidden where it is, until it comes back.
            current = str(row_classes[index] or 'job-row')
            gone = current if 'job-row-gone' in current.split() else current + ' job-row-gone'
            out_rows.append(changed(gone, row_classes[index]))
            for out in (out_status, out_status_class, out_last, out_title, out_menu, out_menu_class):
                out.append(no_update)
            continue
        counts[job_group(job)] += 1
        out_rows.append(changed(row_class_name(job), row_classes[index]))
        health = job_health(job)
        word, status_class = build_row_status(job, health)
        new_status_class = changed(status_class, status_classes[index])
        out_status_class.append(new_status_class)
        out_status.append(no_update if new_status_class is no_update else word)
        text, title = job_last_run(job, health, now=now)
        out_last.append(changed(text, last_runs[index]))
        out_title.append(changed(title, last_run_titles[index]))
        menu_class = actions_class_name(job, 'row')
        if menu_class.split() == str(actions_classes[index] or '').split():
            out_menu.append(no_update)
            out_menu_class.append(no_update)
        else:
            out_menu.append(build_job_row_menu(job, rights=rights) if may_manage else [])
            out_menu_class.append(menu_class)

    out_group_classes = [
        changed(group_class_name(group, counts.get(group, 0)), group_classes[index])
        for index, group in enumerate(group_names)
    ]
    out_group_counts = [
        changed(str(counts.get(group, 0)), group_counts[index])
        for index, group in enumerate(group_names)
    ]
    appeared = sorted(set(shown) - set(row_names))
    structure = {'appeared': appeared, 'at': time.time()} if appeared else no_update
    return (
        out_rows, out_status, out_status_class, out_last, out_title, out_menu, out_menu_class,
        out_group_classes, out_group_counts, structure,
    )


@dash_app.callback(
    Output({'type': 'job-row', 'index': ALL}, 'className'),
    Output({'type': 'job-row-status', 'index': ALL}, 'children'),
    Output({'type': 'job-row-status', 'index': ALL}, 'className'),
    Output({'type': 'job-row-last-run', 'index': ALL}, 'children'),
    Output({'type': 'job-row-last-run', 'index': ALL}, 'title'),
    Output({'type': 'job-row-actions', 'index': ALL}, 'children'),
    Output({'type': 'job-row-actions', 'index': ALL}, 'className'),
    Output({'type': 'jobs-group', 'index': ALL}, 'className'),
    Output({'type': 'jobs-group-count', 'index': ALL}, 'children'),
    Output('jobs-structure-changed', 'data'),
    Input('jobs-refresh-tick', 'data'),
    Input('jobs-action-done', 'data'),
    Input('jobs-created', 'data'),
    State({'type': 'job-row', 'index': ALL}, 'className'),
    State({'type': 'job-row-status', 'index': ALL}, 'className'),
    State({'type': 'job-row-last-run', 'index': ALL}, 'children'),
    State({'type': 'job-row-last-run', 'index': ALL}, 'title'),
    State({'type': 'job-row-actions', 'index': ALL}, 'className'),
    State({'type': 'jobs-group', 'index': ALL}, 'className'),
    State({'type': 'jobs-group-count', 'index': ALL}, 'children'),
    State('jobs-rendered-names', 'data'),
    State('session-store', 'data'),
    prevent_initial_call=True,
)
def refresh_jobs_table(
    tick, action_done, created,
    row_classes, status_classes, last_runs, last_run_titles, actions_classes,
    group_classes, group_counts, rendered, session_data,
):
    """
    Refresh the jobs table in place (every `JOBS_REFRESH_MS`, and after an action): each row's
    group, Status and Last Run, its menu when its status changed, the groups' counts. Nothing is
    rebuilt, so the page keeps its scroll position, open menus, filter and selection; a job that
    is not in the table yet rebuilds it (through `jobs-structure-changed`).
    """
    if not rendered or rendered.get('tab') != 'jobs':
        raise PreventUpdate
    outputs = dash.callback_context.outputs_list
    row_names = [item['id']['index'] for item in outputs[0]]
    group_names = [item['id']['index'] for item in outputs[7]]
    session_id = (session_data or {}).get('session-id', None)
    try:
        snapshots = get_job_snapshots()
    except Exception:
        raise PreventUpdate
    return table_refresh_outputs(
        row_names, row_classes, status_classes, last_runs, last_run_titles, actions_classes,
        group_names, group_classes, group_counts, rendered, session_id, snapshots=snapshots,
    )


@dash_app.callback(
    Output('jobs-alert-div', 'children', allow_duplicate=True),
    Output('jobs-action-done', 'data'),
    Output('jobs-delete-modal', 'is_open'),
    Output('jobs-delete-target', 'data'),
    Output('jobs-delete-title', 'children'),
    Output('jobs-delete-label', 'children'),
    Output('jobs-delete-name-input', 'value'),
    Output('jobs-delete-name-input', 'placeholder'),
    Output('jobs-delete-alert-div', 'children'),
    Input({'type': 'job-row-action', 'action': ALL, 'index': ALL}, 'n_clicks'),
    Input('jobs-delete-cancel-button', 'n_clicks'),
    Input('jobs-delete-confirm-button', 'n_clicks'),
    State('jobs-delete-target', 'data'),
    State('jobs-delete-name-input', 'value'),
    State('session-store', 'data'),
    prevent_initial_call=True,
)
def jobs_table_action(row_clicks, cancel_clicks, confirm_clicks, target, typed_name, session_data):
    """
    A jobs table row's action: Start, Resume, Pause, Stop and Restart run at once (the row
    then refreshes in place); Delete opens the page's delete dialog for the job, which deletes
    it once its name is typed.
    """
    ctx = dash.callback_context
    trigger = ctx.triggered_id
    value = ctx.triggered[0]['value'] if ctx.triggered else None
    session_id = (session_data or {}).get('session-id', None)
    rights = job_rights(_viewer_of(session_id))
    username = get_username_from_session(session_id)

    def outputs(alert=no_update, done=no_update, is_open=no_update, target_=no_update,
                title=no_update, label=no_update, typed=no_update, placeholder=no_update,
                modal_alert=no_update):
        return alert, done, is_open, target_, title, label, typed, placeholder, modal_alert

    if trigger == 'jobs-delete-cancel-button':
        if not cancel_clicks:
            raise PreventUpdate
        return outputs(is_open=False, target_=None, typed='', modal_alert=[])

    if trigger == 'jobs-delete-confirm-button':
        if not confirm_clicks or not target:
            raise PreventUpdate
        if not rights.get('delete', False):
            return outputs(modal_alert=alert_from_success_tuple(
                (False, f"User '{username}' may not delete jobs.")
            ))
        if (typed_name or '').strip() != target:
            return outputs(modal_alert=alert_from_success_tuple((False, "Type the job's name to delete it.")))
        try:
            job = _get_job(target)
        except Exception:
            job = None
        if job is None or not job.exists() or not job_visible_to_session(job, session_id):
            invalidate_job_snapshots()
            return outputs(done=time.time(), is_open=False, target_=None, typed='', modal_alert=[])
        try:
            success, msg = delete_job(job)
        except Exception:
            success, msg = False, traceback.format_exc()
        invalidate_job_snapshots()
        if not success:
            return outputs(modal_alert=alert_from_success_tuple((success, msg)))
        return outputs(
            alert=alert_from_success_tuple((True, f"Deleted job '{target}'.")),
            done=time.time(), is_open=False, target_=None, typed='', modal_alert=[],
        )

    if not isinstance(trigger, dict) or not value:
        raise PreventUpdate
    job_name, action = trigger.get('index'), trigger.get('action')
    if action == 'delete':
        if not rights.get('delete', False):
            raise PreventUpdate
        return outputs(
            is_open=True,
            target_=job_name,
            title=['Delete job ', html.B(job_name), '?'],
            label=['Type ', html.Code(job_name), ' to confirm'],
            typed='',
            placeholder=job_name,
            modal_alert=[],
        )
    if not rights.get(action, False):
        return outputs(alert=alert_from_success_tuple((False, f"User '{username}' may not {action} jobs.")))
    try:
        job = _get_job(job_name)
    except Exception:
        job = None
    if job is None or not job.exists() or not job_visible_to_session(job, session_id):
        raise PreventUpdate
    functions = {
        'start': job.start,
        'stop': job.stop,
        'pause': job.pause,
        'restart': lambda: restart_job(job),
    }
    if action not in functions:
        return outputs(alert=alert_from_success_tuple((False, f"Invalid action '{action}'.")))
    old_status = job.status
    try:
        success, msg = functions[action]()
    except Exception:
        success, msg = False, traceback.format_exc()
    ### Wait (briefly) for the status to change, so the row moves with this refresh.
    begin = time.perf_counter()
    interval = mrsm.get_config('system', 'cli', 'refresh_seconds')
    while success and action != 'restart' and (time.perf_counter() - begin) < 1.0:
        if job.status != old_status:
            break
        time.sleep(interval)
    invalidate_job_snapshots()
    return outputs(alert=alert_from_success_tuple((success, msg)), done=time.time())


@dash_app.callback(
    Output('jobs-delete-confirm-button', 'disabled'),
    Input('jobs-delete-name-input', 'value'),
    State('jobs-delete-target', 'data'),
)
def enable_jobs_delete_confirm(typed_name: Optional[str], target: Optional[str]):
    """The table's Delete is enabled once the job's name has been typed."""
    return not target or (typed_name or '').strip() != target


########################################################################################
# New job.
########################################################################################

def _pipes_keys_options(instance) -> Tuple[Dict[str, List[str]], Optional[str]]:
    """
    The connectors, metrics, locations and tags of the instance's pipes (internal ones aside),
    from one `fetch_pipes_keys()`, and why they could not be read (`None` when they could).
    """
    from meerschaum.api.dash import access
    try:
        ### Only the pipes the viewer may see name their keys and tags here.
        viewer = access.current_viewer()
        visible = [
            (ck, mk, lk, parameters)
            for ck, mk, lk, parameters in access.registered_pipes(instance)
            if access.can_see_pipe(viewer, parameters)
        ]
        options, _ = pipes_keys_options(visible)
    except Exception as e:
        empty = {'connector_keys': [], 'metric_keys': [], 'location_keys': [], 'tags': []}
        return empty, f"Failed to read the pipes of '{instance}': {e}"
    return options, None


def _pipeline_options(instance) -> List[str]:
    """The saved Pipelines of the instance (its registry) the viewer may see."""
    try:
        from meerschaum.api.dash.access import visible_pipelines
        return sorted(visible_pipelines(instance))
    except Exception:
        return []


def _new_job_instance():
    """The instance the New job form selects pipes on (the console's global instance)."""
    from meerschaum.api.dash.engine import get_instance
    return get_instance()


@dash_app.callback(
    Output('new-job-modal', 'is_open'),
    Output('new-job-action', 'options'),
    Output('new-job-action', 'value'),
    Output('new-job-name', 'value'),
    Output('new-job-alert-div', 'children'),
    Output('new-job-connector-keys', 'options'),
    Output('new-job-metric-keys', 'options'),
    Output('new-job-location-keys', 'options'),
    Output('new-job-tags', 'options'),
    Output('new-job-pipeline', 'options'),
    Output('new-job-connector-keys', 'value'),
    Output('new-job-metric-keys', 'value'),
    Output('new-job-location-keys', 'value'),
    Output('new-job-tags', 'value'),
    Output('new-job-pipeline', 'value'),
    Output('new-job-arguments', 'value'),
    Output('new-job-repeat', 'value'),
    Output('new-job-schedule', 'value'),
    Output('new-job-min-seconds', 'value'),
    Output('new-job-restart', 'value'),
    Input('jobs-new-button', 'n_clicks'),
    Input('jobs-empty-new-button', 'n_clicks', allow_optional=True),
    Input('new-job-cancel-button', 'n_clicks'),
    State('session-store', 'data'),
    prevent_initial_call=True,
)
def toggle_new_job_modal(new_clicks, empty_new_clicks, cancel_clicks, session_data):
    """Open the New job form (filled with this instance's pipes and Pipelines), or close it."""
    trigger = dash.callback_context.triggered_id
    if trigger == 'new-job-cancel-button':
        if not cancel_clicks:
            raise PreventUpdate
        return (False,) + (no_update,) * 19
    if not (new_clicks or empty_new_clicks):
        raise PreventUpdate
    session_id = (session_data or {}).get('session-id', None)
    if not job_rights(_viewer_of(session_id)).get('create', False):
        raise PreventUpdate

    from meerschaum.utils.daemon import get_new_daemon_name
    instance = _new_job_instance()
    keys, keys_problem = _pipes_keys_options(instance)
    try:
        actions = job_action_options()
    except Exception:
        actions = [{'label': 'sync pipes', 'value': 'sync pipes'}]
    return (
        True,
        actions,
        'sync pipes',
        get_new_daemon_name(),
        alert_from_success_tuple((False, keys_problem)) if keys_problem else [],
        keys['connector_keys'],
        keys['metric_keys'],
        keys['location_keys'],
        keys['tags'],
        _pipeline_options(instance),
        [], [], [], [], None, '', 'schedule', '', None, False,
    )


NEW_JOB_FIELDS = (
    State('new-job-action', 'value'),
    State('new-job-connector-keys', 'value'),
    State('new-job-metric-keys', 'value'),
    State('new-job-location-keys', 'value'),
    State('new-job-tags', 'value'),
    State('new-job-pipeline', 'value'),
    State('new-job-arguments', 'value'),
    State('new-job-repeat', 'value'),
    State('new-job-schedule', 'value'),
    State('new-job-min-seconds', 'value'),
    State('new-job-restart', 'value'),
    State('new-job-name', 'value'),
)


def _form_sysargs(action, connector_keys, metric_keys, location_keys, tags, pipeline, arguments,
                  repeat, schedule, min_seconds, restart, instance=None):
    instance = instance if instance is not None else _new_job_instance()
    return build_new_job_sysargs(
        action=action,
        connector_keys=connector_keys,
        metric_keys=metric_keys,
        location_keys=[(None if key == 'None' else key) for key in (location_keys or [])],
        tags=tags,
        pipeline=pipeline,
        arguments=arguments,
        repeat=repeat,
        schedule=schedule,
        min_seconds=min_seconds,
        restart=bool(restart),
        instance=str(instance) if instance is not None else None,
    )


@dash_app.callback(
    Output('new-job-preview', 'children'),
    Output('new-job-preview', 'className'),
    Output('new-job-create-button', 'disabled'),
    Output('new-job-pipes-div', 'hidden'),
    Output('new-job-pipeline-div', 'hidden'),
    Output('new-job-schedule-div', 'hidden'),
    Output('new-job-loop-div', 'hidden'),
    Output('new-job-restart-div', 'hidden'),
    Input('new-job-action', 'value'),
    Input('new-job-connector-keys', 'value'),
    Input('new-job-metric-keys', 'value'),
    Input('new-job-location-keys', 'value'),
    Input('new-job-tags', 'value'),
    Input('new-job-pipeline', 'value'),
    Input('new-job-arguments', 'value'),
    Input('new-job-repeat', 'value'),
    Input('new-job-schedule', 'value'),
    Input('new-job-min-seconds', 'value'),
    Input('new-job-restart', 'value'),
    Input('new-job-name', 'value'),
    State('session-store', 'data'),
)
def preview_new_job(action, connector_keys, metric_keys, location_keys, tags, pipeline, arguments,
                    repeat, schedule, min_seconds, restart, name, session_data):
    """Show the command the form describes (or what is missing) and the fields it needs."""
    if not job_rights(_viewer_of((session_data or {}).get('session-id'))).get('create', False):
        raise PreventUpdate
    sysargs, message = _form_sysargs(
        action, connector_keys, metric_keys, location_keys, tags, pipeline, arguments,
        repeat, schedule, min_seconds, restart,
    )
    valid_name, name_message = validate_new_job_name(name)
    if sysargs is not None and not valid_name:
        message = name_message
    ready = sysargs is not None and valid_name
    return (
        job_command_preview(sysargs, name) if ready else message,
        'new-job-preview' + ('' if ready else ' new-job-preview-invalid'),
        not ready,
        not action_selects_pipes(action),
        not action_selects_pipeline(action),
        repeat != 'schedule',
        repeat != 'loop',
        repeat != 'once',
    )


def _policy_allows(sysargs: List[str], username: Optional[str]) -> SuccessTuple:
    """
    Ask the server whether this user may run `sysargs` (`POST /execute/validate`): what a
    non-administrator may run from the console, the same rule as the console's Exec.
    """
    from meerschaum.api.dash import execute as _execute
    if not _execute.server_url():
        return False, "The console has no server to check this command with."
    try:
        allowed = _execute.execution_request('/execute/validate', username, method='POST',
                                             body={'sysargs': sysargs})
    except Exception as exc:
        return False, f"The server refused this command: {exc}"
    if isinstance(allowed, dict):
        return bool(allowed.get('success')), str(allowed.get('message', allowed.get('msg', '')))
    if isinstance(allowed, (list, tuple)) and len(allowed) == 2:
        return bool(allowed[0]), str(allowed[1])
    return False, "The server cannot authorize this command."


def create_job_from_form(
    sysargs: List[str],
    name: str,
    session_id: Optional[str],
) -> SuccessTuple:
    """
    Create and start a job from the New job form with the console's executor: a session that may
    run actions (administrators only under `--secure`; a non-administrator only what the server's
    console policy allows) on a server serving the `jobs` routes.
    """
    from meerschaum.jobs import Job
    from meerschaum.api import no_auth
    ### New job runs an action as a job: `jobs:write` and `actions:execute`.
    creator = _viewer_of(session_id)
    if not job_rights(creator).get('create', False):
        return False, "You are not allowed to create jobs."
    if not jobs_route_group_allowed():
        return False, "This server does not manage jobs (the 'jobs' route group is disabled)."
    existing = list(get_jobs(executor_keys='local', include_hidden=True, combine_local_and_systemd=True))
    valid, message = validate_new_job_name(name, existing=existing)
    if not valid:
        return False, message
    if not no_auth and not creator.is_admin:
        allowed, message = _policy_allows(sysargs, creator.username)
        if not allowed:
            return False, message or "You may not run this command as a job."
    try:
        ### The job is the session user's, in their primary team (AC9).
        from meerschaum.jobs import ownership_properties
        from meerschaum.api.dash.access import get_viewer
        viewer = get_viewer(session_id=session_id)
        job = Job(
            name.strip(),
            sysargs,
            executor_keys=CONSOLE_EXECUTOR_KEYS,
            _properties=ownership_properties(
                None if viewer.no_auth else viewer.username,
                None if viewer.no_auth else viewer.primary_team,
            ),
        )
    except Exception as e:
        return False, str(e)
    if job.exists():
        return False, f"A job named '{name}' already exists."
    try:
        success, msg = job.start()
    except Exception as e:
        return False, f"Failed to start job '{name}': {e}"
    if not success:
        return False, msg
    return True, f"Created job '{job.name}'."


@dash_app.callback(
    Output('new-job-modal', 'is_open', allow_duplicate=True),
    Output('new-job-alert-div', 'children', allow_duplicate=True),
    Output('jobs-alert-div', 'children'),
    Output('jobs-created', 'data'),
    Input('new-job-create-button', 'n_clicks'),
    *NEW_JOB_FIELDS,
    State('session-store', 'data'),
    prevent_initial_call=True,
)
def create_new_job(create_clicks, action, connector_keys, metric_keys, location_keys, tags,
                   pipeline, arguments, repeat, schedule, min_seconds, restart, name,
                   session_data):
    """Create and start the job the New job form describes (the table then lists it)."""
    if not create_clicks:
        raise PreventUpdate
    session_id = (session_data or {}).get('session-id', None)
    sysargs, message = _form_sysargs(
        action, connector_keys, metric_keys, location_keys, tags, pipeline, arguments,
        repeat, schedule, min_seconds, restart,
    )
    if sysargs is None:
        return no_update, alert_from_success_tuple((False, message)), no_update, no_update
    success, message = create_job_from_form(sysargs, str(name or ''), session_id)
    invalidate_job_snapshots()
    if not success:
        return no_update, alert_from_success_tuple((False, message)), no_update, no_update
    return False, [], alert_from_success_tuple((True, message)), time.time()


def _read_job_logs(job_name: str, session_id: Optional[str]) -> Optional[Dict[str, Any]]:
    """The logs and status of a job the session may read, or `None`."""
    try:
        job = _get_job(job_name)
    except Exception:
        return None
    if job is None or not job.exists() or not job_visible_to_session(job, session_id):
        return None
    text = job.get_logs() or ''
    return {'job': job_name, 'text': text, 'status': job.status, 'digest': _digest(text)}


def _digest(text: str) -> str:
    """Tell whether the logs changed (a rotated log may keep its length)."""
    import hashlib
    return hashlib.sha1(text.encode('utf-8', 'replace')).hexdigest()


@dash_app.callback(
    Output('job-logs-modal', 'is_open'),
    Output('job-logs-modal-title', 'children'),
    Output('job-logs-meta', 'data'),
    Output('job-logs-text', 'data'),
    Output('job-logs-interval', 'disabled'),
    Input({'type': 'job-view-logs-button', 'index': ALL}, 'n_clicks'),
    Input('job-logs-interval', 'n_intervals'),
    Input('job-logs-modal', 'is_open'),
    State('job-logs-meta', 'data'),
    State('session-store', 'data'),
    prevent_initial_call=True,
)
def view_job_logs(view_clicks, n_intervals, is_open, meta, session_store_data):
    """
    "Logs" > "View": open the logs modal on the job's logs, and re-read them while the job runs
    (only what changed is sent; the browser keeps its scroll position unless it follows the end).
    """
    ctx = dash.callback_context
    return job_logs_outputs(
        ctx.triggered_id,
        ctx.triggered[0]['value'] if ctx.triggered else None,
        is_open,
        meta,
        session_store_data,
    )


def job_logs_outputs(trigger_id, triggered_value, is_open, meta, session_store_data):
    """
    The outputs of `view_job_logs`: the modal's state, title, the meta and text stores, and
    whether the refresh interval is disabled.
    """
    if trigger_id == 'job-logs-modal':
        if is_open:
            raise PreventUpdate
        return no_update, no_update, None, None, True

    ### Job logs may contain connector URIs and query text: `logs:read`, on a job the viewer sees.
    from meerschaum.api.dash.access import get_viewer
    if not get_viewer(session_store_data).can('logs:read'):
        raise PreventUpdate
    session_id = (session_store_data or {}).get('session-id')

    if isinstance(trigger_id, dict):
        ### A card appeared (no click).
        if not triggered_value:
            raise PreventUpdate
        logs = _read_job_logs(trigger_id['index'], session_id)
        if logs is None:
            raise PreventUpdate
        status = logs['status']
        return (
            True,
            build_logs_modal_title(logs['job'], status),
            {'job': logs['job'], 'status': status, 'digest': logs['digest']},
            {'job': logs['job'], 'text': logs['text'], 'opened': time.time()},
            status not in ('running', 'paused'),
        )

    if not is_open or not meta:
        return no_update, no_update, no_update, no_update, True
    logs = _read_job_logs(meta['job'], session_id)
    if logs is None:
        return no_update, no_update, no_update, no_update, True
    status, digest = logs['status'], logs['digest']
    if status == meta.get('status') and digest == meta.get('digest'):
        raise PreventUpdate
    return (
        no_update,
        build_logs_modal_title(logs['job'], status) if status != meta.get('status') else no_update,
        {'job': logs['job'], 'status': status, 'digest': digest},
        {'job': logs['job'], 'text': logs['text']} if digest != meta.get('digest') else no_update,
        status not in ('running', 'paused'),
    )


def build_logs_modal_title(job_name: str, status: str):
    """The logs modal's title: the job's status emoji and name."""
    emoji = STATUS_EMOJI.get(status, STATUS_EMOJI['stopped'])
    return [html.Span(emoji, title=str(status).capitalize()), ' ', html.B(job_name)]


### Render the logs with their ANSI colors through the Pipelines page's output renderer
### (`assets/pipeline_output.js`): it opens at the end and follows new lines only while the
### view is scrolled to the bottom, so scrolling up to read history is never interrupted.
dash_app.clientside_callback(
    """
    function(data){
        if (!data) return dash_clientside.no_update;
        function render(attempt){
            const view = document.getElementById('job-logs-view');
            // The modal mounts its body as it opens.
            if (!view){
                if (attempt < 40) setTimeout(() => render(attempt + 1), 25);
                return;
            }
            if (data.opened && view._mrsmOpened !== data.opened){
                view._mrsmOpened = data.opened;
                view._mrsmText = null;
                view._mrsmFollow = true;
            }
            if (window.mrsmRenderPipelineOutput) window.mrsmRenderPipelineOutput(view, data.text);
            else { view.textContent = data.text; view.scrollTop = view.scrollHeight; }
        }
        render(0);
        return dash_clientside.no_update;
    }
    """,
    Output('job-logs-dummy', 'children'),
    Input('job-logs-text', 'data'),
)


@dash_app.callback(
    Output({'type': 'manage-job-alert-div', 'index': MATCH}, 'children', allow_duplicate=True),
    Input({'type': 'retain-run-button', 'index': MATCH}, 'n_clicks'),
    State('session-store', 'data'), prevent_initial_call=True,
)
def retain_automatic_run(clicks, session):
    if not clicks or not job_rights(_viewer_of((session or {}).get('session-id'))).get('retain', False):
        raise PreventUpdate
    from meerschaum.api.dash.execute import execution_request
    from urllib.parse import quote
    name = dash.callback_context.triggered_id['index']
    try:
        result = execution_request('/execute/runs/' + quote(name, safe='') + '/retain',
            get_username_from_session((session or {}).get('session-id')), method='POST')
        if isinstance(result, dict):
            result = (bool(result.get('success')), str(result.get('message', result.get('msg', ''))))
        invalidate_job_snapshots()
        if result and result[0]:
            return [alert_from_success_tuple((True, 'Kept as a persistent Job.')),
                    html.A('Open Jobs', href='/dash/jobs')]
        return alert_from_success_tuple(tuple(result))
    except Exception as exc:
        return alert_from_success_tuple((False, str(exc)))
