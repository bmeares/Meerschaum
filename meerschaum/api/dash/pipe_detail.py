#! /usr/bin/env python3
# -*- coding: utf-8 -*-
# vim:fenc=utf-8

"""
A pipe's page (`/dash/pipes/<connector>/<metric>[/<location>]?instance=...`): the header
`Pipe(...)` with the instance badge and Sync, Verify, Edit and Manage (Clear, Drop, Delete,
Copy), then the tabs Overview, Data, Parameters, Definition, Lineage and Runs.

The tabs reuse the pipe card's section builders (`meerschaum.api.dash.pipes`), so their editors,
queries and downloads keep the card's callbacks. Actions run on the server (`POST /execute`) and
their output streams into the page (`assets/pipes.js`).
"""

from __future__ import annotations

import json
from urllib.parse import quote

import meerschaum as mrsm
from meerschaum.utils.typing import Any, Dict, List, Optional, Tuple
from meerschaum.utils.packages import attempt_import, import_html, import_dcc
from meerschaum.api import CHECK_UPDATE
from meerschaum.api.dash import pipes_table

html, dcc = import_html(check_update=CHECK_UPDATE), import_dcc(check_update=CHECK_UPDATE)
import dash_bootstrap_components as dbc
dash_ace = attempt_import('dash_ace', lazy=False, check_update=CHECK_UPDATE)

#: The tabs, in order: `(tab_id, label)`.
TABS: Tuple[Tuple[str, str], ...] = (
    ('overview', 'Overview'),
    ('data', 'Data'),
    ('parameters', 'Parameters'),
    ('definition', 'Definition'),
    ('lineage', 'Lineage'),
    ('runs', 'Runs'),
)
TAB_IDS = tuple(tab_id for tab_id, _ in TABS)

#: The tab Edit opens when the current one has nothing to edit.
EDIT_TAB: str = 'parameters'
EDITABLE_TABS = frozenset({'data', 'parameters', 'definition', 'lineage'})

#: The Manage menu's destructive actions: `action -> (title, button, consequence)`.
DESTRUCTIVE_ACTIONS: Dict[str, Tuple[str, str, str]] = {
    'clear': ('Clear rows', 'Clear', 'Deletes the rows of its table (all, or those between Begin and End).'),
    'drop': ('Drop table', 'Drop', "Drops its table. The pipe stays registered."),
    'delete': ('Delete pipe', 'Delete', "Deletes the pipe's registration and drops its table."),
}

#: How many runs the Runs tab lists.
RUNS_LIMIT: int = 20

#: What ran a pipe's last sync (C2 `source`), in words.
SYNC_SOURCES: Dict[str, str] = {
    'cli': 'from the command line',
    'job': 'in a job',
    'engine': 'in a Pipeline run',
    'api': 'through the API',
    'python': 'from Python',
}


def pipe_ref(pipe: mrsm.Pipe) -> str:
    return pipes_table.make_ref(pipe.instance_keys, pipe.connector_keys, pipe.metric_key, pipe.location_key)


def meta_of(pipe: mrsm.Pipe) -> Dict[str, Any]:
    """The JSON the page's stores carry for a pipe."""
    return {
        'connector_keys': str(pipe.connector_keys),
        'metric_key': str(pipe.metric_key),
        'location_key': (None if pipe.location_key is None else str(pipe.location_key)),
        'instance_keys': str(pipe.instance_keys),
    }


def pipe_from_meta(meta: Dict[str, Any]) -> mrsm.Pipe:
    return mrsm.Pipe(
        meta['connector_keys'], meta['metric_key'], meta.get('location_key'),
        instance=meta['instance_keys'],
    )


def read_error(pipe: mrsm.Pipe, error: Any) -> str:
    """`Could not read <instance>: <error>`, with the instance connector's secrets masked."""
    try:
        instance = pipe.instance_connector
    except Exception:
        instance = None
    return f"Could not read {pipe.instance_keys}: {pipes_table.error_text(error, instance)}"


def registration(pipe: mrsm.Pipe) -> Tuple[Optional[bool], str]:
    """
    `(True, '')` when the pipe is registered, `(False, '')` when it is not, and `(None, why)` when
    its instance could not be read (a database outage is not an unregistered pipe).
    """
    try:
        return pipe.get_id() is not None, ''
    except Exception as e:
        return None, read_error(pipe, e)


def register_href(pipe: mrsm.Pipe) -> str:
    """New pipe (`/dash/pipes/new`) with this pipe's keys and instance filled in."""
    from urllib.parse import urlencode
    query = {
        'connector_keys': str(pipe.connector_keys),
        'metric_key': str(pipe.metric_key),
        'instance': str(pipe.instance_keys),
    }
    if pipe.location_key is not None:
        query['location_key'] = str(pipe.location_key)
    return '/dash/pipes/new?' + urlencode(query)


# ------------------------------------------------------------------------------------------------
# Layout
# ------------------------------------------------------------------------------------------------

def pipe_rights(viewer: Any, pipe: mrsm.Pipe, parameters: Optional[Dict[str, Any]] = None) -> Dict[str, bool]:
    """
    What the viewer may do with this pipe on its page: `sync`, `verify`, `edit`, `copy`,
    `clear`, `drop`, `delete`, `access`, `locks` (permissions and edit rights; locks are not
    rights: a locked action shows and says why it cannot run).
    """
    from meerschaum.api.dash import access
    if parameters is None:
        try:
            parameters = pipe.get_parameters(apply_symlinks=False) or {}
        except Exception:
            parameters = {}
    rights = {}
    for action in ('sync', 'verify', 'edit', 'clear', 'drop', 'delete'):
        permission = access.PIPE_ACTION_PERMISSIONS[action]
        rights[action] = access.can_edit_pipe(viewer, parameters, permission)
    from meerschaum.config import get_config
    rights['copy'] = bool(
        viewer.can('pipes:register')
        and get_config('api', 'permissions', 'registration', 'pipes', warn=False)
        and access.can_see_pipe(viewer, parameters)
    )
    rights['access'] = access.can_manage_access(viewer, parameters)
    rights['locks'] = access.can_lock(viewer, parameters)
    return rights


def build_badges(parameters: Optional[Dict[str, Any]]) -> List[Any]:
    """
    The pipe's owner, team and visibility badges and a lock icon, for a pipe that has them (a
    pipe without an `access` key is Everyone's, visible to its team: no badges).
    """
    from meerschaum.api.dash import access
    parameters = parameters or {}
    try:
        fields = access.access_fields(parameters)
        locks = access.pipe_locks(parameters)
    except Exception:
        return []
    badges = []
    if fields.get('owner'):
        badges.append(dbc.Badge(
            [html.Span('Owner ', className='pipes-badge-key'), fields['owner']],
            color='secondary', className='pipes-access-badge', id='pipe-detail-owner',
        ))
    if fields.get('team'):
        badges.append(dbc.Badge(
            [html.Span('Team ', className='pipes-badge-key'), fields['team']],
            color='secondary', className='pipes-access-badge', id='pipe-detail-team',
        ))
    visibility = fields.get('visibility') or 'team'
    if visibility != 'team':
        badges.append(dbc.Badge(
            dict(access.VISIBILITIES).get(visibility, visibility),
            color=('warning' if visibility == 'private' else 'info'),
            text_color=('dark' if visibility == 'private' else None),
            className='pipes-access-badge pipes-visibility-badge', id='pipe-detail-visibility',
        ))
    if locks:
        title = pipes_table.locks_title(locks)
        badges.append(html.Span(
            [
                html.Span(className='mrsm-icon mrsm-icon-lock', **{'aria-hidden': 'true'}),
                html.Span(title, className='visually-hidden'),
            ],
            className='pipes-lock pipes-lock-badge', title=title, id='pipe-detail-locks',
        ))
    return badges


def build_header(
    pipe: mrsm.Pipe,
    global_instance: str,
    allowed: Any,
    parameters: Optional[Dict[str, Any]] = None,
) -> Any:
    """
    `Pipe(...)` with its instance badge, its access badges and lock icon, and the actions the
    viewer may run (`allowed`: `pipe_rights()`, or a bool for every action or none). An action
    the viewer lacks the permission for is not shown.
    """
    if isinstance(allowed, dict):
        rights = dict(allowed)
    else:
        rights = {key: bool(allowed) for key in (
            'sync', 'verify', 'edit', 'copy', 'clear', 'drop', 'delete', 'access', 'locks',
        )}
    identity = [
        html.H3(
            pipes_table.pipe_title(pipe.connector_keys, pipe.metric_key, pipe.location_key),
            id='pipe-detail-title',
        ),
    ]
    if str(pipe.instance_keys) != str(global_instance):
        identity.append(dbc.Badge(str(pipe.instance_keys), color='secondary',
                                  className='pipes-instance-badge', id='pipe-detail-instance'))
    identity.append(html.Span(build_badges(parameters), id='pipe-detail-badges',
                              className='pipes-access-badges'))
    actions = []
    if rights.get('sync'):
        actions.append(dbc.Button('Sync', id='pipe-action-sync', n_clicks=0, size='sm', color='primary'))
    if rights.get('verify'):
        actions.append(dbc.Button('Verify', id='pipe-action-verify', n_clicks=0, size='sm', color='secondary'))
    if rights.get('edit'):
        actions.append(dbc.Button('Edit', id='pipe-edit-toggle', n_clicks=0, size='sm', color='secondary'))
    items = []
    if rights.get('copy'):
        items.append(dbc.DropdownMenuItem('Copy', id='pipe-manage-copy', n_clicks=0))
    if rights.get('access'):
        items.append(dbc.DropdownMenuItem('Access…', id='pipe-manage-access', n_clicks=0))
    if rights.get('locks'):
        items.append(dbc.DropdownMenuItem('Locks…', id='pipe-manage-locks', n_clicks=0))
    destructive = [
        dbc.DropdownMenuItem(label, id=f'pipe-manage-{action}', n_clicks=0)
        for action, label in (('clear', 'Clear'), ('drop', 'Drop'), ('delete', 'Delete'))
        if rights.get(action)
    ]
    if items and destructive:
        items.append(dbc.DropdownMenuItem(divider=True))
    items += destructive
    if items:
        actions.append(dbc.DropdownMenu(
            items,
            label='Manage',
            size='sm',
            color='secondary',
            menu_variant='dark',
            align_end=True,
            id='pipe-manage-menu',
        ))
    return html.Div(
        [
            dcc.Link('Pipes', href='/dash/pipes', className='pipes-breadcrumb'),
            html.Div(
                [
                    html.Div(identity, className='pipes-identity'),
                    ### Hidden while the pipe is not known to be registered (see `build_tab`).
                    html.Div(actions, className='pipes-header-actions', id='pipe-detail-actions'),
                ],
                className='pipes-detail-header',
            ),
        ],
    )


def build_output_panel() -> Any:
    """Where an action's output streams (hidden until one runs)."""
    return html.Section(
        [
            html.Div(
                [
                    html.Code(id='pipe-action-command', className='pipes-output-command'),
                    html.Span(id='pipe-action-status', className='pipes-output-status'),
                    html.Button('Close', id='pipe-action-close', className='btn btn-sm btn-secondary',
                                n_clicks=0, type='button'),
                ],
                className='pipes-output-header',
            ),
            html.Pre(id='pipe-action-output', className='pipes-output'),
        ],
        id='pipe-action-panel',
        className='pipes-output-panel',
        hidden=True,
    )


def build_dialogs(pipe: mrsm.Pipe) -> Any:
    """The destructive confirmation and the Copy dialog."""
    from meerschaum.api.dash.new_pipe import instance_options
    confirm = dbc.Modal(
        [
            dbc.ModalHeader(dbc.ModalTitle(id='pipe-confirm-title')),
            dbc.ModalBody([
                html.P(pipes_table.pipe_title(pipe.connector_keys, pipe.metric_key, pipe.location_key),
                       className='pipes-confirm-pipe'),
                html.P(id='pipe-confirm-consequence'),
                html.Div(
                    dbc.Row([
                        dbc.Col([dbc.Label('Begin', html_for='pipe-confirm-begin', size='sm'),
                                 dbc.Input(id='pipe-confirm-begin', size='sm', placeholder='2026-01-01')], xs=6),
                        dbc.Col([dbc.Label('End', html_for='pipe-confirm-end', size='sm'),
                                 dbc.Input(id='pipe-confirm-end', size='sm', placeholder='2026-02-01')], xs=6),
                    ], className='g-2'),
                    id='pipe-confirm-bounds',
                    hidden=True,
                ),
                dbc.Label(
                    ['Type ', html.Code(str(pipe.metric_key)), ' to confirm'],
                    html_for='pipe-confirm-input',
                    className='pipes-confirm-label',
                ),
                dbc.Input(id='pipe-confirm-input', autocomplete='off', placeholder=str(pipe.metric_key)),
            ]),
            dbc.ModalFooter([
                dbc.Button('Cancel', id='pipe-confirm-cancel', color='secondary', n_clicks=0),
                dbc.Button('Confirm', id='pipe-confirm-button', color='danger', n_clicks=0, disabled=True),
            ]),
        ],
        id='pipe-confirm-modal',
        is_open=False,
        className='pipes-dialog',
    )
    copy = dbc.Modal(
        [
            dbc.ModalHeader(dbc.ModalTitle('Copy pipe')),
            dbc.ModalBody([
                dbc.Label('Connector', html_for='pipe-copy-connector', size='sm'),
                dbc.Input(id='pipe-copy-connector', value=str(pipe.connector_keys), size='sm'),
                dbc.Label('Metric', html_for='pipe-copy-metric', size='sm'),
                dbc.Input(id='pipe-copy-metric', value=str(pipe.metric_key), size='sm'),
                dbc.Label('Location', html_for='pipe-copy-location', size='sm'),
                dbc.Input(id='pipe-copy-location', value=(pipe.location_key or ''), size='sm',
                          placeholder='None'),
                dbc.Label('Instance', html_for='pipe-copy-instance', size='sm'),
                dbc.Select(
                    id='pipe-copy-instance',
                    options=instance_options(str(pipe.instance_keys)),
                    value=str(pipe.instance_keys),
                    size='sm',
                    class_name='dbc_dark custom-select custom-select-sm',
                ),
                dbc.Checkbox(id='pipe-copy-rows', label='Copy rows', value=True,
                             className='pipes-copy-rows'),
                html.Div(id='pipe-copy-alert'),
            ]),
            dbc.ModalFooter([
                dbc.Button('Cancel', id='pipe-copy-cancel', color='secondary', n_clicks=0),
                dbc.Button('Copy', id='pipe-copy-button', color='primary', n_clicks=0),
            ]),
        ],
        id='pipe-copy-modal',
        is_open=False,
        className='pipes-dialog',
    )
    from meerschaum.api.dash import access as _access
    access_dialog = dbc.Modal(
        [
            dbc.ModalHeader(dbc.ModalTitle('Access')),
            dbc.ModalBody([
                html.P(pipes_table.pipe_title(pipe.connector_keys, pipe.metric_key, pipe.location_key),
                       className='pipes-confirm-pipe'),
                dbc.Label('Owner', html_for='pipe-access-owner', size='sm'),
                dbc.Select(id='pipe-access-owner', options=[], value=None, size='sm',
                           class_name='dbc_dark custom-select custom-select-sm'),
                dbc.Label('Team', html_for='pipe-access-team', size='sm'),
                dbc.Select(id='pipe-access-team', options=[], value=_access.EVERYONE, size='sm',
                           class_name='dbc_dark custom-select custom-select-sm'),
                dbc.Label('Who can see it', size='sm'),
                dbc.RadioItems(
                    id='pipe-access-visibility',
                    options=[
                        {'label': 'Its team', 'value': 'team'},
                        {'label': 'Its owner and team managers', 'value': 'private'},
                        {'label': 'Everyone', 'value': 'everyone'},
                    ],
                    value='team',
                    class_name='pipes-access-radios',
                ),
                dbc.Label('Who can change it', size='sm'),
                dbc.RadioItems(
                    id='pipe-access-edit',
                    options=[{'label': label, 'value': value} for value, label in _access.EDIT_MODES],
                    value='team',
                    class_name='pipes-access-radios',
                ),
                html.Div(id='pipe-access-alert'),
            ]),
            dbc.ModalFooter([
                dbc.Button('Cancel', id='pipe-access-cancel', color='secondary', n_clicks=0),
                dbc.Button('Save', id='pipe-access-save', color='primary', n_clicks=0),
            ]),
        ],
        id='pipe-access-modal',
        is_open=False,
        className='pipes-dialog',
    )
    locks_dialog = dbc.Modal(
        [
            dbc.ModalHeader(dbc.ModalTitle('Locks')),
            dbc.ModalBody([
                html.P(pipes_table.pipe_title(pipe.connector_keys, pipe.metric_key, pipe.location_key),
                       className='pipes-confirm-pipe'),
                dbc.Checklist(
                    id='pipe-locks-value',
                    options=[
                        {'label': label, 'value': lock}
                        for lock, label in _access.LOCKS
                    ],
                    value=[],
                    switch=True,
                    class_name='pipes-locks-switches',
                ),
                html.Div(id='pipe-locks-alert'),
            ]),
            dbc.ModalFooter([
                dbc.Button('Cancel', id='pipe-locks-cancel', color='secondary', n_clicks=0),
                dbc.Button('Save', id='pipe-locks-save', color='primary', n_clicks=0),
            ]),
        ],
        id='pipe-locks-modal',
        is_open=False,
        className='pipes-dialog',
    )
    return html.Div([confirm, copy, access_dialog, locks_dialog])


def layout(
    connector_keys: str,
    metric_key: str,
    location_key: Optional[str],
    instance_keys: str,
    global_instance: str,
    allowed: Any = False,
    tab: Optional[str] = None,
    problem: Optional[str] = None,
    viewer: Any = None,
) -> List[Any]:
    """
    The page's skeleton: the header and tabs; the active tab fills in by callback. With a
    `viewer`, the actions are the ones they may run on this pipe (`pipe_rights`), and a pipe they
    may not see shows no actions (its tabs say it is not registered).
    """
    pipe = mrsm.Pipe(connector_keys, metric_key, location_key, instance=instance_keys)
    parameters = None
    if viewer is not None:
        from meerschaum.api.dash import access
        visible = access.visible_pipe(connector_keys, metric_key, location_key, instance_keys, viewer=viewer)
        if visible is None:
            allowed = False
        else:
            try:
                parameters = visible.get_parameters(apply_symlinks=False) or {}
            except Exception:
                parameters = {}
            allowed = pipe_rights(viewer, visible, parameters)
    tab = tab if tab in TAB_IDS else 'overview'
    tabs = dbc.Tabs(
        [
            dbc.Tab(label=label, tab_id=tab_id, id=f'pipe-detail-tab-{tab_id}')
            for tab_id, label in TABS
        ],
        id='pipe-detail-tabs',
        active_tab=tab,
        className='pipes-detail-tabs',
    )
    alerts = [dbc.Alert(problem, color='danger', id='pipes-instance-alert')] if problem else []
    return [
        dcc.Store(id='pipe-detail-meta', data=meta_of(pipe)),
        dcc.Store(id='pipe-detail-edit', data=False),
        dcc.Store(id='pipe-detail-refresh', data=0),
        dcc.Store(id='pipe-action-store', data=None),
        dcc.Store(id='pipe-copy-store', data=None),
        dcc.Store(id='pipe-confirm-store', data=None),
        dcc.Store(id='pipes-follow-store', storage_type='session'),
        dcc.Store(id='pipes-navigate-store', data=None),
        dcc.Download(id='download-dataframe-csv'),
        html.Div(id='pipe-detail-dummy', hidden=True),
        html.Main(
            alerts + [
                build_header(pipe, global_instance, allowed, parameters=parameters),
                html.Div(id='pipe-detail-alert'),
                tabs,
                dcc.Loading(
                    [
                        html.Div(id='pipe-detail-content', className='pipes-detail-content'),
                        ### The active tab as built for its tab and mode (shown if still current).
                        dcc.Store(id='pipe-detail-result', data=None),
                    ],
                    type='dot',
                    delay_show=400,
                    target_components={'pipe-detail-result': 'data'},
                ),
                build_output_panel(),
                build_dialogs(pipe),
            ],
            id='pipe-detail-page',
            className='pipes-page pipes-detail-page',
            **{
                'data-ref': pipe_ref(pipe),
                'data-edit': 'false',
                'data-tab': tab,
            },
        ),
    ]


# ------------------------------------------------------------------------------------------------
# Tabs
# ------------------------------------------------------------------------------------------------

def _section(title: Optional[str], *children: Any, actions: Any = None, class_name: str = '') -> Any:
    head = []
    if title:
        head = [html.Div(
            [html.H5(title)] + ([html.Div(actions, className='pipes-section-actions')] if actions else []),
            className='pipes-section-header',
        )]
    return html.Section(head + list(children), className=('pipes-section ' + class_name).strip())


def _fact(label: str, value: Any, class_name: str = '') -> Any:
    return html.Div(
        [html.Dt(label), html.Dd(value)],
        className=('pipes-fact ' + class_name).strip(),
    )


def _tile(label: str, value: Any, extra: Any = None, title: str = '', class_name: str = '') -> Any:
    return html.Div(
        [
            html.Div(label, className='pipes-tile-label'),
            html.Div(value, className='pipes-tile-value', title=title),
        ] + ([html.Div(extra, className='pipes-tile-extra')] if extra is not None else []),
        className=('pipes-tile ' + class_name).strip(),
    )


def _status_tile(status: Optional[Dict[str, Any]], has_data: bool) -> Any:
    """The last sync: when, how it went, the rows it wrote and where it ran."""
    if not status:
        if has_data:
            return _tile('Last sync', '—', title='No sync recorded yet.')
        return _tile('Last sync', 'Never synced', class_name='pipes-tile-never')
    success = status.get('success') is not False and not int(status.get('consecutive_failures') or 0)
    message = str(status.get('message') or '').strip()
    if success:
        when = pipes_table.time_ago(status.get('last_success') or status.get('last_attempt'))
        value = 'Synced ' + when if when else 'Synced'
    else:
        failures = int(status.get('consecutive_failures') or 1)
        when = pipes_table.time_ago(status.get('last_failure') or status.get('last_attempt'))
        value = ('Failed' if failures <= 1 else f'Failed {failures} times') + (f' · {when}' if when else '')
    counts = []
    for key, noun in (('rows_inserted', 'inserted'), ('rows_updated', 'updated'), ('rows_upserted', 'upserted')):
        try:
            if status.get(key):
                counts.append(f"{int(status[key]):,} {noun}")
        except (TypeError, ValueError):
            continue
    if status.get('duration_seconds') is not None:
        try:
            seconds = float(status['duration_seconds'])
            counts.append('<0.1 s' if seconds < 0.1 else f"{seconds:.1f} s")
        except (TypeError, ValueError):
            pass
    if status.get('job'):
        counts.append(f"job {status['job']}")
    elif status.get('source'):
        counts.append(SYNC_SOURCES.get(str(status['source']), str(status['source'])))
    extra = []
    if counts:
        extra.append(html.Div(' · '.join(counts)))
    ### A failure's message says why; a success's counts say what its message would.
    if message and (not success or not counts):
        extra.append(html.Pre(message[:2000], className='pipes-status-message'))
    return _tile(
        'Last sync', value, extra=(extra or None),
        title=str(status.get('last_attempt') or ''),
        class_name=('pipes-tile-ok' if success else 'pipes-tile-failing'),
    )


def columns_table(pipe: mrsm.Pipe) -> Any:
    """Each column's role, dtype (declared or inferred) and database type."""
    try:
        db_types = pipe.get_columns_types() or {}
    except Exception as e:
        db_types, error = {}, str(e)
    else:
        error = ''
    try:
        dtypes = pipe.get_dtypes(infer=True) or {}
    except Exception:
        dtypes = {}
    try:
        declared = pipe.get_dtypes(infer=False) or {}
    except Exception:
        declared = {}
    roles: Dict[str, List[str]] = {}
    for role, column in (pipe.columns or {}).items():
        if column:
            roles.setdefault(str(column), []).append(str(role))
    names = []
    for name in list(db_types) + list(dtypes) + list(roles):
        if name not in names:
            names.append(name)
    if not names:
        return html.P(error or 'No columns yet.')
    rows = []
    for name in names:
        dtype = dtypes.get(name, '')
        rows.append(html.Tr([
            html.Td(html.Code(name)),
            html.Td(', '.join(roles.get(name, []))),
            html.Td([
                html.Code(str(dtype)) if dtype else '',
                (dbc.Badge('declared', color='info', className='pipes-declared-badge')
                 if name in declared else ''),
            ]),
            html.Td(html.Code(str(db_types[name]).replace('_', ' ')) if name in db_types else '—'),
        ]))
    return html.Div(
        html.Table(
            [
                html.Thead(html.Tr([html.Th('Column'), html.Th('Role'), html.Th('Dtype'), html.Th('DB type')])),
                html.Tbody(rows),
            ],
            className='table table-sm pipes-columns-table',
        ),
        className='pipes-table-scroll',
    )


def overview_tab(pipe: mrsm.Pipe, allowed: bool = False) -> Any:
    """Rows, sync time, the last sync, the target, tags, jobs, columns and indices."""
    from meerschaum.api.dash.pipes import build_indices_table, build_rowcount_button, get_target_label
    ref = pipe_ref(pipe)
    try:
        exists = pipe.exists()
    except Exception:
        exists = False
    try:
        rows, estimated = pipes_table.get_rowcount(pipe) if exists else (0, False)
        rows_value = pipes_table.rows_text({'rows': rows, 'estimated': estimated})
        rows_error = ''
    except Exception as e:
        rows, rows_value, rows_error = None, '—', str(e)
    try:
        newest = pipe.get_sync_time(newest=True) if exists else None
        oldest = pipe.get_sync_time(newest=False) if exists else None
    except Exception:
        newest, oldest = None, None
    newest_text = pipes_table.sync_time_text(pipes_table._sync_time_fields(newest))
    oldest_text = pipes_table.sync_time_text(pipes_table._sync_time_fields(oldest))
    status = pipes_table.get_sync_status(pipe)
    try:
        jobs = pipes_table.get_jobs_index(str(pipe.instance_keys), [pipes_table._row_from_pipe(pipe)]).get(ref, [])
    except Exception:
        jobs = []

    tiles = html.Div(
        [
            _tile(
                'Rows', rows_value,
                extra=(build_rowcount_button(pipe, label='Count') if allowed and exists else None),
                title=rows_error,
                class_name='pipes-tile-rows',
            ),
            _tile(
                'Sync time', newest_text,
                extra=(html.Span(['Oldest ', oldest_text]) if oldest is not None else None),
                title=str(newest or ''),
            ),
            _status_tile(status, has_data=bool(rows) or newest is not None),
        ],
        className='pipes-tiles',
    )
    facts = [
        _fact('Target', html.Code(get_target_label(pipe))),
        _fact('Instance', html.Code(str(pipe.instance_keys))),
    ]
    tags = pipe.tags or []
    if tags:
        facts.append(_fact('Tags', [html.Span(str(tag), className='pipes-tag') for tag in tags]))
    if jobs:
        facts.append(_fact('Jobs', pipes_table.job_badges(jobs)))
    declared_precision = (pipe.get_parameters(apply_symlinks=False) or {}).get('precision', None)
    precision = pipe.precision if declared_precision else None
    if precision:
        facts.append(_fact('Precision', f"{precision.get('interval', 1)} {precision.get('unit', '')}".strip()))
    sections = [
        tiles,
        html.Dl(facts, className='pipes-facts'),
        _section('Columns', columns_table(pipe)),
    ]
    indices = build_indices_table(pipe)
    if indices is not None:
        sections.append(_section('Indices', html.Div(indices, className='pipes-table-scroll')))
    return html.Div(sections)


def data_tab(
    pipe: mrsm.Pipe,
    allowed: bool = False,
    edit: bool = False,
    can_sync: Optional[bool] = None,
) -> Any:
    """
    Recent rows (with Download CSV), a query by begin, end and `params`, and (in Edit mode, for
    a viewer who may write to the pipe: `can_sync`) Sync rows.
    """
    from meerschaum.api.dash.pipes import (
        build_recent_data_body, build_query_data_body, build_sync_documents_body,
    )
    can_sync = allowed if can_sync is None else can_sync
    meta_str = json.dumps(pipe.meta)
    download = dbc.Button(
        'Download CSV', id={'type': 'pipe-download-csv-button', 'index': meta_str},
        size='sm', color='secondary', n_clicks=0,
    ) if allowed else None
    sections = [_section('Recent rows', build_recent_data_body(pipe), actions=download)]
    if allowed:
        sections.append(_section('Query', build_query_data_body(pipe)))
    if can_sync and edit:
        sections.append(_section('Sync rows', build_sync_documents_body(pipe, authenticated=True)))
    return html.Div(sections)


def parameters_tab(pipe: mrsm.Pipe, allowed: bool = False, edit: bool = False) -> Any:
    """The parameters editor; Update only in Edit mode."""
    from meerschaum.api.dash.pipes import build_parameters_body
    editing = allowed and edit
    return build_parameters_body(
        pipe,
        authenticated=allowed,
        read_only=not editing,
        include_relatives=False,
        show_resolve=not editing,
    )


def fetch_text(pipe: mrsm.Pipe) -> str:
    """A non-SQL pipe's `fetch` parameters as YAML."""
    from meerschaum.utils.yaml import yaml
    parameters = pipe.get_parameters(apply_symlinks=False, refresh=True) or {}
    fetch = parameters.get('fetch', None) or {}
    return yaml.dump(fetch) if fetch else '{}\n'


def definition_tab(pipe: mrsm.Pipe, allowed: bool = False, edit: bool = False) -> Any:
    """The SQL definition of a `sql:` pipe, else its `fetch` parameters; Update only in Edit mode."""
    from meerschaum.api.dash.pipes import build_sql_body
    editing = allowed and edit
    if str(pipe.connector_keys).startswith('sql:'):
        ### As written (its `{{ Pipe(...) }}` references kept); Update saves exactly this.
        return build_sql_body(pipe, read_only=not editing, resolve_symlinks=False,
                              show_resolve=not editing)
    editor = dash_ace.DashAceEditor(
        value=fetch_text(pipe),
        mode='norm',
        tabSize=4,
        theme='twilight',
        id='pipe-fetch-editor',
        width='100%',
        height='320px',
        readOnly=not editing,
        showGutter=True,
        showPrintMargin=False,
        highlightActiveLine=True,
        wrapEnabled=True,
        style={'min-height': '120px'},
    )
    return html.Div([
        html.Div([html.Span('fetch', className='pipes-editor-label'), html.Code(str(pipe.connector_keys))],
                 className='pipes-editor-heading'),
        editor,
        html.Div(
            [dbc.Button('Update', id='pipe-fetch-update', n_clicks=0, size='sm')] if editing else [],
            className='pipes-editor-actions',
        ),
        html.Div(id='pipe-fetch-alert'),
    ])


def lineage_tab(pipe: mrsm.Pipe, allowed: bool = False, edit: bool = False) -> Any:
    """Parents and children (added and removed in Edit mode) and the Pipelines explorer."""
    from meerschaum.api.dash.pipes import build_relations_body
    explore = dcc.Link(
        'Explore graph',
        href=('/dash/pipelines/explore?pipe=' + quote(pipe_ref(pipe), safe='')
              + '&instance=' + quote(str(pipe.instance_keys), safe='')),
        className='btn btn-sm btn-secondary pipes-explore-link',
    )
    return html.Div([
        html.Div(explore, className='pipes-section-actions pipes-lineage-actions'),
        build_relations_body(pipe, authenticated=(allowed and edit), new_tab=False),
    ])


def runs_available(instance: Any) -> bool:
    """Whether the instance keeps engine runs the console can read."""
    cached = pipes_table._cache_get(('runs-available', str(instance)), 60.0)
    if cached is not None:
        return cached
    from meerschaum.api.dash import engine
    try:
        engine.get_runs(instance, limit=1)
        available = True
    except Exception:
        available = False
    pipes_table._cache_set(('runs-available', str(instance)), available)
    return available


def _run_pipeline_name(run: Dict[str, Any]) -> Optional[str]:
    record = run.get('run') or {}
    selection = record.get('selection')
    if isinstance(selection, str):
        try:
            selection = json.loads(selection)
        except ValueError:
            selection = None
    if isinstance(selection, dict):
        for snapshot in selection.get('pipeline_definitions') or []:
            if isinstance(snapshot, dict) and snapshot.get('name'):
                return str(snapshot['name'])
    return None


def runs_tab(pipe: mrsm.Pipe) -> Any:
    """The engine runs that selected or synced this pipe, newest first."""
    from meerschaum.api.dash import engine
    from urllib.parse import urlencode
    try:
        runs = engine.get_runs(pipe.instance_connector, pipe=pipe_ref(pipe), limit=RUNS_LIMIT)
    except Exception as e:
        return html.P(f"Runs are unavailable: {e}")
    if not runs:
        return html.P('No runs yet.', className='pipes-empty')
    rows = []
    for run in runs:
        record = run.get('run') or {}
        run_id = str(record.get('run_id', ''))
        pipeline = _run_pipeline_name(run)
        if pipeline:
            href = '/dash/pipelines/' + quote(pipeline, safe='') + '?' + urlencode({
                'run_id': run_id, 'instance': str(pipe.instance_keys),
            })
            run_cell = dcc.Link(run_id, href=href, className='pipes-run-link', title=run_id)
        else:
            run_cell = html.Code(run_id, title=run_id, className='pipes-run-id')
        status = str(run.get('status', ''))
        counts = ', '.join(
            f"{value} {key.replace('_', ' ')}"
            for key, value in (run.get('counts') or {}).items() if value
        )
        try:
            duration = f"{float(run.get('duration_seconds') or 0.0):.1f} s"
        except (TypeError, ValueError):
            duration = ''
        rows.append(html.Tr([
            html.Td(run_cell),
            html.Td(html.Span(status, className=f'run-status-{status}')),
            html.Td(str(record.get('started', ''))[:19].replace('T', ' ')),
            html.Td(duration),
            html.Td(counts),
            html.Td(pipeline or ''),
        ]))
    return html.Div(
        html.Table(
            [
                html.Thead(html.Tr([html.Th(label) for label in (
                    'Run', 'Status', 'Started', 'Duration', 'Stages', 'Pipeline',
                )])),
                html.Tbody(rows),
            ],
            className='table table-sm table-hover pipes-runs-table',
        ),
        className='pipes-table-scroll',
    )


def unregistered_state(pipe: mrsm.Pipe, can_register: bool = False) -> Any:
    """`Pipe(...) is not registered.`, with Register (New pipe with its keys) for those who may."""
    children = [
        html.H5(
            [pipes_table.pipe_title(pipe.connector_keys, pipe.metric_key, pipe.location_key),
             ' is not registered.'],
            className='pipes-unregistered-title',
        ),
    ]
    if can_register:
        children.append(dbc.Button(
            'Register', id='pipe-detail-register', href=register_href(pipe),
            color='primary', size='sm',
        ))
    return html.Div(children, id='pipe-detail-unregistered', className='pipes-unregistered')


def build_tab(
    tab: str,
    pipe: mrsm.Pipe,
    allowed: bool = False,
    edit: bool = False,
    state: Optional[Tuple[Optional[bool], str]] = None,
    can_register: bool = False,
    rights: Optional[Dict[str, bool]] = None,
) -> Any:
    """
    The content of one tab (`state`: the pipe's `registration()` when already read). A pipe whose
    instance cannot be read says why; an unregistered pipe offers Register when `can_register`.
    `rights` (`pipe_rights()`) decide what may be changed; without them `allowed` decides all.
    """
    registered, problem = state if state is not None else registration(pipe)
    if registered is None:
        return dbc.Alert(problem, color='danger', id='pipe-detail-read-error',
                         className='pipes-read-error')
    if not registered:
        return unregistered_state(pipe, can_register=can_register)
    if rights is None:
        rights = {key: bool(allowed) for key in ('edit', 'sync', 'define_sql', 'read')}
    read = rights.get('read', True)
    write = bool(rights.get('edit'))
    sql = str(pipe.connector_keys).startswith('sql:')
    builders = {
        'overview': lambda: overview_tab(pipe, allowed=read),
        'data': lambda: data_tab(pipe, allowed=read, edit=edit, can_sync=bool(rights.get('sync'))),
        'parameters': lambda: parameters_tab(pipe, allowed=write, edit=edit),
        'definition': lambda: definition_tab(
            pipe, allowed=(write and (not sql or bool(rights.get('define_sql')))), edit=edit,
        ),
        'lineage': lambda: lineage_tab(pipe, allowed=write, edit=edit),
        'runs': lambda: runs_tab(pipe),
    }
    return builders.get(tab, builders['overview'])()


# ------------------------------------------------------------------------------------------------
# Actions
# ------------------------------------------------------------------------------------------------

def action_sysargs(
    meta: Dict[str, Any],
    action: str,
    begin: Optional[str] = None,
    end: Optional[str] = None,
) -> Tuple[Optional[List[str]], str]:
    """
    `(sysargs, message)` of a header or Manage action on this pipe: `<action> pipes -c -m -l -i`
    (destructive actions with `--yes`: they were confirmed in the dialog; Clear may take bounds).
    """
    from meerschaum.api.dash.execute import build_pipe_sysargs
    sysargs, message = build_pipe_sysargs(meta, action, confirmed=True)
    if sysargs is None:
        return None, message
    if action == 'clear':
        for flag, value in (('--begin', begin), ('--end', end)):
            value = str(value or '').strip()
            if value:
                sysargs += [flag, value]
    return sysargs, ''


def copy_destination(
    pipe: mrsm.Pipe,
    connector_keys: Optional[str],
    metric_key: Optional[str],
    location_key: Optional[str],
    instance_keys: Optional[str],
) -> Tuple[Optional[Dict[str, Any]], str]:
    """
    `(destination, message)` of the Copy dialog: the new keys and instance (`same_keys` when only
    the instance changes), or `None` and why they cannot be used.
    """
    connector_keys = str(connector_keys or '').strip()
    metric_key = str(metric_key or '').strip()
    location_key = str(location_key or '').strip() or None
    if location_key in ('None', '[None]'):
        location_key = None
    instance_keys = str(instance_keys or pipe.instance_keys).strip()
    if not connector_keys or not metric_key:
        return None, "Enter the new pipe's connector and metric."
    same_keys = (
        connector_keys == str(pipe.connector_keys)
        and metric_key == str(pipe.metric_key)
        and location_key == pipe.location_key
    )
    if same_keys and instance_keys == str(pipe.instance_keys):
        return None, "Change the keys or the instance to copy this pipe."
    new_pipe = mrsm.Pipe(connector_keys, metric_key, location_key, instance=instance_keys)
    registered, problem = registration(new_pipe)
    if registered is None:
        return None, problem
    if registered:
        return None, f"{new_pipe} is already registered."
    return {
        'connector_keys': connector_keys,
        'metric_key': metric_key,
        'location_key': location_key,
        'instance_keys': instance_keys,
        'same_keys': same_keys,
    }, ''


def copy_pipe(
    pipe: mrsm.Pipe,
    destination: Dict[str, Any],
    viewer: Any = None,
) -> Tuple[mrsm.SuccessTuple, mrsm.Pipe]:
    """
    Register a copy of `pipe` with the same parameters under `destination`'s keys. An explicit
    `target` is left out when the keys change, so the copy gets its own table. The copy keeps the
    pipe's locks; its access is the viewer's (`pipe_writes.register_pipe`: they own it, in their
    primary team), as `copy pipes` resets it.
    """
    from copy import deepcopy
    from meerschaum.api.dash import pipe_writes
    parameters = deepcopy(pipe.get_parameters(apply_symlinks=False, refresh=True) or {})
    if not destination.get('same_keys'):
        for key in ('target', 'target_name', 'target_table', 'target_table_name'):
            parameters.pop(key, None)
    parameters.pop('access', None)
    new_pipe = mrsm.Pipe(
        destination['connector_keys'],
        destination['metric_key'],
        destination['location_key'],
        instance=destination['instance_keys'],
    )
    success, message = pipe_writes.register_pipe(new_pipe, parameters, viewer=viewer)
    if not success:
        return (False, message), new_pipe
    return (True, f"Copied {pipe} to {new_pipe}."), new_pipe


def copy_sysargs(pipe: mrsm.Pipe, instance_keys: str, copy_rows: bool = True) -> List[str]:
    """
    `copy pipes <instance>`: register this pipe on another instance under the same keys and
    (with `copy_rows`) copy its rows.
    """
    return [
        'copy', 'pipes', str(instance_keys),
        '-c', str(pipe.connector_keys),
        '-m', str(pipe.metric_key),
        '-l', str(pipe.location_key) if pipe.location_key is not None else 'None',
        '-i', str(pipe.instance_keys),
        '--sync-data', ('all' if copy_rows else 'nothing'),
        '--yes',
    ]
