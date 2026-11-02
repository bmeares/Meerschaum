#! /usr/bin/env python3
# -*- coding: utf-8 -*-
# vim:fenc=utf-8

"""
The console's Jobs page: the jobs table (a group per state, running jobs first, each job's
single-word status, when it last ran and its command), the job cards (the Runs tab, one job's
page, the Terminal page's Jobs button), the New job form and the command it builds.

The page reads every job from one listing (`get_job_snapshots()`: `meerschaum._core`'s
`jobs_get_summaries`, one batched `systemctl show` for the `systemd` jobs) instead of asking
each job for its status, health and times, which cost a few `systemctl` calls per job and value.
"""

from __future__ import annotations

import json
import shlex
import threading
import time
from datetime import datetime, timezone

from meerschaum.utils.typing import List, Dict, Any, Optional, Tuple, WebState
from meerschaum.utils.packages import attempt_import, import_html, import_dcc
from meerschaum.api.dash.sessions import is_session_authenticated
from meerschaum.api import CHECK_UPDATE
from meerschaum.jobs import (
    get_jobs,
    get_executor_keys_from_context,
    Job,
)
from meerschaum.config import get_config
from meerschaum.utils.misc import sorted_dict
dbc = attempt_import('dash_bootstrap_components', lazy=False, check_update=CHECK_UPDATE)
html, dcc = import_html(), import_dcc()

STATUS_EMOJI: Dict[str, str] = {
    'running': get_config('formatting', 'emoji', 'running'),
    'paused': get_config('formatting', 'emoji', 'paused'),
    'stopped': get_config('formatting', 'emoji', 'stopped'),
    'dne': get_config('formatting', 'emoji', 'failure')
}

EXECUTOR_KEYS: str = get_executor_keys_from_context()

#: The executor of the jobs the console creates (New job): the context's executor when it is one
#: the Jobs page lists and manages (`local` or `systemd`), else `local`.
CONSOLE_EXECUTOR_KEYS: str = EXECUTOR_KEYS if EXECUTOR_KEYS in ('local', 'systemd') else 'local'

#: Past this many jobs the Jobs tab has a filter box.
JOBS_FILTER_THRESHOLD: int = 8

#: How often (ms) the Jobs page refreshes its jobs' statuses and Last Run times.
JOBS_REFRESH_MS: int = 15000

#: The action the New job form starts with.
DEFAULT_JOB_ACTION: str = 'sync pipes'

#: Flags the New job form sets itself (its Name and Repeat fields), refused in its Arguments.
FORM_OWNED_FLAGS = frozenset({
    '-d', '--daemon', '--name', '--job-name', '-s', '--schedule', '--cron', '--loop',
    '--min-seconds', '--restart',
})

#: Flags that name the instance (the form adds the console's instance unless one is given).
INSTANCE_FLAGS = frozenset({'-i', '-I', '--instance', '--mrsm-instance'})


def is_automatic_job(job):
    """Explicit worker metadata takes precedence; support pre-rc6 execution workers."""
    try:
        properties = (job.daemon.properties if hasattr(job, 'daemon')
                      else job.get_daemon_properties())
        metadata = properties.get('mrsm_execution', {})
    except Exception:
        metadata = {}
    metadata = metadata if isinstance(metadata, dict) else {}
    kind = metadata.get('kind')
    return kind == 'temporary_execution' if kind else getattr(job, 'name', '').startswith('execute-')


def job_visible_to_session(job, session_id):
    """
    Whether the session's viewer may see a job (AC9, `meerschaum.jobs.can_see_job`): a job
    started through the API or the console is its team's (a console run is private to its owner,
    the team's managers and administrators); a local job is Everyone's.
    """
    from meerschaum.api import no_auth
    from meerschaum.api.dash.access import get_viewer
    if no_auth:
        return True
    viewer = get_viewer(session_id=(str(session_id) if session_id else None))
    if not viewer.signed_in:
        return False
    if viewer.is_admin:
        return True
    if not viewer.can('jobs:read'):
        return False
    from meerschaum.jobs import can_see_job
    try:
        return bool(can_see_job(viewer, job))
    except Exception:
        return False


def jobs_route_group_allowed() -> bool:
    """Whether `api:permissions:routes` serves the `jobs` route group (job changes need it)."""
    from meerschaum.api import route_group_is_allowed
    return route_group_is_allowed('jobs')


#: The permission each job control needs.
JOB_ACTION_PERMISSIONS: Dict[str, Tuple[str, ...]] = {
    'start': ('jobs:execute',),
    'pause': ('jobs:pause',),
    'stop': ('jobs:stop',),
    'restart': ('jobs:stop', 'jobs:execute'),
    'delete': ('jobs:delete',),
    'retain': ('jobs:write',),
    'logs': ('logs:read',),
    'create': ('jobs:write', 'actions:execute'),
}


def job_rights(viewer: Any = None) -> Dict[str, bool]:
    """
    What the viewer may do with the jobs they see: `start`, `pause`, `stop`, `restart`,
    `delete`, `retain` (Keep as Job), `logs` and `create` (New job), from their permissions,
    on a server that serves the `jobs` routes.
    """
    from meerschaum.api.dash.access import current_viewer
    viewer = viewer if viewer is not None else current_viewer()
    allowed = jobs_route_group_allowed()
    return {
        action: bool(
            (viewer.no_auth or viewer.signed_in)
            and all(viewer.can(permission) for permission in permissions)
            and (allowed or action == 'logs')
        )
        for action, permissions in JOB_ACTION_PERMISSIONS.items()
    }


def may_manage_jobs(session_id: Optional[str]) -> bool:
    """
    Whether a session may control jobs from the console at all (start, stop, pause, delete or
    create one: the job permissions) on a server that serves the `jobs` routes.
    """
    from meerschaum.api.dash.access import get_viewer
    rights = job_rights(get_viewer(session_id=(str(session_id) if session_id else None)))
    return any(value for key, value in rights.items() if key != 'logs')


def get_jobs_cards(state: WebState):
    """
    Build cards and alerts lists for jobs (the web terminal page's Jobs button): the jobs the
    session's viewer may see.
    """
    jobs = (
        get_job_snapshots()
        if EXECUTOR_KEYS in ('local', 'systemd')
        else get_jobs(executor_keys=EXECUTOR_KEYS, include_hidden=False)
    )
    session_id = state['session-store.data'].get('session-id', None)
    is_authenticated = may_manage_jobs(session_id) or job_rights().get('logs', False)

    cards = []

    for name, job in jobs.items():
        if is_automatic_job(job) or not job_visible_to_session(job, session_id):
            continue
        cards.append(build_job_card(job, authenticated=is_authenticated))

    return cards, []


########################################################################################
# One listing of every job (with its state) for the page's callbacks.
########################################################################################

def _parse_dt(value: Any) -> Optional[datetime]:
    """`Job.began` and friends: `datetime.fromisoformat()` of the native text (or `None`)."""
    if not value:
        return None
    try:
        return datetime.fromisoformat(str(value))
    except ValueError:
        return None


class JobSnapshot:
    """
    A job as one listing saw it (`meerschaum._core.jobs_get_summaries`): its name, executor,
    command, status, PID, result, health, times and properties, read in one pass. It answers
    what the page's helpers ask a `Job` (`status`, `sysargs`, `label`, `health`, `result`,
    `began`, `get_daemon_properties()`, ...) without another read; to act on the job (start,
    stop, logs), open it with `meerschaum.api.dash._support.get_job`.
    """

    __slots__ = (
        'name', 'executor_keys', 'sysargs', 'label', 'status', 'pid', 'result', 'restart',
        'health', 'began', 'ended', 'paused', 'stop_time', '_properties',
    )

    def __init__(self, row: Dict[str, Any]):
        self.name = str(row.get('name', ''))
        self.executor_keys = str(row.get('executor_keys') or 'local')
        self.sysargs = [str(arg) for arg in (row.get('sysargs') or [])]
        self.label = str(row.get('label') or ' '.join(self.sysargs))
        self.status = str(row.get('status') or 'stopped')
        self.pid = row.get('pid')
        result = row.get('result') or (False, 'No result available.')
        self.result = (bool(result[0]), str(result[1])) if len(result) == 2 else (False, '')
        self.restart = bool(row.get('restart'))
        self.health = row.get('health') if isinstance(row.get('health'), dict) else None
        self.began = _parse_dt(row.get('began'))
        self.ended = _parse_dt(row.get('ended'))
        self.paused = _parse_dt(row.get('paused'))
        self.stop_time = _parse_dt(row.get('stop_time'))
        properties = row.get('properties')
        self._properties = properties if isinstance(properties, dict) else {}

    def get_daemon_properties(self) -> Dict[str, Any]:
        """The daemon's properties as the listing read them."""
        return self._properties

    def exists(self) -> bool:
        """A listed job exists (as of the listing)."""
        return True

    def __repr__(self) -> str:
        return f"JobSnapshot({self.name!r}, {self.status!r})"


#: How long (seconds) one listing answers the page's callbacks: a page load and the refreshes
#: of every open tab within this window share it.
SNAPSHOT_MAX_AGE_SECONDS: float = 2.0

_snapshot_lock = threading.Lock()
_snapshot_cache: Dict[str, Any] = {}


def get_job_snapshots(max_age: Optional[float] = None) -> Dict[str, JobSnapshot]:
    """
    Every job the Jobs page lists (`local`, and the `systemd` jobs on a `systemd` host; hidden
    ones aside), by name, with its state: read at most `max_age` seconds ago
    (`SNAPSHOT_MAX_AGE_SECONDS` by default). Concurrent callers share one read.
    """
    max_age = SNAPSHOT_MAX_AGE_SECONDS if max_age is None else max_age
    with _snapshot_lock:
        cached = _snapshot_cache.get('jobs')
        if cached is not None and (time.monotonic() - cached[0]) <= max_age:
            return dict(cached[1])
        from meerschaum.jobs import _native
        rows = _native.core().jobs_get_summaries(False)
        snapshots = {}
        for row in rows:
            snapshot = JobSnapshot(row)
            snapshots[snapshot.name] = snapshot
        _snapshot_cache['jobs'] = (time.monotonic(), snapshots)
        return dict(snapshots)


def invalidate_job_snapshots() -> None:
    """Forget the last listing (a job was started, stopped, created or deleted)."""
    with _snapshot_lock:
        _snapshot_cache.pop('jobs', None)


def build_archived_run_card(record):
    """Show retained execution identity after its output expires."""
    from urllib.parse import urlencode
    metadata = record.get('metadata', {})
    execution = metadata.get('daemon', {}).get('properties', {}).get('mrsm_execution', {})
    execution = execution if isinstance(execution, dict) else {}
    pipeline = execution.get('pipeline') or {}
    pipeline_link = (html.A(pipeline.get('name', 'Pipeline'),
        href='/dash/pipelines?' + urlencode({'pipeline': pipeline.get('name', ''),
                                            'instance': pipeline.get('registry', '')}))
                     if isinstance(pipeline, dict) and pipeline.get('name') else None)
    return dbc.Card(dbc.CardBody([
        html.H5(record['name']), pipeline_link,
        html.P('Run: ' + str(execution.get('run_id', 'legacy'))),
        html.P('Completed: ' + str(record.get('ended', ''))),
        html.P('Output retention expired; execution summary retained.'),
        html.Details([html.Summary('Execution details'),
            html.Pre(json.dumps({'result': metadata.get('result'),
                                 'sysargs': metadata.get('sysargs'),
                                 'pipeline': pipeline}, indent=2))]),
    ]))


########################################################################################
# Status and Last Run.
########################################################################################

def job_health(job: Optional[Job]) -> Optional[Dict[str, Any]]:
    """
    The job's health (`job.health`: `state`, `last_success`, `last_lap`, `consecutive_failures`,
    `expected_interval_seconds`, `message`), or `None` when the job does not report one.
    """
    if job is None:
        return None
    try:
        health = getattr(job, 'health', None)
        if callable(health):
            health = health()
    except Exception:
        return None
    return health if isinstance(health, dict) else None


def _parse_ts(value: Any) -> Optional[datetime]:
    """An aware UTC datetime from an ISO-8601 string or a datetime (naive means UTC)."""
    if value is None or value == '':
        return None
    if isinstance(value, datetime):
        dt = value
    else:
        try:
            dt = datetime.fromisoformat(str(value).replace('Z', '+00:00'))
        except ValueError:
            return None
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(timezone.utc)


def time_ago(value: Any, now: Optional[datetime] = None) -> str:
    """`just now`, `2 min ago`, `3 h ago`, `1 day ago`, `12 days ago` (empty when unknown)."""
    ts = _parse_ts(value)
    if ts is None:
        return ''
    now = _parse_ts(now) or datetime.now(timezone.utc)
    seconds = max((now - ts).total_seconds(), 0)
    if seconds < 45:
        return 'just now'
    minutes = int(round(seconds / 60))
    if minutes < 60:
        return f"{max(minutes, 1)} min ago"
    hours = int(seconds // 3600)
    if hours < 24:
        return f"{hours} h ago"
    days = int(seconds // 86400)
    return f"{days} day{'s' if days != 1 else ''} ago"


def _is_sync_job(job: Job) -> bool:
    try:
        sysargs = job.sysargs
    except Exception:
        return False
    return bool(sysargs) and sysargs[0] == 'sync'


#: The single word of each state a job's Status shows.
STATE_LABELS: Dict[str, str] = {
    'running': 'Running',
    'stalled': 'Stalled',
    'failing': 'Failing',
    'paused': 'Paused',
    'stopped': 'Stopped',
    'failed': 'Failed',
    'dne': 'Missing',
}


def job_state(job: Optional[Job], health: Optional[Dict[str, Any]] = None) -> str:
    """
    A job's state: `running`, `stalled` (running, no lap for too long), `failing` (running, its
    laps fail), `paused`, `stopped`, `failed` (stopped after a failure it was not stopped by
    hand) or `dne`. The health (`job.health`) is read when not given.
    """
    if job is None:
        return 'dne'
    status = job.status
    health = health if health is not None else job_health(job)
    health = health or {}
    state = str(health.get('state') or '')
    try:
        failures = int(health.get('consecutive_failures') or 0)
    except (TypeError, ValueError):
        failures = 0
    if status == 'running':
        if state == 'stalled':
            return 'stalled'
        if state == 'failing' or failures > 0:
            return 'failing'
        return 'running'
    if status == 'paused':
        return 'paused'

    ### A stopped job's health counts failed laps, not how its one run ended: a job that ended
    ### with a failure (and was not stopped by hand) failed too.
    failed = state == 'failing' or failures > 0
    if not failed:
        try:
            result = job.result
            failed = (
                isinstance(result, (tuple, list))
                and len(result) > 0
                and result[0] is False
                and job.stop_time is None
            )
        except Exception:
            failed = False
    return 'failed' if failed else 'stopped'


def job_status_summary(
    job: Optional[Job],
    health: Optional[Dict[str, Any]] = None,
) -> Tuple[str, str]:
    """
    Return `(state, word)` for a job's Status: the state (`job_state`) names its color, the word
    is what the page shows (`Running`, `Stalled`, `Failing`, `Paused`, `Stopped`, `Failed`).
    When it last ran is `job_last_run`.
    """
    state = job_state(job, health)
    return state, STATE_LABELS.get(state, state.capitalize())


def _stamp(value: Any) -> str:
    ts = _parse_ts(value)
    return ts.strftime('%Y-%m-%d %H:%M UTC') if ts is not None else ''


def job_last_run(
    job: Optional[Job],
    health: Optional[Dict[str, Any]] = None,
    now: Optional[datetime] = None,
) -> Tuple[str, str]:
    """
    Return `(text, title)` for a job's Last Run: when its last lap ended (a job that repeats:
    `--loop`, `-s`, `--listen`), else when its run ended, or began while it runs (`2 min ago`;
    `Never` for a job that never ran). The title has the exact times, the last good one when the
    last runs failed, and how many failed in a row.
    """
    if job is None:
        return '', ''
    health = health if health is not None else job_health(job)
    health = health or {}
    noun = 'sync' if _is_sync_job(job) else 'run'
    last_lap = health.get('last_lap')
    status = getattr(job, 'status', 'stopped')
    if last_lap:
        when, verb = last_lap, 'ended'
    elif status in ('running', 'paused') and getattr(job, 'began', None):
        when, verb = job.began, 'began'
    elif getattr(job, 'ended', None):
        when, verb = job.ended, 'ended'
    elif getattr(job, 'began', None):
        when, verb = job.began, 'began'
    else:
        return 'Never', ''
    text = time_ago(when, now=now) or 'Never'
    lines = [f"Last {noun} {verb} {_stamp(when)}"] if _stamp(when) else []
    last_success = health.get('last_success')
    try:
        failures = int(health.get('consecutive_failures') or 0)
    except (TypeError, ValueError):
        failures = 0
    if failures > 0:
        lines.append(f"{failures} failed {noun}{'s' if failures != 1 else ''} in a row")
        if last_success:
            lines.append(f"Last good {noun} {_stamp(last_success)}")
    return text, '\n'.join(lines)


#: The states the Jobs page filters by (`/dash/jobs?status=stalled`, the Overview's Jobs tile).
JOB_FILTER_STATES: Dict[str, str] = {
    'running': 'Running',
    'stalled': 'Stalled',
    'failing': 'Failing',
    'paused': 'Paused',
    'stopped': 'Stopped',
}


def job_filter_state(job: Optional[Job]) -> str:
    """
    A job's state for `?status=`: `running`, `stalled`, `failing` (a running job whose laps fail,
    or a job that stopped with a failure), `paused` or `stopped`.
    """
    state = job_state(job)
    return 'failing' if state == 'failed' else state


#: The groups of the Jobs table, in their order: a job's group is its process's state.
JOB_GROUPS: Tuple[Tuple[str, str], ...] = (
    ('running', 'Running'),
    ('paused', 'Paused'),
    ('stopped', 'Stopped'),
)


def job_group(job: Optional[Job]) -> str:
    """`running` (also a stalled or failing job: its process runs), `paused` or `stopped`."""
    status = getattr(job, 'status', 'stopped') if job is not None else 'stopped'
    return status if status in ('running', 'paused') else 'stopped'


#: The status line's dot for each state of `job_status_summary` (the job cards): a running job
#: that stalls or fails gets the Overview's colors for those states (amber, red), not its
#: process's green.
STATE_EMOJI: Dict[str, str] = {
    'running': STATUS_EMOJI['running'],
    'paused': STATUS_EMOJI['paused'],
    'stopped': STATUS_EMOJI['stopped'],
    'stalled': '🟠',
    'failing': '🔴',
    'failed': '🔴',
    'dne': STATUS_EMOJI['dne'],
}


def build_status_children(job: Job) -> Any:
    """
    Return the status line of a job card: the dot and the word of its state.
    """
    if job is None:
        return STATUS_EMOJI['dne']

    state, text = job_status_summary(job)
    emoji = STATE_EMOJI.get(state, STATUS_EMOJI['stopped'])
    ### `running-job`, `paused-job`, `stopped-job` color the plain states (`dash.css`); the
    ### health states are colored by `job-state-*` (`console_pages.css`).
    status_class = f"{state}-job " if state in ('running', 'paused', 'stopped') else ''
    return html.P(
        [html.Span(emoji, className='job-status-emoji', **{'aria-hidden': 'true'}), ' ', html.B(text)],
        className=f"{status_class}job-status-line job-state-{state}",
        title=text,
    )


def build_row_status(job: Optional[Job], health: Optional[Dict[str, Any]] = None) -> Tuple[str, str]:
    """`(word, className)` of a table row's Status (its dot is the class's, `jobs.css`)."""
    state, word = job_status_summary(job, health)
    return word, f"job-status job-state-{state}"


########################################################################################
# Cards and rows.
########################################################################################

def _job_title(job: Job, size: str = 'h5') -> Any:
    heading = html.H5 if size == 'h5' else html.Span
    return heading(
        html.A(job.name, href=f"/dash/jobs/{job.name}", className='job-name-link console-name-link'),
        className='card-title job-title',
    )


def _job_command(job: Job) -> str:
    """The job's command on one line (chains keep their `+` and `:`)."""
    try:
        return ' '.join(str(job.label).split())
    except Exception:
        return ''


def actions_class_name(job: Optional[Job], variant: str = 'card') -> str:
    """
    The class of a job's actions container: the variant (`card` buttons or a table `row` menu)
    and the status they were built for (the refresh rebuilds them only when it changed).
    """
    status = job.status if job is not None else 'dne'
    base = 'job-row-actions' if variant == 'row' else 'job-card-actions'
    return f"{base} job-actions-{status}"


def build_job_card(
    job: Job,
    authenticated: bool = False,
    include_follow: bool = True,
    rights: Optional[Dict[str, bool]] = None,
):
    """
    Return a card for a given job: its logs menu and controls for an authenticated session,
    each one the viewer's permissions allow (`rights`, by default the request's: `job_rights`).
    """
    rights = (rights if rights is not None else job_rights()) if authenticated else {}
    footer_children = html.Div(
        build_process_timestamps_children(job),
        id={'type': 'process-timestamps-div', 'index': job.name},
    )
    follow_logs_button = dbc.DropdownMenuItem(
        "Follow logs",
        id={'type': 'follow-logs-button', 'index': job.name},
    )
    view_logs_button = dbc.DropdownMenuItem(
        "View",
        id={'type': 'job-view-logs-button', 'index': job.name},
    )
    download_logs_button = dbc.DropdownMenuItem(
        "Download",
        id={'type': 'job-download-logs-button', 'index': job.name},
    )
    logs_menu_children = (
        [view_logs_button] + ([follow_logs_button] if include_follow else []) + [download_logs_button]
        if rights.get('logs')
        else []
    )
    header_children = [
        html.Div(
            build_status_children(job),
            id={'type': 'manage-job-status-div', 'index': job.name},
            className='job-card-status',
        ),
    ] + ([
        html.Div(
            dbc.DropdownMenu(
                logs_menu_children,
                label="Logs",
                size="sm",
                align_end=True,
                color="secondary",
                menu_variant='dark',
                class_name='job-logs-menu',
            ),
        ),
    ] if logs_menu_children else [])

    body_children = [
        _job_title(job),
        html.Div(
            html.Code(
                job.label,
                className="card-text job-card-text",
                id={'type': 'job-label-p', 'index': job.name},
            ),
            className='job-card-command',
        ),
        html.Div(
            (
                build_manage_job_buttons_div_children(job, rights=rights)
                if authenticated
                else []
            ),
            id={'type': 'manage-job-buttons-div', 'index': job.name},
            className=actions_class_name(job, 'card'),
        ),
        html.Div(id={'type': 'manage-job-alert-div', 'index': job.name}),
        build_delete_job_modal(job),
    ]

    return dbc.Card(
        [
            dbc.CardHeader(header_children, class_name='job-card-header'),
            dbc.CardBody(body_children),
            dbc.CardFooter(footer_children),
        ],
        class_name='job-card',
        id={'type': 'job-card', 'index': job.name},
    )


def row_class_name(job: Optional[Job], gone: bool = False) -> str:
    """A table row's class: its group (`job-group-running`, ... `jobs.css` orders the groups)."""
    return f"job-row job-group-{job_group(job)}" + (' job-row-gone' if gone else '')


def build_job_row(
    job: Job,
    authenticated: bool = False,
    rights: Optional[Dict[str, bool]] = None,
    now: Optional[datetime] = None,
) -> Any:
    """
    Return a row of the jobs table: the job's name, its single-word Status, its Last Run, its
    command (wrapped, selectable) and a menu with its actions. The refresh updates the row in
    place (`callbacks/jobs.py`): its group class, Status, Last Run and, when the job's status
    changed, its menu.
    """
    rights = (rights if rights is not None else job_rights()) if authenticated else {}
    health = job_health(job)
    word, status_class = build_row_status(job, health)
    last_run, last_run_title = job_last_run(job, health, now=now)
    try:
        command = str(job.label).strip()
    except Exception:
        command = ''
    return html.Tr(
        [
            html.Td(
                html.A(job.name, href=f"/dash/jobs/{job.name}", className='job-name-link console-name-link'),
                className='job-row-name',
                role='cell',
            ),
            html.Td(
                html.Span(word, id={'type': 'job-row-status', 'index': job.name}, className=status_class),
                className='job-row-status',
                role='cell',
            ),
            html.Td(
                html.Span(
                    last_run,
                    id={'type': 'job-row-last-run', 'index': job.name},
                    title=last_run_title,
                    className='job-last-run',
                ),
                className='job-row-last-run',
                role='cell',
            ),
            html.Td(html.Code(command, className='job-command'), className='job-row-command', role='cell'),
            html.Td(
                html.Div(
                    build_job_row_menu(job, rights=rights) if authenticated else [],
                    id={'type': 'job-row-actions', 'index': job.name},
                    className=actions_class_name(job, 'row'),
                ),
                className='job-row-menu-cell',
                role='cell',
            ),
        ],
        id={'type': 'job-row', 'index': job.name},
        className=row_class_name(job),
        role='row',
    )


def build_jobs_table(jobs: List[Job], authenticated: bool = False, now: Optional[datetime] = None) -> Any:
    """
    The Jobs tab's table: a header row per group (Running, Paused, Stopped, in that order, each
    with its count; an empty group is hidden) and a row per job, by name. The rows stay in name
    order in the document and `jobs.css` orders them by their group's class, so the refresh moves
    a job to another group by changing its class, without rebuilding the table.
    """
    rights = job_rights() if authenticated else {}
    jobs = sorted(jobs, key=lambda job: job.name.lower())
    counts = {group: 0 for group, _ in JOB_GROUPS}
    for job in jobs:
        counts[job_group(job)] += 1
    group_rows = [
        html.Tr(
            html.Th(
                [
                    html.Span(label, className='jobs-group-label'),
                    html.Span(
                        str(counts[group]),
                        id={'type': 'jobs-group-count', 'index': group},
                        className='jobs-group-count',
                    ),
                ],
                colSpan=5,
                role='rowheader',
            ),
            id={'type': 'jobs-group', 'index': group},
            className=group_class_name(group, counts[group]),
            role='row',
        )
        for group, label in JOB_GROUPS
    ]
    return html.Table(
        [
            html.Thead(
                html.Tr(
                    [
                        html.Th('Job', role='columnheader'),
                        html.Th('Status', role='columnheader'),
                        html.Th('Last Run', role='columnheader'),
                        html.Th('Command', role='columnheader'),
                        html.Th(html.Span('Actions', className='visually-hidden'), role='columnheader'),
                    ],
                    className='jobs-columns-row',
                    role='row',
                ),
                role='rowgroup',
            ),
            html.Tbody(
                group_rows + [
                    build_job_row(job, authenticated=authenticated, rights=rights, now=now)
                    for job in jobs
                ],
                role='rowgroup',
            ),
        ],
        className='table jobs-grid',
        id='jobs-table',
        role='table',
    )


def group_class_name(group: str, count: int) -> str:
    """A group header row's class (`jobs-group-empty` hides it)."""
    return f"jobs-group-row jobs-group-{group}" + ('' if count else ' jobs-group-empty')


def _job_action_specs(job: Job, rights: Optional[Dict[str, bool]] = None) -> List[Tuple[str, str, str]]:
    """
    The `(action, label, color)` of the actions a job's current status allows (not Delete) and
    the viewer may run (`rights`; every one without).
    """
    specs = _status_action_specs(job)
    if rights is None:
        return specs
    return [spec for spec in specs if rights.get(spec[0])]


def _status_action_specs(job: Job) -> List[Tuple[str, str, str]]:
    status = job.status
    specs = []
    if is_automatic_job(job) and status == 'stopped':
        return specs
    if status == 'stopped':
        specs.append(('start', 'Start', 'success'))
    if status == 'paused':
        specs.append(('start', 'Resume', 'success'))
    if status == 'running':
        specs.append(('pause', 'Pause', 'warning'))
    if status in ('running', 'paused'):
        specs.append(('stop', 'Stop', 'danger'))
        specs.append(('restart', 'Restart', 'info'))
    return specs


def build_manage_job_buttons_div_children(job: Job, rights: Optional[Dict[str, bool]] = None):
    """
    Return the children for the manage job buttons div of a card (the controls the viewer may
    use: `rights`, by default the request's).
    """
    if job is None:
        return []
    rights = rights if rights is not None else job_rights()
    buttons = build_manage_job_buttons(job, rights=rights)
    if rights.get('delete'):
        buttons.append(
            dbc.Button(
                'Delete',
                size='sm',
                color='danger',
                outline=True,
                id={'type': 'job-delete-button', 'index': job.name},
                class_name='job-action-button',
            )
        )
    return html.Div(buttons, className='job-actions')


def build_manage_job_buttons(job: Job, rights: Optional[Dict[str, bool]] = None):
    """
    Return the currently available job management buttons for a given Job (besides Delete, see
    `build_manage_job_buttons_div_children`) that the viewer may use.
    """
    if job is None:
        return []
    rights = rights if rights is not None else job_rights()

    buttons = []
    if is_automatic_job(job) and job.status == 'stopped' and rights.get('retain'):
        buttons.append(dbc.Button('Keep as Job', size='sm', color='info',
            id={'type': 'retain-run-button', 'index': job.name}, class_name='job-action-button'))
    for action, label, color in _job_action_specs(job, rights=rights):
        buttons.append(
            dbc.Button(
                label,
                size='sm',
                color=color,
                id={'type': 'manage-job-button', 'action': action, 'index': job.name},
                class_name='job-action-button',
            )
        )
    return buttons


def build_job_row_menu(job: Job, rights: Optional[Dict[str, bool]] = None) -> Any:
    """
    The `⋮` menu of a row of the jobs table: the actions the viewer may use (handled by one
    callback for the table, `callbacks/jobs.py::jobs_table_action`), the logs and Delete (which
    asks first, in the page's one delete dialog).
    """
    if job is None:
        return []
    rights = rights if rights is not None else job_rights()
    items = [
        dbc.DropdownMenuItem(
            label,
            id={'type': 'job-row-action', 'action': action, 'index': job.name},
        )
        for action, label, _ in _job_action_specs(job, rights=rights)
    ]
    if rights.get('logs'):
        items += [
            dbc.DropdownMenuItem(divider=True),
            dbc.DropdownMenuItem('View logs', id={'type': 'job-view-logs-button', 'index': job.name}),
            dbc.DropdownMenuItem('Download logs', id={'type': 'job-download-logs-button', 'index': job.name}),
        ]
    if rights.get('delete'):
        items += [
            dbc.DropdownMenuItem(divider=True),
            dbc.DropdownMenuItem(
                'Delete',
                id={'type': 'job-row-action', 'action': 'delete', 'index': job.name},
                class_name='text-danger',
            ),
        ]
    items = items[1:] if items and getattr(items[0], 'divider', False) else items
    if not items:
        return []
    return dbc.DropdownMenu(
        items,
        label='⋮',
        toggle_class_name='job-row-menu-toggle',
        color='link',
        size='sm',
        align_end=True,
        menu_variant='dark',
        class_name='job-row-menu',
    )


def build_delete_job_modal(job: Job) -> dbc.Modal:
    """
    Return the confirmation modal for deleting a job: typing the job's name enables Delete.
    """
    return dbc.Modal(
        [
            dbc.ModalHeader(dbc.ModalTitle(["Delete job ", html.B(job.name), "?"])),
            dbc.ModalBody([
                html.Div(id={'type': 'job-delete-alert-div', 'index': job.name}),
                dbc.Label(['Type ', html.Code(job.name), ' to confirm']),
                dbc.Input(
                    id={'type': 'job-delete-name-input', 'index': job.name},
                    placeholder=job.name,
                    autoComplete='off',
                    debounce=False,
                ),
            ]),
            dbc.ModalFooter([
                dbc.Button(
                    "Cancel",
                    color='secondary',
                    id={'type': 'job-delete-cancel-button', 'index': job.name},
                ),
                dbc.Button(
                    "Delete",
                    color='danger',
                    disabled=True,
                    id={'type': 'job-delete-confirm-button', 'index': job.name},
                ),
            ]),
        ],
        is_open=False,
        id={'type': 'job-delete-modal', 'index': job.name},
    )


def build_jobs_delete_modal() -> Any:
    """
    The Jobs table's one delete dialog (a row's menu > Delete opens it for that job): typing the
    job's name enables Delete.
    """
    return html.Div([
        dbc.Modal(
            [
                dbc.ModalHeader(dbc.ModalTitle(id='jobs-delete-title')),
                dbc.ModalBody([
                    html.Div(id='jobs-delete-alert-div'),
                    dbc.Label(id='jobs-delete-label', html_for='jobs-delete-name-input'),
                    dbc.Input(id='jobs-delete-name-input', autoComplete='off', debounce=False),
                ]),
                dbc.ModalFooter([
                    dbc.Button('Cancel', color='secondary', id='jobs-delete-cancel-button'),
                    dbc.Button('Delete', color='danger', disabled=True, id='jobs-delete-confirm-button'),
                ]),
            ],
            id='jobs-delete-modal',
            is_open=False,
        ),
        dcc.Store(id='jobs-delete-target', data=None),
    ])


def build_process_timestamps_children(job: Job) -> List[dbc.Row]:
    """
    Return the children to the process timestamps in the footer of the job card: when it last
    ran, then when it began, paused and ended.
    """
    if job is None:
        return []

    def row(key: str, value: str, title: str = '') -> Any:
        return dbc.Row(
            [
                dbc.Col(
                    html.P(key, style={'font-size': 'small'}, className='text-muted mb-0'),
                    width=4,
                ),
                dbc.Col(
                    html.P(
                        value,
                        style={'font-size': 'small', 'text-align': 'right'},
                        className='text-muted mb-0',
                        title=title or None,
                    ),
                    width=8,
                ),
            ],
            justify='between',
        )

    children = []
    last_run, last_run_title = job_last_run(job)
    if last_run:
        children.append(row('Last run', last_run, last_run_title))
    for timestamp_key, timestamp in sorted_dict(
        {
            'began': job.began,
            'paused': job.paused,
            'ended': job.ended,
        }
    ).items():
        if timestamp is None:
            continue
        children.append(row(timestamp_key.capitalize(), timestamp.strftime('%Y-%m-%d %H:%M UTC')))
    return children


def get_run_cards(state):
    """Automatic execution workers and retained summaries in the Web Console."""
    session_id = (state.get('session-store.data') or {}).get('session-id')
    authenticated = may_manage_jobs(session_id) or job_rights().get('logs', False)
    jobs = get_job_snapshots()
    cards = [build_job_card(job, authenticated=authenticated, include_follow=False)
             for job in jobs.values()
             if is_automatic_job(job) and job_visible_to_session(job, session_id)]
    if authenticated:
        from meerschaum.api.dash.execute import execution_request
        from meerschaum.api.dash.sessions import get_username_from_session
        records = execution_request('/execute/runs', get_username_from_session(session_id)) or []
        cards.extend(build_archived_run_card(record) for record in records)
    return cards, []


########################################################################################
# The New job form.
########################################################################################

def job_action_options(omitted: Optional[frozenset] = None) -> List[Dict[str, str]]:
    """
    The actions the New job form offers: every action and subaction (`sync pipes`, `verify
    pipes`, `show version`, a plugin's action, ...) the console does not omit, `sync pipes` first.
    """
    import textwrap
    from meerschaum.actions import actions, get_subactions
    if omitted is None:
        from meerschaum.api.dash.execute import omit_actions
        omitted = omit_actions()
    options = []
    for action_name, function in actions.items():
        action = action_name.replace('_', ' ')
        if action_name in omitted or action.split(' ')[0] in omitted:
            continue
        try:
            subactions = get_subactions(action_name)
        except Exception:
            subactions = {}
        if not subactions:
            doc = textwrap.dedent(function.__doc__ or '').strip().split('\n')[0]
            options.append({'label': action, 'value': action, 'title': doc})
            continue
        for sub_name, sub_function in subactions.items():
            value = f"{action} {sub_name.replace('_', ' ')}"
            doc = textwrap.dedent(sub_function.__doc__ or '').strip().split('\n')[0]
            options.append({'label': value, 'value': value, 'title': doc})
    seen = set()
    unique = []
    for option in sorted(options, key=lambda o: o['label']):
        if option['value'] in seen:
            continue
        seen.add(option['value'])
        unique.append(option)
    first = [o for o in unique if o['value'] == DEFAULT_JOB_ACTION]
    if not first:
        first = [{'label': DEFAULT_JOB_ACTION, 'value': DEFAULT_JOB_ACTION}]
    return first + [o for o in unique if o['value'] != DEFAULT_JOB_ACTION]


def action_selects_pipes(action: Optional[str]) -> bool:
    """Whether an action works on pipes (`sync pipes`, `verify pipes`, ...): the form asks which."""
    words = str(action or '').split()
    return len(words) >= 2 and words[1] in ('pipes', 'pipe')


def action_selects_pipeline(action: Optional[str]) -> bool:
    """Whether an action syncs a saved Pipeline (`sync pipelines`): the form asks which one."""
    words = str(action or '').split()
    return len(words) >= 2 and words[0] == 'sync' and words[1] in ('pipelines', 'pipeline')


def pipes_keys_options(keys: Any) -> Tuple[Dict[str, List[str]], bool]:
    """
    Return `(options, with_parameters)`: the New job form's Connectors, Metrics, Locations and
    Tags options from what `fetch_pipes_keys('registered', instance)` returns, a dict of pipe IDs
    to key tuples or a list of them. Each tuple is `(connector_keys, metric_key, location_key)`,
    optionally with the pipe's parameters (or its tags) as a fourth item; a pipe without a
    location is `'None'`. Internal `mrsm` pipes are left out. `with_parameters` says whether the
    tuples had that fourth item (without it the tags are not known from the keys).
    """
    items = list(keys.values()) if isinstance(keys, dict) else list(keys or [])
    connectors, metrics, locations, tags = set(), set(), set(), set()
    with_parameters = False
    for item in items:
        if not isinstance(item, (list, tuple)) or len(item) < 3:
            continue
        connector_keys, metric_key, location_key = item[0], item[1], item[2]
        if str(connector_keys) == 'mrsm':
            continue
        connectors.add(str(connector_keys))
        metrics.add(str(metric_key))
        locations.add(str(location_key))
        if len(item) < 4:
            continue
        with_parameters = True
        extra = item[3]
        if isinstance(extra, str):
            try:
                extra = json.loads(extra)
            except ValueError:
                extra = None
        pipe_tags = extra.get('tags', None) if isinstance(extra, dict) else extra
        if isinstance(pipe_tags, str):
            pipe_tags = [pipe_tags]
        if isinstance(pipe_tags, (list, tuple, set)):
            tags.update(str(tag) for tag in pipe_tags if tag is not None and str(tag) != '')
    key = lambda value: value.lower()
    return {
        'connector_keys': sorted(connectors, key=key),
        'metric_keys': sorted(metrics, key=key),
        'location_keys': sorted(locations, key=key),
        'tags': sorted(tags, key=key),
    }, with_parameters


def build_new_job_sysargs(
    action: Optional[str] = DEFAULT_JOB_ACTION,
    connector_keys: Optional[List[str]] = None,
    metric_keys: Optional[List[str]] = None,
    location_keys: Optional[List[Optional[str]]] = None,
    tags: Optional[List[str]] = None,
    pipeline: Optional[str] = None,
    arguments: Optional[str] = None,
    repeat: Optional[str] = 'once',
    schedule: Optional[str] = None,
    min_seconds: Any = None,
    restart: bool = False,
    instance: Optional[str] = None,
    omitted: Optional[frozenset] = None,
) -> Tuple[Optional[List[str]], str]:
    """
    Return `(sysargs, message)`: the command the New job form describes (what `start job` runs),
    or `(None, why)` when the form is not complete or not valid.

    The selection applies to actions on pipes (`-c`, `-m`, `-l`, `-t`) and to `sync pipelines`
    (the saved Pipeline's name); the console's instance is added (`-i`) for both unless the
    arguments name one. `repeat` is `once` (optionally `--restart`), `schedule` (`-s <schedule>`)
    or `loop` (`--loop`, optionally `--min-seconds`).
    """
    if omitted is None:
        from meerschaum.api.dash.execute import omit_actions
        omitted = omit_actions()
    try:
        words = shlex.split(str(action or '').strip())
    except ValueError as e:
        return None, f"Invalid action: {e}"
    if not words:
        return None, "Choose an action."
    if words[0].startswith('-'):
        return None, "Choose an action."
    if words[0] in omitted:
        return None, f"The action '{words[0]}' cannot run as a job from the console."

    try:
        extra = shlex.split(str(arguments or '').strip())
    except ValueError as e:
        return None, f"Invalid arguments: {e}"
    for arg in extra:
        flag = arg.split('=', 1)[0]
        if flag in FORM_OWNED_FLAGS:
            return None, f"Remove '{flag}' from the arguments: the form sets it."

    sysargs: List[str] = list(words)
    if action_selects_pipeline(action):
        if not pipeline:
            return None, "Choose a Pipeline."
        sysargs = ['sync', 'pipelines', str(pipeline)]
    elif action_selects_pipes(action):
        for flag, values in (
            ('-c', connector_keys),
            ('-m', metric_keys),
            ('-l', location_keys),
            ('-t', tags),
        ):
            values = [
                ('None' if value is None else str(value))
                for value in (values or [])
                if value is None or str(value).strip()
            ]
            if values:
                sysargs += [flag] + values

    sysargs += extra
    if (
        instance
        and (action_selects_pipes(action) or action_selects_pipeline(action))
        and not any(arg.split('=', 1)[0] in INSTANCE_FLAGS for arg in extra)
    ):
        sysargs += ['-i', str(instance)]

    repeat = (repeat or 'once').lower()
    if repeat == 'schedule':
        schedule = str(schedule or '').strip()
        if not schedule:
            return None, 'Enter a schedule, for example "every 10 minutes".'
        try:
            from meerschaum.utils.schedule import parse_schedule
            parse_schedule(schedule)
        except Exception as e:
            return None, f"Invalid schedule '{schedule}': {e}"
        sysargs += ['-s', schedule]
    elif repeat == 'loop':
        sysargs.append('--loop')
        if min_seconds not in (None, ''):
            try:
                seconds = float(min_seconds)
            except (TypeError, ValueError):
                return None, "The seconds between runs must be a number."
            if seconds < 0:
                return None, "The seconds between runs cannot be negative."
            sysargs += ['--min-seconds', str(int(seconds)) if seconds.is_integer() else str(seconds)]
    elif repeat == 'once':
        if restart:
            sysargs.append('--restart')
    else:
        return None, f"Unknown repeat option '{repeat}'."

    return sysargs, ''


def job_command_preview(sysargs: Optional[List[str]], name: Optional[str] = None) -> str:
    """The terminal command that creates the same job: `mrsm <sysargs> --name <name> -d`."""
    if not sysargs:
        return ''
    args = list(sysargs)
    if name:
        args += ['--name', str(name)]
    args.append('-d')
    return 'mrsm ' + shlex.join(args)


def validate_new_job_name(name: Optional[str], existing: Optional[List[str]] = None) -> Tuple[bool, str]:
    """Whether `name` may name a new job (allowed characters, not taken)."""
    from meerschaum.jobs._Job import BANNED_CHARS
    name = str(name or '').strip()
    if not name:
        return False, "Enter a name for the job."
    for char in BANNED_CHARS:
        if char in name:
            return False, f"Job names cannot contain '{char}'."
    if any(c.isspace() for c in name) or '/' in name or '\\' in name or name in ('.', '..'):
        return False, "Job names cannot contain spaces or slashes."
    if existing is not None and name in existing:
        return False, f"A job named '{name}' already exists."
    return True, ''


def build_new_job_modal() -> dbc.Modal:
    """The New job form (filled each time it opens, see `callbacks/jobs.py`)."""
    field = lambda label, control, field_id=None, **kw: html.Div(
        [dbc.Label(label, html_for=field_id) if field_id else dbc.Label(label), control],
        className='new-job-field',
        **kw,
    )
    return dbc.Modal(
        [
            dbc.ModalHeader(dbc.ModalTitle('New job')),
            dbc.ModalBody([
                html.Div(id='new-job-alert-div'),
                field(
                    'Action',
                    dcc.Dropdown(
                        id='new-job-action',
                        options=[{'label': DEFAULT_JOB_ACTION, 'value': DEFAULT_JOB_ACTION}],
                        value=DEFAULT_JOB_ACTION,
                        clearable=False,
                        searchable=True,
                        className='dbc_dark new-job-dropdown',
                    ),
                    'new-job-action',
                ),
                html.Div(
                    [
                        html.Div([
                            field('Connectors', dcc.Dropdown(
                                id='new-job-connector-keys', multi=True, placeholder='All',
                                className='dbc_dark new-job-dropdown',
                            ), 'new-job-connector-keys'),
                            field('Metrics', dcc.Dropdown(
                                id='new-job-metric-keys', multi=True, placeholder='All',
                                className='dbc_dark new-job-dropdown',
                            ), 'new-job-metric-keys'),
                            field('Locations', dcc.Dropdown(
                                id='new-job-location-keys', multi=True, placeholder='All',
                                className='dbc_dark new-job-dropdown',
                            ), 'new-job-location-keys'),
                            field('Tags', dcc.Dropdown(
                                id='new-job-tags', multi=True, placeholder='Any',
                                className='dbc_dark new-job-dropdown',
                            ), 'new-job-tags'),
                        ], className='new-job-keys'),
                    ],
                    id='new-job-pipes-div',
                ),
                html.Div(
                    field('Pipeline', dcc.Dropdown(
                        id='new-job-pipeline', placeholder='Choose a Pipeline',
                        className='dbc_dark new-job-dropdown',
                    ), 'new-job-pipeline'),
                    id='new-job-pipeline-div',
                    hidden=True,
                ),
                field('Arguments', dbc.Input(
                    id='new-job-arguments', placeholder='--workers 4', autoComplete='off',
                    debounce=True,
                ), 'new-job-arguments'),
                field('Repeat', dbc.RadioItems(
                    id='new-job-repeat',
                    options=[
                        {'label': 'Once', 'value': 'once'},
                        {'label': 'On a schedule', 'value': 'schedule'},
                        {'label': 'Continuously', 'value': 'loop'},
                    ],
                    value='schedule',
                    inline=True,
                    class_name='new-job-repeat',
                ), 'new-job-repeat'),
                html.Div(
                    field('Schedule', dbc.Input(
                        id='new-job-schedule',
                        placeholder='every 10 minutes · daily starting 00:00 · 0 * * * *',
                        autoComplete='off',
                        debounce=True,
                    ), 'new-job-schedule'),
                    id='new-job-schedule-div',
                ),
                html.Div(
                    field('Seconds between runs', dbc.Input(
                        id='new-job-min-seconds', type='number', min=0, placeholder='60',
                        debounce=True,
                    ), 'new-job-min-seconds'),
                    id='new-job-loop-div',
                    hidden=True,
                ),
                html.Div(
                    dbc.Switch(id='new-job-restart', label='Restart if it stops', value=False),
                    id='new-job-restart-div',
                    hidden=True,
                    className='new-job-field',
                ),
                field('Name', dbc.Input(
                    id='new-job-name', autoComplete='off', debounce=True,
                ), 'new-job-name'),
                field('Command', html.Pre(id='new-job-preview', className='new-job-preview')),
            ]),
            dbc.ModalFooter([
                dbc.Button('Cancel', id='new-job-cancel-button', color='secondary'),
                dbc.Button('Create job', id='new-job-create-button', color='primary', disabled=True),
            ]),
        ],
        id='new-job-modal',
        is_open=False,
        size='lg',
        fullscreen='sm-down',
        scrollable=True,
    )


#: How often (ms) the logs modal re-reads a running job's logs.
JOB_LOGS_REFRESH_MS: int = 1500

def build_job_logs_modal() -> Any:
    """
    The logs modal ("Logs" > "View" on a job card, "View output" of a plugin action): the job's
    logs rendered with their ANSI colors, opened at the bottom and following new lines while the
    job runs (see `callbacks/jobs.py`). One per page that shows job logs.
    """
    return html.Div([
        dbc.Modal(
            [
                dbc.ModalHeader(dbc.ModalTitle(id='job-logs-modal-title')),
                dbc.ModalBody(html.Pre(id='job-logs-view', className='job-logs-view')),
            ],
            id='job-logs-modal',
            size='xl',
            is_open=False,
            fullscreen='sm-down',
        ),
        dcc.Store(id='job-logs-meta', data=None),
        dcc.Store(id='job-logs-text', data=None),
        dcc.Interval(id='job-logs-interval', interval=JOB_LOGS_REFRESH_MS, disabled=True),
        html.Div(id='job-logs-dummy', hidden=True),
    ])


#: The logs modal of the Jobs and web terminal pages.
job_logs_modal = build_job_logs_modal()
