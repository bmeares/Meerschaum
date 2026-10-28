#! /usr/bin/env python3
# -*- coding: utf-8 -*-
# vim:fenc=utf-8

"""
The Overview's tiles (`pages/overview.py`): each loads on its own after the page and again every
`OVERVIEW_REFRESH_MS`, so a slow one never holds the others, and an unchanged tile is not redrawn.
"""

from __future__ import annotations

import json
import hashlib

import dash
from dash.dependencies import Input, Output, State
from dash.exceptions import PreventUpdate

from meerschaum.utils.typing import Any, Dict, Optional, Tuple
from meerschaum.api.dash import dash_app
from meerschaum.api.dash import overview


def _viewer(session_data: Optional[Dict[str, Any]]):
    """
    Who is looking at the Overview (`meerschaum.api.dash.access.Viewer`), if they may: a
    signed-in user (or `--no-auth`); else no update. Every tile counts only what they may see.
    """
    from meerschaum.api.dash.access import get_viewer
    viewer = get_viewer(session_data if isinstance(session_data, dict) else {})
    if not (viewer.no_auth or viewer.signed_in):
        raise PreventUpdate
    return viewer


def _instance(store: Optional[Dict[str, Any]]):
    from meerschaum.api.dash.engine import get_instance
    return get_instance((store or {}).get('instance', None))


def _digest(children: Any) -> str:
    import plotly.utils
    return hashlib.sha1(
        json.dumps(children, cls=plotly.utils.PlotlyJSONEncoder, sort_keys=True).encode('utf-8')
    ).hexdigest()


def _changed(children: Any, digest: Optional[str]) -> Tuple[Any, Any]:
    """`(children, digest)`, or `no_update` for both when the tile would look the same."""
    new_digest = _digest(children)
    if new_digest == digest:
        return dash.no_update, dash.no_update
    return children, new_digest


@dash_app.callback(
    Output('overview-pipes-body', 'children'),
    Output('overview-pipes-digest', 'data'),
    Output('overview-empty', 'hidden'),
    Output('overview-pipes-body', 'hidden'),
    Input('overview-interval', 'n_intervals'),
    State('overview-store', 'data'),
    State('overview-pipes-digest', 'data'),
    State('session-store', 'data'),
)
def render_pipes_tile(n_intervals, store, digest, session_data):
    """
    The Pipes tile. On an instance without pipes it shows the empty state (New pipe, Open
    terminal) in place of its numbers; the other tiles stay, since the jobs and runs they list
    may be the server's or the instance's other work.
    """
    viewer = _viewer(session_data)
    try:
        summary = overview.pipes_summary(_instance(store), viewer=viewer)
    except Exception as e:
        children, digest = _changed(overview.build_error(f"Could not read the pipes: {e}"), digest)
        return children, digest, True, False
    empty = summary['total'] == 0
    children, digest = _changed(overview.build_pipes_body(summary, (store or {}).get('query')), digest)
    return children, digest, (not empty), empty


@dash_app.callback(
    Output('overview-pipelines-body', 'children'),
    Output('overview-pipelines-digest', 'data'),
    Input('overview-interval', 'n_intervals'),
    State('overview-store', 'data'),
    State('overview-pipelines-digest', 'data'),
    State('session-store', 'data'),
)
def render_pipelines_tile(n_intervals, store, digest, session_data):
    """The Pipelines tile: each saved Pipeline's last run."""
    viewer = _viewer(session_data)
    try:
        children = overview.build_pipelines_body(
            overview.pipelines_summary(_instance(store), viewer=viewer),
            (store or {}).get('query'),
        )
    except Exception as e:
        children = overview.build_error(f"Could not read the Pipelines: {e}")
    return _changed(children, digest)


@dash_app.callback(
    Output('overview-jobs-body', 'children'),
    Output('overview-jobs-digest', 'data'),
    Input('overview-interval', 'n_intervals'),
    State('overview-jobs-digest', 'data'),
    State('session-store', 'data'),
)
def render_jobs_tile(n_intervals, digest, session_data):
    """The Jobs tile: the jobs by state."""
    viewer = _viewer(session_data)
    try:
        children = overview.build_jobs_body(overview.jobs_summary(viewer=viewer))
    except Exception as e:
        children = overview.build_error(f"Could not read the jobs: {e}")
    return _changed(children, digest)


@dash_app.callback(
    Output('overview-runs-body', 'children'),
    Output('overview-runs-digest', 'data'),
    Input('overview-interval', 'n_intervals'),
    State('overview-store', 'data'),
    State('overview-runs-digest', 'data'),
    State('session-store', 'data'),
)
def render_runs_tile(n_intervals, store, digest, session_data):
    """Recent runs, failures first."""
    viewer = _viewer(session_data)
    try:
        children = overview.build_runs_body(
            overview.recent_runs(_instance(store), viewer=viewer),
            (store or {}).get('query'),
        )
    except Exception as e:
        children = overview.build_error(f"Could not read the runs: {e}")
    return _changed(children, digest)
