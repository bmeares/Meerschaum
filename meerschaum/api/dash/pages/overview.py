#! /usr/bin/env python3
# -*- coding: utf-8 -*-
# vim:fenc=utf-8

"""
The Overview (`/dash`): the console's home page. Tiles for the instance's pipes (how many, how
many failing), its saved Pipelines (each one's last run), the jobs (running, stalled, failing,
stopped) and the recent runs (failures first), each linked to its page. On an instance without
pipes the Pipes tile shows New pipe and Open terminal instead of its numbers; the other tiles
stay (the server's jobs are not the instance's).

The page is built per request (`route`) for its instance: the console's global instance, or a
`?instance=` link's. The tiles load after the page (`callbacks/overview.py`) and refresh every
`OVERVIEW_REFRESH_MS`.
"""

from __future__ import annotations

from urllib.parse import parse_qs, urlparse

from meerschaum.utils.typing import Any, List, Optional
from meerschaum.api import CHECK_UPDATE
from meerschaum.utils.packages import import_html, import_dcc
html, dcc = import_html(check_update=CHECK_UPDATE), import_dcc(check_update=CHECK_UPDATE)
import dash_bootstrap_components as dbc

from meerschaum.api.dash.components import build_pages_navbar, get_nav_state
from meerschaum.api.dash.overview import OVERVIEW_REFRESH_MS

#: The tiles, in order: `(key, title, path)`.
TILES = (
    ('pipes', 'Pipes', '/dash/pipes'),
    ('pipelines', 'Pipelines', '/dash/pipelines'),
    ('jobs', 'Jobs', '/dash/jobs'),
    ('runs', 'Recent runs', None),
)


def _placeholder() -> Any:
    return html.Div(
        [html.Div(className='overview-skeleton-line'), html.Div(className='overview-skeleton-line short')],
        className='overview-skeleton',
        role='status',
        **{'aria-label': 'Loading'},
    )


def build_tile(key: str, title: str, href: Optional[str], extra: Any = None) -> Any:
    """
    An empty tile (its body is filled by `callbacks/overview.py`); `extra` follows the body (the
    Pipes tile's empty state).
    """
    heading = (
        dcc.Link(title, href=href, className='overview-tile-title')
        if href else html.Span(title, className='overview-tile-title')
    )
    return html.Section(
        [
            html.H2(heading, className='overview-tile-heading'),
            html.Div(_placeholder(), id=f'overview-{key}-body', className='overview-tile-body'),
        ]
        + ([extra] if extra is not None else [])
        + [dcc.Store(id=f'overview-{key}-digest', data=None)],
        id=f'overview-{key}',
        className=f'overview-tile overview-tile-{key}',
    )


def _can_register_pipes(session_id: Optional[str]) -> bool:
    try:
        from meerschaum.api.dash.pipes_access import can_register
        allowed, _ = can_register({'session-id': session_id} if session_id else {})
        return bool(allowed)
    except Exception:
        pass
    from meerschaum.api import no_auth
    from meerschaum.config import get_config
    if not get_config('api', 'permissions', 'registration', 'pipes', warn=False):
        return False
    if no_auth:
        return True
    from meerschaum.api.dash.sessions import is_session_authenticated
    return bool(session_id) and is_session_authenticated(str(session_id))


def build_empty_state(state: dict) -> Any:
    """
    What the Pipes tile of an instance without pipes shows: a short title, New pipe and Open
    terminal (each for a user who may use it).
    """
    actions: List[Any] = []
    if _can_register_pipes(state.get('session_id', None)):
        actions.append(dbc.Button('New pipe', href='/dash/pipes/new', color='primary', id='overview-new-pipe'))
    if state.get('terminal', False):
        actions.append(dbc.Button(
            'Open terminal', href='/dash/terminal', color='secondary', id='overview-open-terminal',
        ))
    return html.Div(
        [
            html.H3('No pipes yet', className='overview-empty-title'),
            html.Div(actions, className='overview-empty-actions'),
        ],
        id='overview-empty',
        className='overview-empty',
        hidden=True,
    )


def route(pathname: Optional[str] = None, href: Optional[str] = None) -> List[Any]:
    """The Overview of the requested (`?instance=`) or global instance."""
    from meerschaum.api.dash.engine import get_instance, instance_problem
    from meerschaum.api.dash.performance import build_overview_card
    query = parse_qs(urlparse(href or '').query)
    requested = (query.get('instance', None) or [None])[0] or None
    instance_keys = str(get_instance(requested))
    problem = instance_problem(requested)
    state = get_nav_state()
    performance = build_overview_card()
    return [
        build_pages_navbar(instance_keys=instance_keys),
        dcc.Store(
            id='overview-store',
            data={'instance': instance_keys, 'query': (instance_keys if requested else None)},
        ),
        dcc.Interval(id='overview-interval', interval=OVERVIEW_REFRESH_MS, n_intervals=0),
        html.Main(
            [
                html.H1('Overview', className='overview-title'),
                (
                    dbc.Alert(problem, color='danger', id='overview-instance-alert')
                    if problem else html.Div(id='overview-instance-alert')
                ),
                html.Div(
                    [
                        build_tile(
                            key, title, path,
                            build_empty_state(state) if key == 'pipes' else None,
                        )
                        for key, title, path in TILES
                    ] + ([performance] if performance is not None else []),
                    id='overview-tiles',
                    className='overview-grid',
                ),
            ],
            id='overview-page',
            className='overview-page',
            **{'data-instance': instance_keys},
        ),
    ]
