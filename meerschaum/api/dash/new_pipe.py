#! /usr/bin/env python3
# -*- coding: utf-8 -*-
# vim:fenc=utf-8

"""
The New pipe form (`/dash/pipes/new`), the console's `bootstrap pipes`: the connector (an
existing one or any keys), metric, location and instance; for `sql:` connectors a query with
Preview, whose columns fill the Datetime and ID pickers; for other connectors the parameters.
Nothing is saved before Register (or Register and sync).
"""

from __future__ import annotations

import json
import re

import meerschaum as mrsm
from meerschaum.utils.typing import Any, Dict, List, Optional, Tuple
from meerschaum.utils.packages import attempt_import, import_html, import_dcc
from meerschaum.api import CHECK_UPDATE

html, dcc = import_html(check_update=CHECK_UPDATE), import_dcc(check_update=CHECK_UPDATE)
import dash_bootstrap_components as dbc
dash_ace = attempt_import('dash_ace', lazy=False, check_update=CHECK_UPDATE)

#: How many rows Preview shows.
PREVIEW_LIMIT: int = 10

#: The metric key of the temporary pipe Preview fetches through (it is never registered).
PREVIEW_METRIC: str = 'mrsm_preview'

#: Column names that usually hold a pipe's datetime axis, most likely first.
DATETIME_NAMES: Tuple[str, ...] = (
    'timestamp', 'datetime', 'ts', 'dt', 'time', 'date', 'created_at', 'updated_at',
)
DATETIME_SUFFIXES: Tuple[str, ...] = ('_at', '_time', '_ts', '_timestamp', '_datetime', '_date', '_dt')


def defined_connectors(*types: str) -> List[str]:
    """
    The keys of the defined connectors (of `types`, else of every type and the plugins), without
    Meerschaum's own environment variables read as connectors (`api:host`, `api:host_config`,
    ...), as the console's other connector lists.
    """
    from meerschaum.utils.misc import get_connector_labels
    from meerschaum.api.dash.connector_listing import is_internal_connector
    try:
        labels = list(get_connector_labels(*types))
    except Exception:
        return []
    seen, out = set(), []
    for label in labels:
        typ, _, name = str(label).partition(':')
        if label in seen or not name or is_internal_connector(typ, name):
            continue
        seen.add(label)
        out.append(str(label))
    return out


def connector_options() -> List[str]:
    """Every defined connector (configuration, `MRSM_<TYPE>_<LABEL>`) and plugin connector."""
    options = [label for label in defined_connectors() if not label.endswith(':default')]
    return sorted(options, key=lambda label: (not label.startswith('sql:'), label.lower()))


def default_connector(connectors: List[str], instance_keys: Optional[str] = None) -> str:
    """The connector New pipe starts with: the instance when it is a `sql:` connector, else the first `sql:` one."""
    if instance_keys and str(instance_keys).startswith('sql:') and str(instance_keys) in connectors:
        return str(instance_keys)
    return next((label for label in connectors if label.startswith('sql:')), '') or ''


def instance_options(current: Optional[str] = None) -> List[Dict[str, str]]:
    """The instance connectors to choose from (`current` first when it is not defined)."""
    from meerschaum.connectors import instance_types
    labels = defined_connectors(*instance_types)
    if current and current not in labels:
        labels.insert(0, current)
    return [{'label': label, 'value': label} for label in labels]


def is_sql_connector(connector_keys: Optional[str]) -> bool:
    return str(connector_keys or '').strip().startswith('sql:')


def layout(
    instance_keys: str,
    allowed: bool = True,
    reason: str = '',
    connector_keys: Optional[str] = None,
    metric_key: Optional[str] = None,
    location_key: Optional[str] = None,
) -> List[Any]:
    """
    The form; Register stays disabled for sessions that may not register pipes. The keys may be
    filled in (`/dash/pipes/new?connector_keys=...&metric_key=...&location_key=...`, the Register
    button of an unregistered pipe's page).
    """
    connectors = connector_options()
    sql_editor = dash_ace.DashAceEditor(
        id='new-pipe-query',
        value='',
        mode='sql',
        theme='twilight',
        tabSize=4,
        width='100%',
        height='220px',
        placeholder='SELECT * FROM my_table',
        showGutter=True,
        showPrintMargin=False,
        highlightActiveLine=True,
        wrapEnabled=True,
        style={'min-height': '120px'},
    )
    parameters_editor = dash_ace.DashAceEditor(
        id='new-pipe-parameters',
        value='{}',
        mode='norm',
        theme='twilight',
        tabSize=4,
        width='100%',
        height='200px',
        showGutter=True,
        showPrintMargin=False,
        highlightActiveLine=True,
        wrapEnabled=True,
        style={'min-height': '120px'},
    )
    field = lambda label, control, field_id, class_name='': html.Div(
        [dbc.Label(label, html_for=field_id), control],
        className=('new-pipe-field ' + class_name).strip(),
    )
    return [
        dcc.Location(id='new-pipe-location', refresh=False),
        dcc.Store(id='pipes-follow-store', storage_type='session'),
        dcc.Store(id='new-pipe-navigate-store', data=None),
        html.Datalist([html.Option(value=label) for label in connectors], id='new-pipe-connectors'),
        html.Datalist([], id='new-pipe-columns'),
        html.Main(
            [
                dcc.Link('Pipes', href='/dash/pipes', className='pipes-breadcrumb'),
                html.Div(html.H3('New pipe'), className='pipes-header'),
                (dbc.Alert(reason, color='warning', id='new-pipe-refused') if not allowed else html.Div()),
                html.Div(
                    [
                        field(
                            'Connector',
                            dbc.Input(id='new-pipe-connector', list='new-pipe-connectors',
                                      placeholder='sql:main', autocomplete='off',
                                      value=(str(connector_keys).strip() if connector_keys else
                                             default_connector(connectors, instance_keys))),
                            'new-pipe-connector',
                        ),
                        field('Metric', dbc.Input(id='new-pipe-metric', placeholder='weather', autocomplete='off',
                                                  value=(str(metric_key) if metric_key else None)),
                              'new-pipe-metric'),
                        field('Location', dbc.Input(id='new-pipe-location-key', placeholder='None',
                                                    autocomplete='off',
                                                    value=(str(location_key) if location_key else None)),
                              'new-pipe-location-key'),
                        field(
                            'Instance',
                            dbc.Select(id='new-pipe-instance', options=instance_options(instance_keys),
                                       value=instance_keys,
                                       class_name='dbc_dark custom-select custom-select-sm'),
                            'new-pipe-instance',
                        ),
                    ],
                    className='new-pipe-keys',
                ),
                html.Div(
                    [
                        html.Div(
                            [
                                dbc.Label('Query', html_for='new-pipe-query'),
                                sql_editor,
                                html.Div(
                                    dbc.Button('Preview', id='new-pipe-preview', n_clicks=0, size='sm',
                                               color='secondary'),
                                    className='new-pipe-preview-actions',
                                ),
                                dcc.Loading(
                                    html.Div(id='new-pipe-preview-result', className='new-pipe-preview'),
                                    type='dot',
                                    target_components={'new-pipe-preview-result': 'children'},
                                ),
                            ],
                            id='new-pipe-sql',
                        ),
                        html.Details(
                            [html.Summary('Parameters'), parameters_editor],
                            id='new-pipe-parameters-details',
                            className='new-pipe-parameters',
                        ),
                    ],
                    className='new-pipe-source',
                ),
                html.Div(
                    [
                        field('Datetime column', dbc.Input(id='new-pipe-datetime', list='new-pipe-columns',
                                                           placeholder='None', autocomplete='off'),
                              'new-pipe-datetime'),
                        field('ID column', dbc.Input(id='new-pipe-id', list='new-pipe-columns',
                                                     placeholder='None', autocomplete='off'),
                              'new-pipe-id'),
                    ],
                    className='new-pipe-columns',
                ),
                html.Div(id='new-pipe-alert'),
                html.Div(
                    [
                        dbc.Button('Register', id='new-pipe-register', n_clicks=0, color='secondary',
                                   disabled=not allowed),
                        dbc.Button('Register and sync', id='new-pipe-register-sync', n_clicks=0,
                                   color='primary', disabled=not allowed),
                    ],
                    className='new-pipe-actions',
                ),
            ],
            id='new-pipe-page',
            className='pipes-page new-pipe-page',
            **{'data-sql': 'true'},
        ),
    ]


# ------------------------------------------------------------------------------------------------
# Preview
# ------------------------------------------------------------------------------------------------

def _strip_query(query: str) -> str:
    query = str(query or '').strip()
    while query.endswith(';'):
        query = query[:-1].rstrip()
    return query


def limit_query(query: str, flavor: str, limit: int = PREVIEW_LIMIT) -> str:
    """
    Limit a query to its first `limit` rows in the flavor's syntax: `TOP` on the top-level
    `SELECT` for SQL Server (so an `ORDER BY` stays valid), `ROWNUM` for Oracle, else
    `LIMIT` on the query as a derived table.
    """
    from meerschaum.utils.sql import find_top_level_select_index
    query = _strip_query(query)
    limit = int(limit)
    if flavor == 'mssql':
        index = find_top_level_select_index(query)
        if index == -1:
            return f"SELECT TOP {limit} *\nFROM (\n{query}\n) AS [mrsm_preview]"
        head, rest = query[:index + len('select')], query[index + len('select'):]
        stripped = rest.lstrip()
        if re.match(r'(?is)^top\b', stripped):
            return query
        distinct = re.match(r'(?is)^(distinct|all)\b', stripped)
        if distinct:
            keyword = stripped[:distinct.end()]
            return f"{head} {keyword} TOP {limit} {stripped[distinct.end():].lstrip()}"
        return f"{head} TOP {limit} {stripped}"
    if flavor == 'oracle':
        return f"SELECT *\nFROM (\n{query}\n)\nWHERE ROWNUM <= {limit}"
    return f"SELECT *\nFROM (\n{query}\n) AS mrsm_preview\nLIMIT {limit}"


def preview_query(
    connector_keys: str,
    query: str,
    instance_keys: str,
    limit: int = PREVIEW_LIMIT,
) -> Tuple[Any, str]:
    """
    Run `query` on the source connector, limited to `limit` rows, through the pipe fetch path
    (so `{{ Pipe(...) }}` references resolve on `instance_keys`). Return `(df, message)`;
    `df` is `None` (and `message` says why) when it failed.
    """
    connector_keys = str(connector_keys or '').strip()
    query = _strip_query(query)
    if not is_sql_connector(connector_keys):
        return None, "Preview runs queries on `sql:` connectors."
    if not query:
        return None, "Enter a query to preview."
    try:
        connector = mrsm.get_connector(connector_keys)
    except Exception as e:
        return None, f"Connector '{connector_keys}' is not defined: {e}"
    if connector is None:
        return None, f"Connector '{connector_keys}' is not defined."
    flavor = str(getattr(connector, 'flavor', '') or '')
    pipe = mrsm.Pipe(
        connector_keys,
        PREVIEW_METRIC,
        instance=instance_keys,
        temporary=True,
        parameters={'fetch': {'definition': limit_query(query, flavor, limit)}},
    )
    try:
        df = pipe.fetch(begin=None, end=None, check_existing=False)
    except Exception as e:
        return None, str(e)
    if df is None or isinstance(df, bool):
        return None, "The query returned nothing."
    if not hasattr(df, 'columns'):
        try:
            import pandas as pd
            chunks = list(df)
            df = pd.concat(chunks) if chunks else None
        except Exception as e:
            return None, f"Unexpected result: {e}"
    if df is None:
        return None, "The query returned nothing."
    return df.head(limit), ''


def guess_datetime_column(df: Any) -> Optional[str]:
    """
    The preview's most likely datetime column: a datetime-typed column (preferring common
    names), else a column with a datetime-like name whose values parse as datetimes.
    """
    if df is None or not hasattr(df, 'columns'):
        return None
    try:
        import pandas as pd
    except ImportError:
        return None
    columns = [str(col) for col in df.columns]

    def rank(name: str) -> Tuple[int, int]:
        lower = name.lower()
        if lower in DATETIME_NAMES:
            return (0, DATETIME_NAMES.index(lower))
        if lower.endswith(DATETIME_SUFFIXES):
            return (1, 0)
        return (2, 0)

    typed = [col for col in columns if pd.api.types.is_datetime64_any_dtype(df[col])]
    if typed:
        return sorted(typed, key=rank)[0]
    named = sorted((col for col in columns if rank(col)[0] < 2), key=rank)
    for col in named:
        values = df[col].dropna()
        if len(values) == 0 or pd.api.types.is_numeric_dtype(values) or pd.api.types.is_bool_dtype(values):
            continue
        text = values.astype(str)
        try:
            ### Text timestamps may mix formats (with and without an offset).
            parsed = pd.to_datetime(text, errors='coerce', utc=True, format='mixed')
        except (TypeError, ValueError):
            try:
                parsed = pd.to_datetime(text, errors='coerce', utc=True)
            except Exception:
                continue
        except Exception:
            continue
        if parsed.notna().all():
            return col
    return None


def preview_table(df: Any) -> Any:
    """The preview's rows as a table (values as text)."""
    if df is None or len(df.columns) == 0:
        return html.P('No columns.')
    header = html.Thead(html.Tr([
        html.Th([html.Code(str(col)), html.Span(str(df[col].dtype), className='new-pipe-dtype')])
        for col in df.columns
    ]))
    body = html.Tbody([
        html.Tr([html.Td('' if value is None else str(value)) for value in row])
        for row in df.astype(object).where(df.notna(), None).itertuples(index=False, name=None)
    ]) if len(df) else html.Tbody(html.Tr(html.Td('No rows.', colSpan=len(df.columns))))
    return html.Div(
        html.Table([header, body], className='table table-sm pipes-preview-table'),
        className='pipes-table-scroll',
    )


# ------------------------------------------------------------------------------------------------
# Register
# ------------------------------------------------------------------------------------------------

def parse_parameters_text(text: Optional[str]) -> Tuple[Optional[Dict[str, Any]], str]:
    """`(parameters, message)` from the Parameters editor (JSON or YAML; empty is `{}`)."""
    text = str(text or '').strip()
    if not text:
        return {}, ''
    try:
        if text.startswith('{'):
            value = json.loads(text)
        else:
            from meerschaum.utils.yaml import yaml
            value = yaml.load(text)
    except Exception as e:
        return None, f"The parameters are not valid JSON or YAML: {e}"
    if value is None:
        return {}, ''
    if not isinstance(value, dict):
        return None, "The parameters must be a dictionary."
    return value, ''


def build_parameters(
    connector_keys: str,
    query: Optional[str] = None,
    datetime_column: Optional[str] = None,
    id_column: Optional[str] = None,
    parameters_text: Optional[str] = None,
) -> Tuple[Optional[Dict[str, Any]], str]:
    """
    The new pipe's parameters: the Parameters editor's dictionary, the query as
    `fetch:definition` for `sql:` connectors, and the chosen columns.
    """
    parameters, message = parse_parameters_text(parameters_text)
    if parameters is None:
        return None, message
    if is_sql_connector(connector_keys):
        query = _strip_query(query or '')
        if query:
            fetch = parameters.get('fetch', None)
            fetch = dict(fetch) if isinstance(fetch, dict) else {}
            fetch['definition'] = query
            parameters['fetch'] = fetch
    columns = parameters.get('columns', None)
    columns = dict(columns) if isinstance(columns, dict) else {}
    for role, column in (('datetime', datetime_column), ('id', id_column)):
        column = str(column or '').strip()
        if column and column != 'None':
            columns[role] = column
    if columns:
        parameters['columns'] = columns
    return parameters, ''


def validate_keys(
    connector_keys: Optional[str],
    metric_key: Optional[str],
    location_key: Optional[str],
) -> Tuple[Optional[Tuple[str, str, Optional[str]]], Dict[str, str]]:
    """`((connector, metric, location), errors)`; `errors` maps a field to what is wrong."""
    connector_keys = str(connector_keys or '').strip()
    metric_key = str(metric_key or '').strip()
    location_key = str(location_key or '').strip() or None
    if location_key in ('None', '[None]'):
        location_key = None
    errors = {}
    if not connector_keys:
        errors['connector'] = 'Enter the connector.'
    if not metric_key:
        errors['metric'] = 'Enter the metric.'
    for field, value in (('connector', connector_keys), ('metric', metric_key), ('location', location_key)):
        if value and ('|' in value or '/' in value):
            errors[field] = "Keys cannot contain '|' or '/'."
    if errors:
        return None, errors
    return (connector_keys, metric_key, location_key), {}


def register_pipe(
    connector_keys: str,
    metric_key: str,
    location_key: Optional[str],
    instance_keys: str,
    parameters: Dict[str, Any],
) -> Tuple[mrsm.SuccessTuple, mrsm.Pipe]:
    """Register the new pipe (refused when it is already registered)."""
    pipe = mrsm.Pipe(connector_keys, metric_key, location_key, instance=instance_keys)
    try:
        registered = pipe.get_id() is not None
    except Exception as e:
        return (False, f"Could not read {instance_keys}: {e}"), pipe
    if registered:
        return (False, f"{pipe} is already registered."), pipe
    pipe = mrsm.Pipe(connector_keys, metric_key, location_key, instance=instance_keys,
                     parameters=parameters)
    try:
        success, message = pipe.register()
    except Exception as e:
        return (False, f"Failed to register {pipe}: {e}"), pipe
    return (success, message), pipe
