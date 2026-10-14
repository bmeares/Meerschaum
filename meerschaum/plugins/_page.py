#! /usr/bin/env python3
# -*- coding: utf-8 -*-
# vim:fenc=utf-8

"""
Helpers for the v5 **page contract** (design §8.3): a plugin page is a function that returns an
HTML fragment, and a chart in that fragment is Plotly figure JSON carried by a
`data-mrsm-figure` attribute. The console renders every such element with its vendored Plotly.js,
on page load and after each HTMX swap.

The figure JSON, not a library, is the charting contract (D19): a Rust plugin built with the
`plotly` crate and a Python plugin built with `plotly.py` emit the same JSON, so a page can move
between them without touching a chart.
"""

from __future__ import annotations

import json
import html
from typing import Any, Dict, Optional, Union


def figure_json(fig: Union[Dict[str, Any], str, Any]) -> str:
    """
    Return the compact Plotly figure JSON for `fig`.

    Parameters
    ----------
    fig: Union[Dict[str, Any], str, plotly.graph_objects.Figure]
        A `plotly.py` figure (anything with `to_json()` or `to_plotly_json()`),
        a figure `dict` (`{'data': [...], 'layout': {...}}`) or an already serialized JSON string.

    Returns
    -------
    A JSON string with no insignificant whitespace, `{"data":[...],"layout":{...}}`.
    Key order is the order the figure produced, so the same figure always serializes to the same
    bytes.
    """
    if isinstance(fig, str):
        document = json.loads(fig)
    elif hasattr(fig, 'to_json'):
        document = json.loads(fig.to_json())
    elif hasattr(fig, 'to_plotly_json'):
        document = fig.to_plotly_json()
    elif isinstance(fig, dict):
        document = fig
    else:
        raise TypeError(
            "A figure must be a plotly.py figure, a dict or a JSON string, "
            f"not '{type(fig).__name__}'."
        )

    if not isinstance(document, dict):
        raise ValueError("A Plotly figure is a JSON object with `data` and `layout`.")

    encoder = _get_encoder()
    return json.dumps(
        document,
        separators=(',', ':'),
        cls=encoder,
        allow_nan=False if encoder is None else True,
    )


def _get_encoder() -> Optional[type]:
    """
    Return plotly's JSON encoder if it is installed (it understands numpy, pandas and datetimes
    inside figure dictionaries), else `None`.
    """
    try:
        from plotly.utils import PlotlyJSONEncoder
    except ImportError:
        return None
    return PlotlyJSONEncoder


def figure(
    fig: Union[Dict[str, Any], str, Any],
    id: Optional[str] = None,
    aria_label: Optional[str] = None,
) -> str:
    """
    Embed a Plotly figure in a page-contract HTML fragment.

    Parameters
    ----------
    fig: Union[Dict[str, Any], str, plotly.graph_objects.Figure]
        The figure (see `figure_json()`).

    id: Optional[str], default None
        An optional element ID, so the page can target the chart with HTMX.

    aria_label: Optional[str], default None
        An optional accessible name for the chart.

    Returns
    -------
    A string like `<div data-mrsm-figure="{...figure JSON...}"></div>`, with the JSON
    attribute-escaped. The console reads the attribute back as the same JSON that
    `figure_json(fig)` returns.

    Examples
    --------
    >>> import plotly.graph_objects as go
    >>> from meerschaum.plugins import figure, web_page
    >>>
    >>> @web_page('counts', login_required=True, page_group='Acme')
    >>> def counts_page():
    ...     fig = go.Figure(go.Bar(x=['a', 'b'], y=[1, 2]))
    ...     return f"<section><h2>Counts</h2>{figure(fig)}</section>"
    """
    attributes = []
    if id is not None:
        attributes.append(f' id="{html.escape(str(id), quote=True)}"')
    if aria_label is not None:
        attributes.append(f' role="img" aria-label="{html.escape(str(aria_label), quote=True)}"')
    escaped = html.escape(figure_json(fig), quote=True)
    return f'<div{"".join(attributes)} data-mrsm-figure="{escaped}"></div>'
