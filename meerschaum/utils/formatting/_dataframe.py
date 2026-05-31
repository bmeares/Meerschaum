#! /usr/bin/env python
# -*- coding: utf-8 -*-
# vim:fenc=utf-8

"""
Print `pandas` DataFrames in a format that is friendly to both humans and LLMs.

The default `pandas` / `rich` representations truncate wide tables with `...`,
dropping columns and clipping cell values. This module renders the full table
(no column truncation) as a GitHub-flavored Markdown table, which is readable in
a terminal and parses cleanly for downstream tooling and language models.
"""

from __future__ import annotations

from meerschaum.utils.typing import Any


def format_dataframe(df: Any, max_rows: int | None = None) -> str:
    """
    Return a full, untruncated string representation of a DataFrame.

    Parameters
    ----------
    df: pandas.DataFrame
        The DataFrame to format.

    max_rows: int | None, default None
        If set, only render the first `max_rows` rows and append a note.
        `None` renders every row.

    Returns
    -------
    A Markdown table (falling back to a plain fixed-width table if `tabulate`
    is unavailable), followed by a `[rows x columns]` shape footer.
    """
    from meerschaum.utils.packages import attempt_import
    pd = attempt_import('pandas', lazy=False)

    n_rows, n_cols = df.shape
    render_df = df if (max_rows is None or n_rows <= max_rows) else df.head(max_rows)
    truncated_rows = n_rows - len(render_df)

    ### Never let pandas insert `...` for columns, rows, or cell contents.
    options = [
        'display.max_columns', None,
        'display.max_rows', None,
        'display.width', None,
        'display.max_colwidth', None,
    ]

    body = None
    with pd.option_context(*options):
        try:
            tabulate = attempt_import('tabulate', warn=False)
            if tabulate is not None:
                body = render_df.to_markdown(index=False)
        except Exception:
            body = None

        if body is None:
            ### Fall back to the built-in fixed-width table (no extra dependency).
            body = render_df.to_string(index=False, max_rows=None, max_cols=None)

    footer = (
        f"\n[{n_rows} row{'s' if n_rows != 1 else ''} "
        f"x {n_cols} column{'s' if n_cols != 1 else ''}]"
    )
    if truncated_rows > 0:
        footer = (
            f"\n[showing first {len(render_df)} of {n_rows} rows "
            f"x {n_cols} column{'s' if n_cols != 1 else ''}]"
        )
    return body + footer


def pprint_df(df: Any, max_rows: int | None = None) -> None:
    """
    Print a DataFrame in full (no column truncation), Markdown-formatted.

    See `format_dataframe` for details.
    """
    print(format_dataframe(df, max_rows=max_rows))
