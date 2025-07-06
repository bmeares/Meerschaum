#! /usr/bin/env python3
# vim:fenc=utf-8

"""
Define utilities for working with pipes.
"""

from __future__ import annotations

from typing import Any, Dict, Callable
import re
import json
import ast
import copy
import uuid

from meerschaum.utils.typing import PipesDict, Optional, Any
import meerschaum as mrsm


def evaluate_pipe_access_chain(access_chain: str, pipe: mrsm.Pipe):
    """
    Safely evaluate the access chain on a Pipe.
    """
    expr = f"pipe{access_chain}"
    tree = ast.parse(expr, mode='eval')

    def _eval(node, context):
        if isinstance(node, ast.Expression):
            return _eval(node.body, context)

        elif isinstance(node, ast.Name):
            if node.id == "pipe":
                return context
            raise ValueError(f"Unknown variable: {node.id}")

        elif isinstance(node, ast.Attribute):
            value = _eval(node.value, context)
            return getattr(value, node.attr)

        elif isinstance(node, ast.Subscript):
            value = _eval(node.value, context)
            key = _eval(node.slice, context) if isinstance(node.slice, ast.Index) else _eval(node.slice, context)
            return value[key]

        elif isinstance(node, ast.Constant):  # Python 3.8+
            return node.value

        elif isinstance(node, ast.Str):  # Older Python
            return node.s

        elif isinstance(node, ast.Index):  # Older Python AST style
            return _eval(node.value, context)

        else:
            raise TypeError(f"Unsupported AST node: {ast.dump(node)}")

    return _eval(tree, pipe)



def _evaluate_pipe_access_chain_from_match(pipe_match: re.Match) -> Any:
    """
    Helper function to evaluate a pipe from a regex match object.
    """
    from meerschaum.utils.warnings import warn
    from meerschaum.utils.misc import parse_arguments_str
    from meerschaum.utils.sql import sql_item_name
    try:
        args_str = pipe_match.group(1)
        access_chain = pipe_match.group(2)
        args, kwargs = parse_arguments_str(args_str)
        pipe = mrsm.Pipe(*args, **kwargs)
    except Exception as e:
        warn(f"Failed to parse pipe from template string:\n{e}")
        raise e

    if not access_chain:
        target = pipe.target
        schema = (
            pipe.instance_connector.get_pipe_schema(pipe)
            if hasattr(pipe.instance_connector, 'get_pipe_schema')
            else None
        )
        return (
            sql_item_name(target, pipe.instance_connector.flavor, schema)
            if pipe.instance_connector.type == 'sql'
            else pipe.target
        )

    return evaluate_pipe_access_chain(access_chain, pipe)


def replace_pipes_syntax(text: str) -> Any:
    """
    Parse a string containing the `{{ Pipe() }}` syntax.
    """
    from meerschaum.utils.warnings import warn
    from meerschaum.utils.sql import sql_item_name
    from meerschaum.utils.dtypes import json_serialize_value
    from meerschaum.utils.misc import parse_arguments_str
    pattern = r'\{\{\s*(?:mrsm\.)?Pipe\((.*?)\)((?:\.[\w]+|\[[^\]]+\])*)\s*\}\}'

    matches = list(re.finditer(pattern, text))
    if not matches:
        return text

    placeholders = {}
    for match in matches:
        placeholder = f"__mrsm_pipe_placeholder_{uuid.uuid4().hex}__"
        placeholders[placeholder] = match

    substituted_text = text
    for placeholder, match in placeholders.items():
        substituted_text = substituted_text.replace(match.group(0), placeholder)

    resolved_values = {}
    for placeholder, match in placeholders.items():
        try:
            resolved_values[placeholder] = _evaluate_pipe_access_chain_from_match(match)
        except Exception as e:
            warn(f"Failed to resolve pipe syntax '{match.group(0)}': {e}")
            resolved_values[placeholder] = match.group(0)

    if len(matches) == 1:
        match = matches[0]
        placeholder = list(placeholders.keys())[0]
        if text.strip() == match.group(0):
            return resolved_values[placeholder]

    final_text = substituted_text
    for placeholder, value in resolved_values.items():
        if isinstance(value, (dict, list, bool, int, float)) or value is None:
            final_text = final_text.replace(placeholder, json.dumps(value, default=json_serialize_value))
        else:
            final_text = final_text.replace(placeholder, str(value))

    return final_text


def replace_pipes_in_dict(
    pipes: Optional[PipesDict] = None,
    func: Callable[[Any], Any] = str,
    debug: bool = False,
    **kw
) -> PipesDict:
    """
    Replace the Pipes in a Pipes dict with the result of another function.

    Parameters
    ----------
    pipes: Optional[PipesDict], default None
        The pipes dict to be processed.

    func: Callable[[Any], Any], default str
        The function to be applied to every pipe.
        Defaults to the string constructor.

    debug: bool, default False
        Verbosity toggle.
    

    Returns
    -------
    A dictionary where every pipe is replaced with the output of a function.

    """
    def change_dict(d : Dict[Any, Any], func : 'function') -> None:
        for k, v in d.items():
            if isinstance(v, dict):
                change_dict(v, func)
            else:
                d[k] = func(v)

    if pipes is None:
        from meerschaum import get_pipes
        pipes = get_pipes(debug=debug, **kw)

    result = copy.deepcopy(pipes)
    change_dict(result, func)
    return result
