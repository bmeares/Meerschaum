#! /usr/bin/env python3
# -*- coding: utf-8 -*-
# vim:fenc=utf-8

"""
Define the transport-agnostic MCP registry: tools, resources, and prompts.

Everything the MCP server exposes is declared here as data so that the HTTP
transport (`meerschaum.api.routes._mcp`), the stdio transport
(`meerschaum.mcp._stdio`), and the generated documentation all read from one
source. Adding a tool means adding one `@tool(...)` decorator — no transport,
route, or docs changes required.
"""

from __future__ import annotations

import base64
import json
import re

from meerschaum.utils.typing import (
    Any,
    Callable,
    Dict,
    List,
    Optional,
    Tuple,
)

__all__ = (
    'MCPTool',
    'MCPResource',
    'MCPPrompt',
    'tool',
    'resource',
    'prompt',
    'get_tools',
    'get_resources',
    'get_resource_templates',
    'get_prompts',
    'encode_cursor',
    'decode_cursor',
    'paginate',
)


class MCPTool:
    """
    A single MCP tool: its JSON Schema, the scopes required to call it, the
    behavioral annotations clients use to decide whether to prompt, and the
    handler which implements it.
    """

    def __init__(
        self,
        name: str,
        description: str,
        handler: Callable[..., Any],
        input_schema: Dict[str, Any],
        scopes: Optional[List[str]] = None,
        output_schema: Optional[Dict[str, Any]] = None,
        title: Optional[str] = None,
        read_only: bool = False,
        destructive: bool = False,
        idempotent: bool = False,
        open_world: bool = False,
    ):
        self.name = name
        self.description = description
        self.handler = handler
        self.input_schema = input_schema
        self.scopes = scopes or []
        self.output_schema = output_schema
        self.title = title
        self.read_only = read_only
        self.destructive = destructive
        self.idempotent = idempotent
        self.open_world = open_world

    @property
    def annotations(self) -> Dict[str, Any]:
        """
        Return the MCP tool annotations.

        Clients use `destructiveHint` to decide whether to ask the user before
        invoking, so it must be accurate.
        """
        annotations: Dict[str, Any] = {
            'readOnlyHint': self.read_only,
            'idempotentHint': self.idempotent,
            'openWorldHint': self.open_world,
        }
        if self.title:
            annotations['title'] = self.title

        ### Per the MCP spec, `destructiveHint` is only meaningful when the tool
        ### is not read-only.
        if not self.read_only:
            annotations['destructiveHint'] = self.destructive

        return annotations

    def to_dict(self) -> Dict[str, Any]:
        """
        Return the `tools/list` representation of this tool.
        """
        tool_dict: Dict[str, Any] = {
            'name': self.name,
            'description': self.description,
            'inputSchema': self.input_schema,
            'annotations': self.annotations,
        }
        if self.title:
            tool_dict['title'] = self.title
        if self.output_schema:
            tool_dict['outputSchema'] = self.output_schema

        return tool_dict

    def __repr__(self) -> str:
        return f"MCPTool('{self.name}')"


class MCPResource:
    """
    An MCP resource: a document addressed by URI rather than an operation.

    Set `uri_template` (RFC 6570) instead of `uri` to register a resource
    template, e.g. `mrsm://pipes/{connector_keys}/{metric_key}/{location_key}`.
    """

    def __init__(
        self,
        name: str,
        description: str,
        handler: Callable[..., Any],
        uri: Optional[str] = None,
        uri_template: Optional[str] = None,
        mime_type: str = 'application/json',
        scopes: Optional[List[str]] = None,
        title: Optional[str] = None,
    ):
        if not uri and not uri_template:
            raise ValueError("Either `uri` or `uri_template` is required.")

        self.name = name
        self.description = description
        self.handler = handler
        self.uri = uri
        self.uri_template = uri_template
        self.mime_type = mime_type
        self.scopes = scopes or []
        self.title = title

    @property
    def is_template(self) -> bool:
        """
        Return whether this resource is addressed by template.
        """
        return self.uri_template is not None

    @property
    def _pattern(self) -> 're.Pattern':
        """
        Return a regex which matches this resource's URI template and captures
        its variables.
        """
        template = self.uri_template or self.uri or ''
        parts = re.split(r'(\{[a-zA-Z_][a-zA-Z0-9_]*\})', template)
        regex = ''
        for part in parts:
            if part.startswith('{') and part.endswith('}'):
                regex += f"(?P<{part[1:-1]}>[^/]+)"
                continue
            regex += re.escape(part)

        return re.compile('^' + regex + '$')

    def match(self, uri: str) -> Optional[Dict[str, str]]:
        """
        If `uri` addresses this resource, return the captured template
        variables (an empty dict for a static URI). Otherwise return `None`.
        """
        match = self._pattern.match(uri)
        if match is None:
            return None

        return match.groupdict()

    def to_dict(self) -> Dict[str, Any]:
        """
        Return the `resources/list` or `resources/templates/list`
        representation of this resource.
        """
        resource_dict: Dict[str, Any] = {
            'name': self.name,
            'description': self.description,
            'mimeType': self.mime_type,
        }
        if self.title:
            resource_dict['title'] = self.title
        if self.is_template:
            resource_dict['uriTemplate'] = self.uri_template
        else:
            resource_dict['uri'] = self.uri

        return resource_dict

    def __repr__(self) -> str:
        return f"MCPResource('{self.uri or self.uri_template}')"


class MCPPrompt:
    """
    An MCP prompt: a reusable, parameterized workflow the client can surface to
    the user (e.g. as a slash command).
    """

    def __init__(
        self,
        name: str,
        description: str,
        handler: Callable[..., Any],
        arguments: Optional[List[Dict[str, Any]]] = None,
        title: Optional[str] = None,
        scopes: Optional[List[str]] = None,
    ):
        self.name = name
        self.description = description
        self.handler = handler
        self.arguments = arguments or []
        self.title = title
        self.scopes = scopes or []

    def to_dict(self) -> Dict[str, Any]:
        """
        Return the `prompts/list` representation of this prompt.
        """
        prompt_dict: Dict[str, Any] = {
            'name': self.name,
            'description': self.description,
            'arguments': self.arguments,
        }
        if self.title:
            prompt_dict['title'] = self.title

        return prompt_dict

    def __repr__(self) -> str:
        return f"MCPPrompt('{self.name}')"


### Cursors are offsets, and an offset is handed to the database as part of a row
### limit, so cap it: a hand-crafted cursor must not turn into `LIMIT 1000000001`.
MAX_CURSOR_OFFSET: int = 1_000_000

_TOOLS: Dict[str, MCPTool] = {}
_RESOURCES: List[MCPResource] = []
_PROMPTS: Dict[str, MCPPrompt] = {}


def tool(
    name: str,
    description: str,
    input_schema: Optional[Dict[str, Any]] = None,
    **kwargs: Any
) -> Callable[[Callable[..., Any]], Callable[..., Any]]:
    """
    Register a function as an MCP tool.

    Examples
    --------
    >>> @tool(
    ...     'get_pipe_attributes',
    ...     "Return a pipe's parameters.",
    ...     scopes=['pipes:read'],
    ...     read_only=True,
    ...     input_schema={'type': 'object', 'properties': {}},
    ... )
    ... def _get_pipe_attributes(**kw):
    ...     ...
    """
    def _decorator(func: Callable[..., Any]) -> Callable[..., Any]:
        if name in _TOOLS:
            raise ValueError(f"Duplicate MCP tool name: '{name}'.")

        _TOOLS[name] = MCPTool(
            name=name,
            description=description,
            handler=func,
            input_schema=(input_schema or {'type': 'object', 'properties': {}}),
            **kwargs
        )
        return func

    return _decorator


def resource(
    name: str,
    description: str,
    **kwargs: Any
) -> Callable[[Callable[..., Any]], Callable[..., Any]]:
    """
    Register a function as an MCP resource (or resource template).
    """
    def _decorator(func: Callable[..., Any]) -> Callable[..., Any]:
        _RESOURCES.append(
            MCPResource(name=name, description=description, handler=func, **kwargs)
        )
        return func

    return _decorator


def prompt(
    name: str,
    description: str,
    **kwargs: Any
) -> Callable[[Callable[..., Any]], Callable[..., Any]]:
    """
    Register a function as an MCP prompt.
    """
    def _decorator(func: Callable[..., Any]) -> Callable[..., Any]:
        if name in _PROMPTS:
            raise ValueError(f"Duplicate MCP prompt name: '{name}'.")

        _PROMPTS[name] = MCPPrompt(
            name=name, description=description, handler=func, **kwargs
        )
        return func

    return _decorator


def _ensure_loaded():
    """
    Import the modules which populate the registries.
    """
    import meerschaum.mcp._tools  # noqa: F401
    import meerschaum.mcp._resources  # noqa: F401
    import meerschaum.mcp._prompts  # noqa: F401


def get_tools() -> Dict[str, MCPTool]:
    """
    Return all registered tools, keyed by name.
    """
    _ensure_loaded()
    return _TOOLS


def get_resources() -> List[MCPResource]:
    """
    Return all registered static (non-template) resources.
    """
    _ensure_loaded()
    return [res for res in _RESOURCES if not res.is_template]


def get_resource_templates() -> List[MCPResource]:
    """
    Return all registered resource templates.
    """
    _ensure_loaded()
    return [res for res in _RESOURCES if res.is_template]


def get_all_resources() -> List[MCPResource]:
    """
    Return all registered resources, templates included.
    """
    _ensure_loaded()
    return list(_RESOURCES)


def get_prompts() -> Dict[str, MCPPrompt]:
    """
    Return all registered prompts, keyed by name.
    """
    _ensure_loaded()
    return _PROMPTS


def encode_cursor(offset: int) -> str:
    """
    Return an opaque pagination cursor for `offset`.
    """
    return base64.urlsafe_b64encode(
        json.dumps({'offset': offset}, separators=(',', ':')).encode('utf-8')
    ).decode('utf-8')


def decode_cursor(cursor: Optional[str]) -> int:
    """
    Return the offset encoded in `cursor`, or `0` if it is unset.

    Raises
    ------
    A `ValueError` if the cursor is malformed, per the MCP spec's
    "Invalid params" requirement for bad cursors.
    """
    if not cursor:
        return 0

    try:
        offset = json.loads(base64.urlsafe_b64decode(cursor.encode('utf-8')))['offset']
    except Exception as e:
        raise ValueError(f"Invalid cursor: {cursor}") from e

    if not isinstance(offset, int) or offset < 0 or offset > MAX_CURSOR_OFFSET:
        raise ValueError(f"Invalid cursor: {cursor}")

    return offset


def paginate(
    items: List[Any],
    cursor: Optional[str] = None,
    page_size: int = 100,
) -> Tuple[List[Any], Optional[str]]:
    """
    Return a page of `items` and the cursor for the following page.

    Parameters
    ----------
    items: List[Any]
        The complete list to page through.

    cursor: Optional[str], default None
        The cursor returned by a previous call, or `None` for the first page.

    page_size: int, default 100
        The maximum number of items to return.

    Returns
    -------
    A tuple of the page's items and the next cursor (`None` when exhausted).
    """
    offset = decode_cursor(cursor)
    page = items[offset:(offset + page_size)]
    next_offset = offset + len(page)
    next_cursor = encode_cursor(next_offset) if next_offset < len(items) else None
    return page, next_cursor
