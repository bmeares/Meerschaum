#! /usr/bin/env python3
# -*- coding: utf-8 -*-
# vim:fenc=utf-8

"""
Handle MCP JSON-RPC 2.0 messages, independent of transport.

Both the HTTP route (`meerschaum.api.routes._mcp`) and the stdio server
(`meerschaum.mcp._stdio`) call `handle_message()`. The only thing that differs
between them is the caller's scopes: over HTTP those come from the bearer token
or session, while stdio runs with `['*']` because it inherits the privileges of
the shell that launched it — the same trust model as the CLI.
"""

from __future__ import annotations

import json

import meerschaum as mrsm
from meerschaum.mcp._primer import MRSM_PRIMER
from meerschaum.utils.typing import Any, Dict, List, Optional, Union

### The protocol version we advertise. Older versions a client may request are
### echoed back when we can still speak them.
LATEST_PROTOCOL_VERSION: str = '2025-06-18'
SUPPORTED_PROTOCOL_VERSIONS: List[str] = [
    '2025-06-18',
    '2025-03-26',
    '2024-11-05',
]

SERVER_NAME: str = 'meerschaum'

### JSON-RPC 2.0 error codes.
PARSE_ERROR: int = -32700
INVALID_REQUEST: int = -32600
METHOD_NOT_FOUND: int = -32601
INVALID_PARAMS: int = -32602
INTERNAL_ERROR: int = -32603


def has_scope(current_scopes: List[str], required_scopes: List[str]) -> bool:
    """
    Return whether `current_scopes` satisfies every scope in `required_scopes`.
    """
    if not required_scopes:
        return True
    if '*' in current_scopes:
        return True

    return all(scope in current_scopes for scope in required_scopes)


def jsonrpc_result(request_id: Any, result: Any) -> Dict[str, Any]:
    """
    Return a JSON-RPC 2.0 success envelope.
    """
    return {'jsonrpc': '2.0', 'id': request_id, 'result': result}


def jsonrpc_error(
    request_id: Any,
    code: int,
    message: str,
    data: Optional[Any] = None,
) -> Dict[str, Any]:
    """
    Return a JSON-RPC 2.0 error envelope.
    """
    error: Dict[str, Any] = {'code': code, 'message': message}
    if data is not None:
        error['data'] = data

    return {'jsonrpc': '2.0', 'id': request_id, 'error': error}


def _tool_error(request_id: Any, message: str) -> Dict[str, Any]:
    """
    Return a tool result flagged as an error.

    Tool failures are reported in the result (not as a JSON-RPC error) so the
    model can see what went wrong and adjust, per the MCP spec.
    """
    return jsonrpc_result(request_id, {
        'content': [{'type': 'text', 'text': message}],
        'isError': True,
    })


def get_visible_tools(current_scopes: List[str]) -> Dict[str, Any]:
    """
    Return the tools the caller may actually call, keyed by name.

    Tools are hidden rather than advertised-then-refused: a model that cannot
    see a tool will not waste a turn calling it.
    """
    from meerschaum.mcp import is_read_only_server
    from meerschaum.mcp._registry import get_tools

    read_only_server = is_read_only_server()
    return {
        name: tool
        for name, tool in get_tools().items()
        if has_scope(current_scopes, tool.scopes)
        and (tool.read_only or not read_only_server)
    }


def get_visible_resources(current_scopes: List[str]) -> List[Any]:
    """
    Return the resources and templates the caller may read.
    """
    from meerschaum.mcp._registry import get_all_resources
    return [
        resource for resource in get_all_resources()
        if has_scope(current_scopes, resource.scopes)
    ]


def get_visible_prompts(current_scopes: List[str]) -> Dict[str, Any]:
    """
    Return the prompts the caller may fetch.
    """
    from meerschaum.mcp import is_read_only_server
    from meerschaum.mcp._registry import get_prompts

    read_only_server = is_read_only_server()
    return {
        name: prompt
        for name, prompt in get_prompts().items()
        if has_scope(current_scopes, prompt.scopes)
        ### A prompt which would drive write tools is useless on a read-only server.
        and not (read_only_server and any(
            scope.endswith((':write', ':delete', ':drop', ':execute'))
            for scope in prompt.scopes
        ))
    }


def _handle_initialize(params: Dict[str, Any], current_scopes: List[str]) -> Dict[str, Any]:
    """
    Return the `initialize` result, negotiating the protocol version.
    """
    requested_version = params.get('protocolVersion', LATEST_PROTOCOL_VERSION)
    protocol_version = (
        requested_version
        if requested_version in SUPPORTED_PROTOCOL_VERSIONS
        else LATEST_PROTOCOL_VERSION
    )
    return {
        'protocolVersion': protocol_version,
        'capabilities': {
            'tools': {'listChanged': False},
            'resources': {'listChanged': False, 'subscribe': False},
            'prompts': {'listChanged': False},
        },
        'serverInfo': {
            'name': SERVER_NAME,
            'title': 'Meerschaum',
            'version': mrsm.__version__,
        },
        'instructions': MRSM_PRIMER,
    }


def _call_tool(
    params: Dict[str, Any],
    current_scopes: List[str],
    request_id: Any,
) -> Dict[str, Any]:
    """
    Dispatch a `tools/call` request.
    """
    from meerschaum.mcp import is_read_only_server
    from meerschaum.mcp._registry import get_tools
    from meerschaum.utils.dtypes import json_serialize_value

    tool_name = params.get('name', '')
    arguments = params.get('arguments') or {}

    all_tools = get_tools()
    tool = all_tools.get(tool_name)
    if tool is None:
        return _tool_error(request_id, f"Unknown tool: '{tool_name}'.")

    if not has_scope(current_scopes, tool.scopes):
        return _tool_error(
            request_id,
            (
                f"Missing required scope for '{tool_name}': "
                f"{', '.join(tool.scopes)}."
            ),
        )

    if is_read_only_server() and not tool.read_only:
        return _tool_error(
            request_id,
            (
                f"'{tool_name}' modifies data, and this MCP server is running in read-only "
                "mode."
            ),
        )

    if not isinstance(arguments, dict):
        return _tool_error(request_id, "Tool arguments must be an object.")

    for required_arg in tool.input_schema.get('required', []):
        if required_arg not in arguments:
            return _tool_error(
                request_id, f"Missing required argument '{required_arg}'."
            )

    try:
        result = tool.handler(**arguments)
    except TypeError as e:
        ### An unexpected keyword means the client sent an argument outside the schema.
        return _tool_error(request_id, f"Invalid arguments for '{tool_name}': {e}")
    except Exception as e:
        return _tool_error(request_id, f"{type(e).__name__}: {e}")

    text = json.dumps(result, default=json_serialize_value, indent=2)
    tool_result: Dict[str, Any] = {
        'content': [{'type': 'text', 'text': text}],
        'isError': False,
    }

    ### Only advertise `structuredContent` when the tool declared an output
    ### schema, since a client may validate one against the other.
    if tool.output_schema is not None and isinstance(result, dict):
        tool_result['structuredContent'] = json.loads(text)

    return jsonrpc_result(request_id, tool_result)


def _read_resource(
    params: Dict[str, Any],
    current_scopes: List[str],
    request_id: Any,
) -> Dict[str, Any]:
    """
    Dispatch a `resources/read` request.
    """
    uri = params.get('uri', '')
    if not uri:
        return jsonrpc_error(request_id, INVALID_PARAMS, "A resource 'uri' is required.")

    for resource in get_visible_resources(current_scopes):
        template_variables = resource.match(uri)
        if template_variables is None:
            continue

        try:
            text = resource.handler(**template_variables)
        except Exception as e:
            return jsonrpc_error(
                request_id, INTERNAL_ERROR, f"{type(e).__name__}: {e}"
            )

        return jsonrpc_result(request_id, {
            'contents': [{
                'uri': uri,
                'mimeType': resource.mime_type,
                'text': text,
            }],
        })

    return jsonrpc_error(request_id, INVALID_PARAMS, f"Resource not found: {uri}")


def _get_prompt(
    params: Dict[str, Any],
    current_scopes: List[str],
    request_id: Any,
) -> Dict[str, Any]:
    """
    Dispatch a `prompts/get` request.
    """
    prompt_name = params.get('name', '')
    arguments = params.get('arguments') or {}

    prompt = get_visible_prompts(current_scopes).get(prompt_name)
    if prompt is None:
        return jsonrpc_error(
            request_id, INVALID_PARAMS, f"Unknown prompt: '{prompt_name}'."
        )

    ### Validate against the declared arguments rather than the handler's
    ### signature: the handlers accept `**kwargs` so that adding an argument is
    ### not a breaking change, which would otherwise silently swallow typos.
    declared = {argument['name'] for argument in prompt.arguments}
    unknown = set(arguments) - declared
    if unknown:
        return jsonrpc_error(
            request_id,
            INVALID_PARAMS,
            (
                f"Unknown argument(s) for '{prompt_name}': "
                f"{', '.join(sorted(unknown))}. "
                f"Accepted: {', '.join(sorted(declared)) or 'none'}."
            ),
        )

    missing = [
        argument['name'] for argument in prompt.arguments
        if argument.get('required') and not arguments.get(argument['name'])
    ]
    if missing:
        return jsonrpc_error(
            request_id,
            INVALID_PARAMS,
            f"Missing required argument(s) for '{prompt_name}': {', '.join(missing)}.",
        )

    try:
        messages = prompt.handler(**arguments)
    except TypeError as e:
        return jsonrpc_error(
            request_id, INVALID_PARAMS, f"Invalid arguments for '{prompt_name}': {e}"
        )
    except Exception as e:
        return jsonrpc_error(request_id, INTERNAL_ERROR, f"{type(e).__name__}: {e}")

    return jsonrpc_result(request_id, {
        'description': prompt.description,
        'messages': messages,
    })


def handle_message(
    message: Dict[str, Any],
    current_scopes: Optional[List[str]] = None,
) -> Optional[Dict[str, Any]]:
    """
    Handle a single MCP JSON-RPC message.

    Parameters
    ----------
    message: Dict[str, Any]
        The parsed JSON-RPC request or notification.

    current_scopes: Optional[List[str]], default None
        The scopes granted to the caller. `['*']` grants everything.
        Defaults to no scopes, i.e. only unscoped tools and resources.

    Returns
    -------
    The JSON-RPC response, or `None` for a notification (which takes no reply).
    """
    from meerschaum.mcp._registry import paginate

    current_scopes = current_scopes if current_scopes is not None else []

    if not isinstance(message, dict):
        return jsonrpc_error(None, INVALID_REQUEST, "Request must be a JSON object.")

    request_id = message.get('id', None)
    method = message.get('method', '')
    params = message.get('params') or {}
    if not isinstance(params, dict):
        return jsonrpc_error(request_id, INVALID_PARAMS, "'params' must be an object.")

    ### Notifications carry no `id` and get no response — a client which receives
    ### an unsolicited response (`"id": null`) may treat it as a protocol error.
    if 'id' not in message or method.startswith('notifications/'):
        return None

    if method == 'initialize':
        return jsonrpc_result(request_id, _handle_initialize(params, current_scopes))

    if method == 'ping':
        return jsonrpc_result(request_id, {})

    if method == 'tools/list':
        try:
            page, next_cursor = paginate(
                sorted(get_visible_tools(current_scopes).values(), key=lambda t: t.name),
                params.get('cursor'),
            )
        except ValueError as e:
            return jsonrpc_error(request_id, INVALID_PARAMS, str(e))

        result: Dict[str, Any] = {'tools': [tool.to_dict() for tool in page]}
        if next_cursor:
            result['nextCursor'] = next_cursor
        return jsonrpc_result(request_id, result)

    if method == 'tools/call':
        return _call_tool(params, current_scopes, request_id)

    if method == 'resources/list':
        resources = [
            res for res in get_visible_resources(current_scopes) if not res.is_template
        ]
        return jsonrpc_result(request_id, {
            'resources': [res.to_dict() for res in resources],
        })

    if method == 'resources/templates/list':
        templates = [
            res for res in get_visible_resources(current_scopes) if res.is_template
        ]
        return jsonrpc_result(request_id, {
            'resourceTemplates': [res.to_dict() for res in templates],
        })

    if method == 'resources/read':
        return _read_resource(params, current_scopes, request_id)

    if method == 'prompts/list':
        prompts = sorted(
            get_visible_prompts(current_scopes).values(), key=lambda p: p.name
        )
        return jsonrpc_result(request_id, {
            'prompts': [prompt.to_dict() for prompt in prompts],
        })

    if method == 'prompts/get':
        return _get_prompt(params, current_scopes, request_id)

    return jsonrpc_error(request_id, METHOD_NOT_FOUND, f"Method not found: {method}")


def handle_payload(
    payload: Union[Dict[str, Any], List[Any]],
    current_scopes: Optional[List[str]] = None,
) -> Union[Dict[str, Any], List[Dict[str, Any]], None]:
    """
    Handle a single message or a JSON-RPC batch.

    Returns `None` when nothing needs to be sent back (i.e. the payload was
    entirely notifications).
    """
    if isinstance(payload, list):
        responses = [
            response for response in (
                handle_message(message, current_scopes) for message in payload
            )
            if response is not None
        ]
        return responses or None

    return handle_message(payload, current_scopes)
