#! /usr/bin/env python3
# -*- coding: utf-8 -*-
# vim:fenc=utf-8

"""
Render the MCP reference from the registry.

One renderer feeds two consumers: the `mrsm://docs` resource (so a client can
read the scope matrix without network access) and the published documentation
page (so it cannot drift from the code). Nothing here is hand-maintained.
"""

from __future__ import annotations

import re

from meerschaum.utils.typing import Dict, List

### Abbreviations whose trailing period does not end a sentence.
_ABBREVIATIONS = ('e.g', 'i.e', 'etc', 'vs', 'cf')


def _first_sentence(text: str) -> str:
    """
    Return the first sentence of `text`, not tripping over abbreviations.
    """
    collapsed = ' '.join(text.split())
    for match in re.finditer(r'\.\s', collapsed):
        head = collapsed[:match.start()]
        if head.endswith(_ABBREVIATIONS):
            continue
        return head + '.'

    return collapsed if collapsed.endswith('.') else collapsed + '.'


def render_tools_table() -> str:
    """
    Return a Markdown table of every tool, its required scopes, and its
    behavioral annotations.
    """
    from meerschaum.mcp._registry import get_tools

    lines = [
        '| Tool | Scope | Read-only | Destructive | Description |',
        '| --- | --- | :-: | :-: | --- |',
    ]
    for name, tool in sorted(get_tools().items()):
        scopes = ', '.join(f'`{scope}`' for scope in tool.scopes) or '—'
        read_only = '✅' if tool.read_only else ''
        destructive = '⚠️' if tool.destructive else ''
        description = _first_sentence(tool.description)
        lines.append(
            f"| `{name}` | {scopes} | {read_only} | {destructive} | {description} |"
        )

    return '\n'.join(lines)


def render_resources_table() -> str:
    """
    Return a Markdown table of every resource and resource template.
    """
    from meerschaum.mcp._registry import get_all_resources

    lines = [
        '| URI | Scope | Description |',
        '| --- | --- | --- |',
    ]
    for resource in get_all_resources():
        uri = resource.uri_template or resource.uri
        scopes = ', '.join(f'`{scope}`' for scope in resource.scopes) or 'none'
        description = _first_sentence(resource.description)
        lines.append(f"| `{uri}` | {scopes} | {description} |")

    return '\n'.join(lines)


def render_prompts_table() -> str:
    """
    Return a Markdown table of every prompt and its arguments.
    """
    from meerschaum.mcp._registry import get_prompts

    lines = [
        '| Prompt | Arguments | Description |',
        '| --- | --- | --- |',
    ]
    for name, prompt in sorted(get_prompts().items()):
        arguments = ', '.join(
            f"`{arg['name']}`" + ('' if arg.get('required') else ' *(optional)*')
            for arg in prompt.arguments
        ) or '—'
        description = _first_sentence(prompt.description)
        lines.append(f"| `{name}` | {arguments} | {description} |")

    return '\n'.join(lines)


def render_scopes_table() -> str:
    """
    Return a Markdown table mapping each scope to the tools it unlocks.
    """
    from meerschaum._internal.static import STATIC_CONFIG
    from meerschaum.mcp._registry import get_tools

    scope_descriptions = STATIC_CONFIG['tokens']['scopes']
    tools_by_scope: Dict[str, List[str]] = {}
    for name, tool in get_tools().items():
        for scope in (tool.scopes or ['(none)']):
            tools_by_scope.setdefault(scope, []).append(name)

    lines = [
        '| Scope | Unlocks | Meaning |',
        '| --- | --- | --- |',
    ]
    for scope in sorted(tools_by_scope):
        tools = ', '.join(f'`{name}`' for name in sorted(tools_by_scope[scope]))
        meaning = scope_descriptions.get(scope, '')
        lines.append(f"| `{scope}` | {tools} | {meaning} |")

    return '\n'.join(lines)


def render_reference() -> str:
    """
    Return the complete Markdown reference served as the `mrsm://docs` resource.
    """
    from meerschaum.mcp._primer import MRSM_PRIMER
    from meerschaum.mcp._security import get_action_denylist, get_action_allowlist
    from meerschaum.mcp import is_read_only_server

    denylist = get_action_denylist()
    allowlist = get_action_allowlist()
    action_notes = []
    if allowlist:
        action_notes.append(
            "`execute_action` is restricted to an allowlist: "
            + ', '.join(f'`{name}`' for name in sorted(allowlist)) + '.'
        )
    if denylist:
        action_notes.append(
            "These actions are blocked because they execute arbitrary code on the API host: "
            + ', '.join(f'`{name}`' for name in sorted(denylist)) + '.'
        )
    if is_read_only_server():
        action_notes.append(
            "**This server is running in read-only mode**, so only read-only tools are exposed "
            "regardless of your token's scopes."
        )

    sections = [
        '# Meerschaum MCP reference',
        '',
        '## Data model',
        '',
        MRSM_PRIMER,
        '',
        '## Tools',
        '',
        render_tools_table(),
        '',
        (
            'Tools marked ⚠️ delete or overwrite data. Tools marked ✅ never modify anything. '
            'Every tool also carries these as MCP annotations (`readOnlyHint`, '
            '`destructiveHint`, `idempotentHint`), so a client can prompt before a '
            'destructive call.'
        ),
        '',
        '## Resources',
        '',
        render_resources_table(),
        '',
        '## Prompts',
        '',
        render_prompts_table(),
        '',
        '## Scopes',
        '',
        (
            'Over HTTP, each tool requires an OAuth2 scope on your token — the same scopes the '
            'REST routes enforce. A token with the `*` scope may call anything. Tools your token '
            'cannot call are hidden from `tools/list` rather than failing at call time.'
        ),
        '',
        render_scopes_table(),
        '',
        '## Limits',
        '',
        (
            '- `read_sql` accepts a single read-only statement. Writes, DDL, and stacked '
            'statements are refused. For untrusted callers, back the connector with a read-only '
            'database role as well.'
        ),
        (
            '- `execute_action` grants broad control over the instance and its host, which is '
            'what the `actions:execute` scope means.'
        ),
    ]
    sections.extend(f'- {note}' for note in action_notes)
    sections.extend([
        (
            '- List results are paginated. Pass a response\'s `next_cursor` back as `cursor` to '
            'continue; a null `next_cursor` means there is nothing more.'
        ),
        '',
    ])

    return '\n'.join(sections)
