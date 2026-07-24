#! /usr/bin/env python3
# -*- coding: utf-8 -*-
# vim:fenc=utf-8

"""
Generate `docs/zensical/reference/mcp/tools.md` from the MCP registry.

Run this after adding or changing a tool, resource, or prompt so the published
reference cannot drift from the code:

    python scripts/generate_mcp_docs.py

`scripts/build.sh` runs it as part of the docs build. Pass `--check` to fail
instead of writing, for CI.
"""

import pathlib
import sys

REPO_ROOT = pathlib.Path(__file__).resolve().parent.parent
sys.path.insert(0, REPO_ROOT.as_posix())

OUTPUT_PATH = REPO_ROOT / 'docs' / 'zensical' / 'reference' / 'mcp' / 'tools.md'

HEADER = """\
<!--
    GENERATED FILE — DO NOT EDIT.
    Regenerate with `python scripts/generate_mcp_docs.py` after changing
    `meerschaum/mcp/_tools.py`, `_resources.py`, or `_prompts.py`.
-->

# MCP Tool Reference

Every tool, resource, and prompt the [MCP server](index.md) exposes, generated
from the registry in `meerschaum.mcp`.

Over HTTP each tool requires an OAuth2 scope, and tools your token does not
cover are hidden from `tools/list` rather than refused at call time. The stdio
transport grants everything, because it runs as the user who launched it.

"""

FOOTER = """
## Annotations

Every tool carries the MCP annotations a client uses to decide whether to prompt
before calling:

| Annotation | Meaning |
| --- | --- |
| `readOnlyHint` | The tool never modifies anything. |
| `destructiveHint` | The tool deletes or overwrites data. |
| `idempotentHint` | Calling it twice with the same arguments has the same effect as once. |
| `openWorldHint` | The tool can reach beyond this Meerschaum instance. |

## Pagination

`list_pipes`, `get_pipe_data`, and `read_sql` return a `next_cursor`. Pass it
back as `cursor` for the next page; a `null` cursor means there is nothing more.

On a pipe with a datetime axis, narrowing with `begin` and `end` is far cheaper
than paging — cursors re-read the rows they skip.
"""


def render() -> str:
    """
    Return the full Markdown page.
    """
    from meerschaum.mcp._docs import (
        render_tools_table,
        render_resources_table,
        render_prompts_table,
        render_scopes_table,
    )
    from meerschaum.mcp._registry import get_tools

    sections = [
        HEADER,
        '## Tools',
        '',
        render_tools_table(),
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
        render_scopes_table(),
        '',
        '## Tool details',
        '',
    ]

    for name, tool in sorted(get_tools().items()):
        sections.append(f'### `{name}`')
        sections.append('')
        sections.append(tool.description)
        sections.append('')
        scopes = ', '.join(f'`{scope}`' for scope in tool.scopes) or 'none'
        flags = [
            flag for flag, enabled in (
                ('read-only', tool.read_only),
                ('destructive', tool.destructive),
                ('idempotent', tool.idempotent),
                ('open-world', tool.open_world),
            ) if enabled
        ]
        sections.append(f'**Scope:** {scopes}')
        sections.append('')
        if flags:
            sections.append(f'**Annotations:** {", ".join(flags)}')
            sections.append('')

        properties = tool.input_schema.get('properties', {})
        required = set(tool.input_schema.get('required', []))
        if properties:
            sections.append('| Argument | Type | Required | Description |')
            sections.append('| --- | --- | :-: | --- |')
            for arg_name, schema in properties.items():
                arg_type = schema.get('type', 'any')
                if isinstance(arg_type, list):
                    arg_type = ' \\| '.join(arg_type)
                if schema.get('enum'):
                    arg_type = ' \\| '.join(f'`{val}`' for val in schema['enum'])
                description = schema.get('description', '').replace('\n', ' ')
                is_required = '✅' if arg_name in required else ''
                sections.append(
                    f'| `{arg_name}` | {arg_type} | {is_required} | {description} |'
                )
            sections.append('')

    sections.append(FOOTER)
    return '\n'.join(sections)


def main() -> int:
    """
    Write (or check) the generated reference.
    """
    content = render()
    check_only = '--check' in sys.argv

    if check_only:
        existing = OUTPUT_PATH.read_text(encoding='utf-8') if OUTPUT_PATH.exists() else ''
        if existing != content:
            print(
                f"{OUTPUT_PATH.relative_to(REPO_ROOT)} is out of date. "
                "Run `python scripts/generate_mcp_docs.py`.",
                file=sys.stderr,
            )
            return 1
        print(f"{OUTPUT_PATH.relative_to(REPO_ROOT)} is up to date.")
        return 0

    OUTPUT_PATH.parent.mkdir(parents=True, exist_ok=True)
    OUTPUT_PATH.write_text(content, encoding='utf-8')
    print(f"Wrote {OUTPUT_PATH.relative_to(REPO_ROOT)}.")
    return 0


if __name__ == '__main__':
    sys.exit(main())
