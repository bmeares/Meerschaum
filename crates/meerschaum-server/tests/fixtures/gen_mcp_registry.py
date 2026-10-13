#! /usr/bin/env python3
# vim:fenc=utf-8

"""
Export v4's built-in MCP registry (`meerschaum.mcp`: tools, resources, prompts and the
`initialize` primer) as data for the Rust MCP server (`src/mcp/registry.json`), so tool names,
descriptions, JSON schemas, scopes and annotations are exactly v4's (§7.2).

Handlers are not exported: the Rust server implements them (`src/mcp/tools.rs`,
`resources.rs`, `prompts.rs`).

Usage (from the repository root):

    scripts/parity/run.sh crates/meerschaum-server/tests/fixtures/gen_mcp_registry.py
"""

import json
import pathlib

HERE = pathlib.Path(__file__).resolve().parent
OUT = HERE.parents[1] / 'src' / 'mcp' / 'registry.json'


def main():
    from meerschaum.mcp import (
        get_tools, get_all_resources, get_prompts, MRSM_PRIMER,
        LATEST_PROTOCOL_VERSION, SUPPORTED_PROTOCOL_VERSIONS, SERVER_NAME,
    )
    from meerschaum.mcp._security import (
        _READ_ONLY_STARTS, _MUTATING_KEYWORDS, _MUTATING_FUNCTIONS, _MUTATING_WORD_PREFIXES,
    )
    from meerschaum.mcp._tools import PROTECTED_TABLES, DEFAULT_ROW_LIMIT, MAX_ROW_LIMIT, DEFAULT_PAGE_SIZE
    from meerschaum.mcp._registry import MAX_CURSOR_OFFSET

    data = {
        'protocol_version': LATEST_PROTOCOL_VERSION,
        'supported_protocol_versions': SUPPORTED_PROTOCOL_VERSIONS,
        'server_name': SERVER_NAME,
        'primer': MRSM_PRIMER,
        'max_cursor_offset': MAX_CURSOR_OFFSET,
        'default_row_limit': DEFAULT_ROW_LIMIT,
        'max_row_limit': MAX_ROW_LIMIT,
        'default_page_size': DEFAULT_PAGE_SIZE,
        'protected_tables': sorted(PROTECTED_TABLES),
        'read_only_starts': sorted(_READ_ONLY_STARTS),
        'mutating_keywords': sorted(_MUTATING_KEYWORDS),
        'mutating_functions': sorted(_MUTATING_FUNCTIONS),
        'mutating_word_prefixes': list(_MUTATING_WORD_PREFIXES),
        'tools': [
            {
                'name': tool.name,
                'description': tool.description,
                'title': tool.title,
                'scopes': tool.scopes,
                'read_only': tool.read_only,
                'destructive': tool.destructive,
                'idempotent': tool.idempotent,
                'open_world': tool.open_world,
                'input_schema': tool.input_schema,
                'output_schema': tool.output_schema,
            }
            for tool in get_tools().values()
        ],
        'resources': [
            {
                'name': res.name,
                'description': res.description,
                'title': res.title,
                'uri': res.uri,
                'uri_template': res.uri_template,
                'mime_type': res.mime_type,
                'scopes': res.scopes,
            }
            for res in get_all_resources()
        ],
        'prompts': [
            {
                'name': prompt.name,
                'description': prompt.description,
                'title': prompt.title,
                'arguments': prompt.arguments,
                'scopes': prompt.scopes,
            }
            for prompt in get_prompts().values()
        ],
    }
    OUT.write_text(json.dumps(data, indent=2, ensure_ascii=False) + '\n', encoding='utf-8')
    print(f"Wrote {OUT}.")


if __name__ == '__main__':
    main()
