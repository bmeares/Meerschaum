#! /usr/bin/env python3
# -*- coding: utf-8 -*-
# vim:fenc=utf-8

"""
Generate `docs/zensical/reference/mcp/tools.md` from the MCP registry.

The registry is the Rust server's (`crates/meerschaum-server/src/mcp/`: v4's generated
`registry.json` plus v5's additions in `v5.rs`), so the page is rendered there, by the
`mcp_docs` test of `meerschaum-server`. Run this after adding or changing a tool, resource, or
prompt so the published reference cannot drift from the code:

    python scripts/generate_mcp_docs.py

`scripts/docs.sh` runs it as part of the docs build. Pass `--check` to fail instead of writing,
for CI. Without `cargo` the page is left as it is (with a warning).
"""

import os
import pathlib
import shutil
import subprocess
import sys

REPO_ROOT = pathlib.Path(__file__).resolve().parent.parent
OUTPUT_PATH = REPO_ROOT / 'docs' / 'zensical' / 'reference' / 'mcp' / 'tools.md'


def main(argv) -> int:
    check = '--check' in argv
    cargo = shutil.which('cargo') or os.path.expanduser('~/.cargo/bin/cargo')
    if not os.path.exists(cargo):
        print(f"cargo is not installed: {OUTPUT_PATH} is left as it is.", file=sys.stderr)
        return 1 if check else 0
    env = {key: value for key, value in os.environ.items() if key != 'MRSM_UPDATE_MCP_DOCS'}
    if not check:
        env['MRSM_UPDATE_MCP_DOCS'] = '1'
    command = [
        cargo, 'test', '-q', '-p', 'meerschaum-server', '--test', 'mcp_docs',
        '--', 'the_tool_reference_page_is_current',
    ]
    result = subprocess.run(command, cwd=REPO_ROOT, env=env)
    if result.returncode != 0:
        return result.returncode
    if not check:
        print(f"Wrote {OUTPUT_PATH.relative_to(REPO_ROOT)}.")
    return 0


if __name__ == '__main__':
    sys.exit(main(sys.argv[1:]))
