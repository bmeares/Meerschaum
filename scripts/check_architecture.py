#!/usr/bin/env python3
"""
Automated architectural guardrails for the Meerschaum workspace (PR #277 / WS-9).

    python3 scripts/check_architecture.py

Runs 8 fast, stdlib-only checks (<1s) enforcing architectural invariants across the
Python dashboard/API layer and Rust workspace crates:

1. No raw `urllib.request.urlopen` on local-server paths in `meerschaum/api/`.
2. No retired shallow viewer modules or helpers in `meerschaum/api/`.
3. No duplicate SQL lexer in `crates/meerschaum-server/src/mcp/security.rs` and no
   brittle error-string `.replace(...)` patches in server/CLI.
4. No import cycles in `meerschaum/api/dash/` (including function-local imports):
   - `sessions.py` must never reach `access.py` (directly or transitively).
   - The plugin cluster must have 0 strongly connected components / cycles.
   - No new cyclic edges anywhere else in `meerschaum/api/dash/`.
5. No `struct Inplace` or `is_fetch_optimization` workarounds, and
   `fn forget_engine_state_after` defined only in `crates/meerschaum-connector/src/`.
6. Canonical schema enforcement in `crates/meerschaum-connector-sql` (`ensure_schema`,
   `bind_pipe`, `require_schema` ceiling <= 5, no `synced_columns` / `internal_pipe_states`).
7. Rust integration test binary ceilings per crate and across the workspace (<= 45).
8. `Cargo.toml` / `Cargo.lock` binary slimming, clippy workspace lints, and no `ring`.

Exits 0 with a summary line when clean, or 1 with file/line diagnostics on any violation.
Runs on any Python >= 3.11 with no third-party imports.
"""

from __future__ import annotations

import ast
import re
import sys
import tomllib
from collections import defaultdict, deque
from pathlib import Path
from typing import Callable

ROOT = Path(__file__).resolve().parent.parent

# ---------------------------------------------------------------------------
# Rule 1: No raw urllib.request.urlopen in meerschaum/api/
# ---------------------------------------------------------------------------

RAW_URLOPEN_ALLOWLIST: frozenset[str] = frozenset({
    # The hardened local-server UrllibTransport adapter (proxy-stripped, no-redirect).
    'meerschaum/api/host/_server_client.py',
    # Probes *external* user-configured API connector URLs, not the local supervising Rust server.
    'meerschaum/api/dash/connector_listing.py',
})


def _parse_py(
    path: Path,
    rel: str,
    ast_cache: dict[Path, ast.AST] | None = None,
    source: str | None = None,
) -> ast.AST:
    if ast_cache is not None and path in ast_cache:
        return ast_cache[path]
    if source is None:
        source = path.read_text(encoding='utf-8')
    tree = ast.parse(source, filename=rel)
    if ast_cache is not None:
        ast_cache[path] = tree
    return tree


def check_no_raw_urlopen(
    root: Path = ROOT,
    *,
    ast_cache: dict[Path, ast.AST] | None = None,
) -> list[str]:
    """Rule 1: Forbid raw `urllib.request.urlopen` outside the documented allowlist."""
    api_dir = root / 'meerschaum' / 'api'
    if not api_dir.is_dir():
        return [f'[no-raw-urlopen] missing directory: {api_dir}']

    violations: list[str] = []
    allowlist_desc = ', '.join(sorted(RAW_URLOPEN_ALLOWLIST))

    for path in sorted(api_dir.rglob('*.py')):
        rel = path.relative_to(root).as_posix()
        if rel in RAW_URLOPEN_ALLOWLIST:
            continue
        source = path.read_text(encoding='utf-8')
        if 'urlopen' not in source and 'urllib.request' not in source:
            continue
        tree = _parse_py(path, rel, ast_cache, source)
        seen_lines: set[int] = set()
        for node in ast.walk(tree):
            hit_reason: str | None = None
            if isinstance(node, ast.ImportFrom) and node.module == 'urllib.request':
                if any(alias.name in ('urlopen', '*') for alias in node.names):
                    hit_reason = 'imports `urlopen` from `urllib.request`'
            elif isinstance(node, ast.Attribute) and node.attr == 'urlopen':
                hit_reason = 'references `.urlopen`'
            elif (
                isinstance(node, ast.Call)
                and isinstance(node.func, ast.Name)
                and node.func.id == 'urlopen'
            ):
                hit_reason = 'calls `urlopen(...)`'

            if hit_reason is not None and node.lineno not in seen_lines:
                seen_lines.add(node.lineno)
                violations.append(
                    f'[no-raw-urlopen] {rel}:{node.lineno}: {hit_reason}; '
                    'route local-server HTTP calls through meerschaum.api.host._server_client '
                    f'(allowlist: {allowlist_desc})'
                )
    return violations


# ---------------------------------------------------------------------------
# Rule 2: No retired shallow viewer modules/helpers in meerschaum/api/
# ---------------------------------------------------------------------------

RETIRED_VIEWER_NAMES: frozenset[str] = frozenset({
    'pipes_access',
    'source_viewer',
    'fresh_viewer',
})


def check_no_retired_viewers(
    root: Path = ROOT,
    *,
    ast_cache: dict[Path, ast.AST] | None = None,
) -> list[str]:
    """Rule 2: Forbid retired shallow viewer module `pipes_access.py` and viewer helpers."""
    violations: list[str] = []
    retired_module = root / 'meerschaum' / 'api' / 'dash' / 'pipes_access.py'
    if retired_module.exists():
        violations.append(
            '[no-retired-viewers] meerschaum/api/dash/pipes_access.py must not exist '
            '(retired shallow viewer module; use canonical access helpers instead)'
        )

    api_dir = root / 'meerschaum' / 'api'
    if not api_dir.is_dir():
        violations.append(f'[no-retired-viewers] missing directory: {api_dir}')
        return violations

    for path in sorted(api_dir.rglob('*.py')):
        rel = path.relative_to(root).as_posix()
        source = path.read_text(encoding='utf-8')
        if not any(tok in source for tok in ('pipes_access', 'source_viewer', 'fresh_viewer', '.viewer')):
            continue
        tree = _parse_py(path, rel, ast_cache, source)
        reported: set[tuple[int, str]] = set()

        def record(lineno: int, symbol: str, detail: str) -> None:
            key = (lineno, symbol)
            if key not in reported:
                reported.add(key)
                violations.append(
                    f'[no-retired-viewers] {rel}:{lineno}: forbidden reference to retired '
                    f'`{symbol}` ({detail})'
                )

        for node in ast.walk(tree):
            if isinstance(node, ast.Import):
                for alias in node.names:
                    parts = alias.name.split('.')
                    for sym in RETIRED_VIEWER_NAMES:
                        if sym in parts:
                            record(node.lineno, sym, f'import `{alias.name}`')
            elif isinstance(node, ast.ImportFrom):
                mod_parts = (node.module or '').split('.')
                for sym in RETIRED_VIEWER_NAMES:
                    if sym in mod_parts:
                        record(node.lineno, sym, f'from `{node.module}` import ...')
                for alias in node.names:
                    if alias.name in RETIRED_VIEWER_NAMES:
                        record(node.lineno, alias.name, f'import `{alias.name}`')
            elif isinstance(node, ast.Attribute):
                if node.attr in RETIRED_VIEWER_NAMES:
                    record(node.lineno, node.attr, f'attribute `.{node.attr}`')
                elif node.attr == 'viewer' and (
                    (isinstance(node.value, ast.Name) and node.value.id == 'users')
                    or (isinstance(node.value, ast.Attribute) and node.value.attr == 'users')
                ):
                    record(node.lineno, 'users.viewer', 'attribute `users.viewer`')
            elif (
                isinstance(node, ast.Call)
                and isinstance(node.func, ast.Name)
                and node.func.id in RETIRED_VIEWER_NAMES
            ):
                record(node.lineno, node.func.id, f'call `{node.func.id}(...)`')
            elif isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef)):
                if node.name in RETIRED_VIEWER_NAMES:
                    record(node.lineno, node.name, f'def `{node.name}`')

    return violations


# ---------------------------------------------------------------------------
# Rule 3: No duplicate SQL lexer or brittle error-string patches
# ---------------------------------------------------------------------------

FORBIDDEN_MCP_SQL_LEXER_FNS: tuple[str, ...] = (
    'strip_sql_noise',
    'dollar_tag_len',
    'opens_call',
    'refuse_internal_tables',
    'is_bare_identifier',
    'is_read_only_query',
)

FORBIDDEN_ERROR_REPLACE_PATTERNS: tuple[str, ...] = (
    '.replace("for an API request"',
    '.replace("a SQL definition"',
)


def _strip_rust_line_comment(line: str) -> str:
    """Strip a `//` line comment outside double-quoted string literals."""
    in_str = False
    escaped = False
    i = 0
    n = len(line)
    while i < n:
        ch = line[i]
        if in_str:
            if escaped:
                escaped = False
            elif ch == '\\':
                escaped = True
            elif ch == '"':
                in_str = False
            i += 1
            continue
        if ch == '"':
            in_str = True
            i += 1
            continue
        if ch == '/' and i + 1 < n and line[i + 1] == '/':
            return line[:i]
        i += 1
    return line


def check_no_duplicate_sql_lexer(root: Path = ROOT) -> list[str]:
    """Rule 3: Forbid duplicate SQL lexer helpers in MCP security and error `.replace` patches."""
    violations: list[str] = []

    security_rs = root / 'crates' / 'meerschaum-server' / 'src' / 'mcp' / 'security.rs'
    if not security_rs.is_file():
        violations.append(
            f'[no-duplicate-sql-lexer] missing expected file: '
            f'{security_rs.relative_to(root).as_posix()}'
        )
    else:
        rel = security_rs.relative_to(root).as_posix()
        fn_re = re.compile(
            r'\bfn\s+(' + '|'.join(re.escape(name) for name in FORBIDDEN_MCP_SQL_LEXER_FNS) + r')\b'
        )
        for lineno, raw_line in enumerate(security_rs.read_text(encoding='utf-8').splitlines(), 1):
            code = _strip_rust_line_comment(raw_line)
            match = fn_re.search(code)
            if match:
                violations.append(
                    f'[no-duplicate-sql-lexer] {rel}:{lineno}: defines `{match.group(1)}`; '
                    'delegate SQL read-only/table validation to the shared SQL guard instead of '
                    'duplicating the lexer in mcp/security.rs'
                )

    for rel_dir in ('crates/meerschaum-server/src', 'crates/meerschaum-cli/src'):
        src_dir = root / rel_dir
        if not src_dir.is_dir():
            violations.append(f'[no-duplicate-sql-lexer] missing directory: {rel_dir}')
            continue
        for rs_path in sorted(src_dir.rglob('*.rs')):
            text = rs_path.read_text(encoding='utf-8')
            if not any(pat in text for pat in FORBIDDEN_ERROR_REPLACE_PATTERNS):
                continue
            rel = rs_path.relative_to(root).as_posix()
            for lineno, raw_line in enumerate(text.splitlines(), 1):
                if not any(pat in raw_line for pat in FORBIDDEN_ERROR_REPLACE_PATTERNS):
                    continue
                code = _strip_rust_line_comment(raw_line)
                for pat in FORBIDDEN_ERROR_REPLACE_PATTERNS:
                    if pat in code:
                        violations.append(
                            f'[no-duplicate-sql-lexer] {rel}:{lineno}: contains brittle error-string '
                            f'patch `{pat}`; construct structured/context-aware errors at the source'
                        )

    return violations


# ---------------------------------------------------------------------------
# Rule 4: No import cycles in meerschaum/api/dash/
# ---------------------------------------------------------------------------

PLUGIN_CLUSTER_EXACT: frozenset[str] = frozenset({
    'meerschaum.api.dash.plugins',
    'meerschaum.api.dash.plugin_sources',
    'meerschaum.api.dash.plugin_upload',
    'meerschaum.api.dash.plugin_publish',
    'meerschaum.api.dash.pages.plugins',
    'meerschaum.api.dash.callbacks.plugins',
})

# Pre-existing cyclic edges outside the strictly acyclic boundaries (`sessions -> access`
# and the internal plugin cluster). Ratchet lock: removing edges is always allowed, but
# introducing any new cyclic edge anywhere in `meerschaum/api/dash/` fails CI.
PREEXISTING_DASH_CYCLE_EDGES: frozenset[tuple[str, str]] = frozenset({
    # SCC 1: legacy access / components / engine / team_setup / sql_guard / webterm / plugins bridge
    ('meerschaum.api.dash._native_execution', 'meerschaum.api.dash.access'),
    ('meerschaum.api.dash._plugins', 'meerschaum.api.dash._plugins._lifecycle'),
    ('meerschaum.api.dash._plugins', 'meerschaum.api.dash._plugins._policy'),
    ('meerschaum.api.dash._plugins._lifecycle', 'meerschaum.api.dash._plugins._policy'),
    ('meerschaum.api.dash._plugins._lifecycle', 'meerschaum.api.dash.access'),
    ('meerschaum.api.dash._plugins._policy', 'meerschaum.api.dash.access'),
    ('meerschaum.api.dash.access', 'meerschaum.api.dash._native_execution'),
    ('meerschaum.api.dash.access', 'meerschaum.api.dash.components'),
    ('meerschaum.api.dash.access', 'meerschaum.api.dash.engine'),
    ('meerschaum.api.dash.access', 'meerschaum.api.dash.sql_guard'),
    ('meerschaum.api.dash.access', 'meerschaum.api.dash.team_setup'),
    ('meerschaum.api.dash.components', 'meerschaum.api.dash.access'),
    ('meerschaum.api.dash.components', 'meerschaum.api.dash.connector_listing'),
    ('meerschaum.api.dash.components', 'meerschaum.api.dash.engine'),
    ('meerschaum.api.dash.components', 'meerschaum.api.dash.team_setup'),
    ('meerschaum.api.dash.components', 'meerschaum.api.dash.webterm'),
    ('meerschaum.api.dash.connector_listing', 'meerschaum.api.dash.access'),
    ('meerschaum.api.dash.engine', 'meerschaum.api.dash.access'),
    ('meerschaum.api.dash.engine', 'meerschaum.api.dash.connector_listing'),
    ('meerschaum.api.dash.pipe_writes', 'meerschaum.api.dash.access'),
    ('meerschaum.api.dash.pipe_writes', 'meerschaum.api.dash.connector_listing'),
    ('meerschaum.api.dash.pipe_writes', 'meerschaum.api.dash.engine'),
    ('meerschaum.api.dash.pipe_writes', 'meerschaum.api.dash.sql_guard'),
    ('meerschaum.api.dash.pipe_writes', 'meerschaum.api.dash.team_setup'),
    ('meerschaum.api.dash.plugin_publish', 'meerschaum.api.dash._plugins'),
    ('meerschaum.api.dash.plugin_publish', 'meerschaum.api.dash.access'),
    ('meerschaum.api.dash.plugin_publish', 'meerschaum.api.dash.connector_listing'),
    ('meerschaum.api.dash.plugin_sources', 'meerschaum.api.dash._plugins._lifecycle'),
    ('meerschaum.api.dash.plugin_sources', 'meerschaum.api.dash._plugins._policy'),
    ('meerschaum.api.dash.plugin_sources', 'meerschaum.api.dash.components'),
    ('meerschaum.api.dash.plugins', 'meerschaum.api.dash._plugins._lifecycle'),
    ('meerschaum.api.dash.plugins', 'meerschaum.api.dash._plugins._policy'),
    ('meerschaum.api.dash.plugins', 'meerschaum.api.dash.access'),
    ('meerschaum.api.dash.plugins', 'meerschaum.api.dash.components'),
    ('meerschaum.api.dash.plugins', 'meerschaum.api.dash.connector_listing'),
    ('meerschaum.api.dash.plugins', 'meerschaum.api.dash.plugin_publish'),
    ('meerschaum.api.dash.plugins', 'meerschaum.api.dash.plugin_sources'),
    ('meerschaum.api.dash.sql_guard', 'meerschaum.api.dash.access'),
    ('meerschaum.api.dash.sql_guard', 'meerschaum.api.dash.engine'),
    ('meerschaum.api.dash.sql_guard', 'meerschaum.api.dash.pipe_writes'),
    ('meerschaum.api.dash.team_setup', 'meerschaum.api.dash.access'),
    ('meerschaum.api.dash.team_setup', 'meerschaum.api.dash.connector_listing'),
    ('meerschaum.api.dash.team_setup', 'meerschaum.api.dash.engine'),
    ('meerschaum.api.dash.team_setup', 'meerschaum.api.dash.plugins'),
    ('meerschaum.api.dash.webterm', 'meerschaum.api.dash.access'),
    ('meerschaum.api.dash.webterm', 'meerschaum.api.dash.components'),
    # SCC 2: legacy Dash app <-> callbacks / pages / pipes / users registration cycle
    ('meerschaum.api.dash', 'meerschaum.api.dash.callbacks'),
    ('meerschaum.api.dash', 'meerschaum.api.dash.callbacks.custom'),
    ('meerschaum.api.dash', 'meerschaum.api.dash.pages'),
    ('meerschaum.api.dash.callbacks', 'meerschaum.api.dash.callbacks.connectors_page'),
    ('meerschaum.api.dash.callbacks', 'meerschaum.api.dash.callbacks.custom'),
    ('meerschaum.api.dash.callbacks', 'meerschaum.api.dash.callbacks.dashboard'),
    ('meerschaum.api.dash.callbacks', 'meerschaum.api.dash.callbacks.execute'),
    ('meerschaum.api.dash.callbacks', 'meerschaum.api.dash.callbacks.jobs'),
    ('meerschaum.api.dash.callbacks', 'meerschaum.api.dash.callbacks.login'),
    ('meerschaum.api.dash.callbacks', 'meerschaum.api.dash.callbacks.overview'),
    ('meerschaum.api.dash.callbacks', 'meerschaum.api.dash.callbacks.performance'),
    ('meerschaum.api.dash.callbacks', 'meerschaum.api.dash.callbacks.pipeline_management'),
    ('meerschaum.api.dash.callbacks', 'meerschaum.api.dash.callbacks.pipeline_pages'),
    ('meerschaum.api.dash.callbacks', 'meerschaum.api.dash.callbacks.pipelines'),
    ('meerschaum.api.dash.callbacks', 'meerschaum.api.dash.callbacks.pipes'),
    ('meerschaum.api.dash.callbacks', 'meerschaum.api.dash.callbacks.plugins'),
    ('meerschaum.api.dash.callbacks', 'meerschaum.api.dash.callbacks.register'),
    ('meerschaum.api.dash.callbacks', 'meerschaum.api.dash.callbacks.settings'),
    ('meerschaum.api.dash.callbacks', 'meerschaum.api.dash.callbacks.tokens'),
    ('meerschaum.api.dash.callbacks', 'meerschaum.api.dash.callbacks.users'),
    ('meerschaum.api.dash.callbacks.connectors_page', 'meerschaum.api.dash'),
    ('meerschaum.api.dash.callbacks.custom', 'meerschaum.api.dash'),
    ('meerschaum.api.dash.callbacks.custom', 'meerschaum.api.dash.callbacks.dashboard'),
    ('meerschaum.api.dash.callbacks.dashboard', 'meerschaum.api.dash'),
    ('meerschaum.api.dash.callbacks.dashboard', 'meerschaum.api.dash.callbacks.pipelines'),
    ('meerschaum.api.dash.callbacks.dashboard', 'meerschaum.api.dash.pages'),
    ('meerschaum.api.dash.callbacks.dashboard', 'meerschaum.api.dash.pipes'),
    ('meerschaum.api.dash.callbacks.execute', 'meerschaum.api.dash'),
    ('meerschaum.api.dash.callbacks.execute', 'meerschaum.api.dash.callbacks.dashboard'),
    ('meerschaum.api.dash.callbacks.execute', 'meerschaum.api.dash.pipe_detail'),
    ('meerschaum.api.dash.callbacks.jobs', 'meerschaum.api.dash'),
    ('meerschaum.api.dash.callbacks.login', 'meerschaum.api.dash'),
    ('meerschaum.api.dash.callbacks.login', 'meerschaum.api.dash.pipes'),
    ('meerschaum.api.dash.callbacks.login', 'meerschaum.api.dash.registration'),
    ('meerschaum.api.dash.callbacks.overview', 'meerschaum.api.dash'),
    ('meerschaum.api.dash.callbacks.performance', 'meerschaum.api.dash'),
    ('meerschaum.api.dash.callbacks.performance', 'meerschaum.api.dash.callbacks.overview'),
    ('meerschaum.api.dash.callbacks.pipeline_management', 'meerschaum.api.dash'),
    ('meerschaum.api.dash.callbacks.pipeline_management', 'meerschaum.api.dash.callbacks.pipelines'),
    ('meerschaum.api.dash.callbacks.pipeline_pages', 'meerschaum.api.dash'),
    ('meerschaum.api.dash.callbacks.pipeline_pages', 'meerschaum.api.dash.callbacks.pipelines'),
    ('meerschaum.api.dash.callbacks.pipelines', 'meerschaum.api.dash'),
    ('meerschaum.api.dash.callbacks.pipelines', 'meerschaum.api.dash.pipes'),
    ('meerschaum.api.dash.callbacks.pipes', 'meerschaum.api.dash'),
    ('meerschaum.api.dash.callbacks.pipes', 'meerschaum.api.dash.pipe_detail'),
    ('meerschaum.api.dash.callbacks.pipes', 'meerschaum.api.dash.pipes'),
    ('meerschaum.api.dash.callbacks.plugins', 'meerschaum.api.dash'),
    ('meerschaum.api.dash.callbacks.register', 'meerschaum.api.dash'),
    ('meerschaum.api.dash.callbacks.register', 'meerschaum.api.dash.registration'),
    ('meerschaum.api.dash.callbacks.settings', 'meerschaum.api.dash.callbacks.settings.password_reset'),
    ('meerschaum.api.dash.callbacks.settings.password_reset', 'meerschaum.api.dash'),
    ('meerschaum.api.dash.callbacks.tokens', 'meerschaum.api.dash'),
    ('meerschaum.api.dash.callbacks.users', 'meerschaum.api.dash'),
    ('meerschaum.api.dash.callbacks.users', 'meerschaum.api.dash.users'),
    ('meerschaum.api.dash.pages', 'meerschaum.api.dash.pages.login'),
    ('meerschaum.api.dash.pages', 'meerschaum.api.dash.pages.pipes'),
    ('meerschaum.api.dash.pages', 'meerschaum.api.dash.pages.register'),
    ('meerschaum.api.dash.pages', 'meerschaum.api.dash.pages.users'),
    ('meerschaum.api.dash.pages.login', 'meerschaum.api.dash.registration'),
    ('meerschaum.api.dash.pages.pipes', 'meerschaum.api.dash.pipe_detail'),
    ('meerschaum.api.dash.pages.register', 'meerschaum.api.dash.registration'),
    ('meerschaum.api.dash.pages.users', 'meerschaum.api.dash.users'),
    ('meerschaum.api.dash.pipe_detail', 'meerschaum.api.dash.pipes'),
    ('meerschaum.api.dash.pipes', 'meerschaum.api.dash'),
    ('meerschaum.api.dash.registration', 'meerschaum.api.dash.users'),
    ('meerschaum.api.dash.users', 'meerschaum.api.dash'),
})


def _is_plugin_cluster_module(mod: str) -> bool:
    return (
        mod in PLUGIN_CLUSTER_EXACT
        or mod == 'meerschaum.api.dash._plugins'
        or mod.startswith('meerschaum.api.dash._plugins.')
    )


def _file_to_module(root: Path, path: Path) -> str:
    parts = list(path.relative_to(root).with_suffix('').parts)
    if parts[-1] == '__init__':
        parts = parts[:-1]
    return '.'.join(parts)


def _resolve_import_targets(
    cur_mod: str,
    is_pkg: bool,
    node: ast.Import | ast.ImportFrom,
    known_mods: set[str],
) -> list[str]:
    targets: list[str] = []
    if isinstance(node, ast.Import):
        for alias in node.names:
            if alias.name in known_mods:
                targets.append(alias.name)
    elif isinstance(node, ast.ImportFrom):
        level = node.level or 0
        if level == 0:
            base = node.module or ''
        else:
            parts = cur_mod.split('.')
            if not is_pkg:
                parts = parts[:-1]
            if level > 1:
                if level - 1 > len(parts):
                    return []
                parts = parts[: -(level - 1)]
            base = '.'.join(parts + ([node.module] if node.module else []))
        for alias in node.names:
            if alias.name != '*' and base and f'{base}.{alias.name}' in known_mods:
                targets.append(f'{base}.{alias.name}')
            elif base in known_mods:
                targets.append(base)
    return targets


def _iter_import_nodes(node: ast.AST):
    """Yield every `ast.Import` / `ast.ImportFrom` in statement bodies (including function-local)."""
    stack: list[ast.AST] = [node]
    while stack:
        cur = stack.pop()
        if isinstance(cur, (ast.Import, ast.ImportFrom)):
            yield cur
            continue
        for attr in ('body', 'orelse', 'finalbody', 'handlers', 'cases'):
            children = getattr(cur, attr, None)
            if isinstance(children, list):
                stack.extend(children)


def _build_dash_import_graph(
    root: Path,
    ast_cache: dict[Path, ast.AST] | None = None,
) -> tuple[dict[str, Path], dict[str, set[str]], dict[tuple[str, str], list[int]]]:
    dash_dir = root / 'meerschaum' / 'api' / 'dash'
    mods: dict[str, Path] = {
        _file_to_module(root, p): p for p in sorted(dash_dir.rglob('*.py'))
    }
    known_mods = set(mods)
    graph: dict[str, set[str]] = defaultdict(set)
    edge_locs: dict[tuple[str, str], list[int]] = defaultdict(list)

    for mod, path in mods.items():
        is_pkg = path.name == '__init__.py'
        tree = _parse_py(path, path.relative_to(root).as_posix(), ast_cache)
        for node in _iter_import_nodes(tree):
            for target in _resolve_import_targets(mod, is_pkg, node, known_mods):
                if target != mod:
                    graph[mod].add(target)
                    edge_locs[(mod, target)].append(node.lineno)

    return mods, graph, edge_locs


def _tarjan_sccs(nodes: set[str], adj: dict[str, set[str]]) -> list[list[str]]:
    index = 0
    stack: list[str] = []
    on_stack: set[str] = set()
    indices: dict[str, int] = {}
    lowlink: dict[str, int] = {}
    sccs: list[list[str]] = []

    def strongconnect(v: str) -> None:
        nonlocal index
        indices[v] = lowlink[v] = index
        index += 1
        stack.append(v)
        on_stack.add(v)
        for w in sorted(adj.get(v, ())):
            if w not in nodes:
                continue
            if w not in indices:
                strongconnect(w)
                lowlink[v] = min(lowlink[v], lowlink[w])
            elif w in on_stack:
                lowlink[v] = min(lowlink[v], indices[w])
        if lowlink[v] == indices[v]:
            comp: list[str] = []
            while True:
                w = stack.pop()
                on_stack.remove(w)
                comp.append(w)
                if w == v:
                    break
            if len(comp) > 1:
                sccs.append(sorted(comp))

    for v in sorted(nodes):
        if v not in indices:
            strongconnect(v)
    return sccs


def _shortest_path(adj: dict[str, set[str]], start: str, goal: str) -> list[str] | None:
    if start == goal:
        return [start]
    q: deque[list[str]] = deque([[start]])
    seen: set[str] = {start}
    while q:
        path = q.popleft()
        cur = path[-1]
        for nxt in sorted(adj.get(cur, ())):
            if nxt == goal:
                return path + [nxt]
            if nxt not in seen:
                seen.add(nxt)
                q.append(path + [nxt])
    return None


def check_dash_import_cycles(
    root: Path = ROOT,
    *,
    ast_cache: dict[Path, ast.AST] | None = None,
) -> list[str]:
    """Rule 4: Enforce acyclic dash import invariants (including function-local imports)."""
    dash_dir = root / 'meerschaum' / 'api' / 'dash'
    if not dash_dir.is_dir():
        return [f'[dash-import-cycles] missing directory: {dash_dir}']

    mods, graph, edge_locs = _build_dash_import_graph(root, ast_cache=ast_cache)
    violations: list[str] = []

    # 4a. sessions.py MUST NOT import access.py (directly or transitively).
    sessions_mod = 'meerschaum.api.dash.sessions'
    access_mod = 'meerschaum.api.dash.access'
    if sessions_mod in mods and access_mod in mods:
        path = _shortest_path(graph, sessions_mod, access_mod)
        if path is not None:
            steps = []
            for u, v in zip(path, path[1:]):
                lines = sorted(set(edge_locs[(u, v)]))
                rel_u = mods[u].relative_to(root).as_posix()
                steps.append(f'{rel_u}:{lines[0]} -> {v}')
            violations.append(
                '[dash-import-cycles] meerschaum/api/dash/sessions.py must not import '
                f'meerschaum.api.dash.access (directly or transitively): {" ; ".join(steps)}'
            )

    # 4b. The plugin cluster MUST have 0 strongly connected components / cycles.
    plugin_mods = {m for m in mods if _is_plugin_cluster_module(m)}
    for scc in _tarjan_sccs(plugin_mods, graph):
        scc_set = set(scc)
        edge_descs = []
        for u in scc:
            for v in sorted(graph.get(u, ())):
                if v in scc_set:
                    rel_u = mods[u].relative_to(root).as_posix()
                    line = min(edge_locs[(u, v)])
                    edge_descs.append(f'{rel_u}:{line} -> {v}')
        violations.append(
            '[dash-import-cycles] plugin cluster must have 0 cycles; found SCC '
            f'({", ".join(scc)}): {" ; ".join(edge_descs)}'
        )

    # 4c. Lock the entire meerschaum/api/dash/ graph against any NEW cyclic edges.
    for scc in _tarjan_sccs(set(mods), graph):
        scc_set = set(scc)
        for u in scc:
            for v in sorted(graph.get(u, ())):
                if v in scc_set and (u, v) not in PREEXISTING_DASH_CYCLE_EDGES:
                    rel_u = mods[u].relative_to(root).as_posix()
                    line = min(edge_locs[(u, v)])
                    violations.append(
                        f'[dash-import-cycles] {rel_u}:{line}: new import cycle edge '
                        f'`{u} -> {v}` in meerschaum/api/dash/ '
                        f'(SCC members: {", ".join(scc)})'
                    )

    return violations


# ---------------------------------------------------------------------------
# Rule 5: No struct Inplace, is_fetch_optimization, or duplicate forget_engine_state_after
# ---------------------------------------------------------------------------

_INPLACE_RE = re.compile(r'\bstruct\s+Inplace\b')
_FETCH_OPT_RE = re.compile(r'\bis_fetch_optimization\b')
_FORGET_STATE_FN_RE = re.compile(r'\bfn\s+forget_engine_state_after\b')


def check_no_sync_workarounds(root: Path = ROOT) -> list[str]:
    """Rule 5: Forbid `struct Inplace`, `is_fetch_optimization`, and duplicate `forget_engine_state_after`."""
    violations: list[str] = []

    for rel_dir in ('crates/meerschaum-cli/src', 'crates/meerschaum-py/src'):
        src_dir = root / rel_dir
        if not src_dir.is_dir():
            violations.append(f'[no-sync-workarounds] missing directory: {rel_dir}')
            continue
        for rs_path in sorted(src_dir.rglob('*.rs')):
            text = rs_path.read_text(encoding='utf-8')
            if 'Inplace' not in text:
                continue
            rel = rs_path.relative_to(root).as_posix()
            for lineno, raw_line in enumerate(text.splitlines(), 1):
                if 'Inplace' in raw_line and _INPLACE_RE.search(_strip_rust_line_comment(raw_line)):
                    violations.append(
                        f'[no-sync-workarounds] {rel}:{lineno}: `struct Inplace` workaround is forbidden; '
                        'use canonical engine sync paths'
                    )

    for rel_dir in (
        'crates/meerschaum-engine/src',
        'crates/meerschaum-cli/src',
        'crates/meerschaum-py/src',
    ):
        src_dir = root / rel_dir
        if not src_dir.is_dir():
            violations.append(f'[no-sync-workarounds] missing directory: {rel_dir}')
            continue
        for rs_path in sorted(src_dir.rglob('*.rs')):
            text = rs_path.read_text(encoding='utf-8')
            if 'is_fetch_optimization' not in text:
                continue
            rel = rs_path.relative_to(root).as_posix()
            for lineno, raw_line in enumerate(text.splitlines(), 1):
                if 'is_fetch_optimization' in raw_line and _FETCH_OPT_RE.search(
                    _strip_rust_line_comment(raw_line)
                ):
                    violations.append(
                        f'[no-sync-workarounds] {rel}:{lineno}: `is_fetch_optimization` workaround '
                        'is forbidden'
                    )

    crates_dir = root / 'crates'
    if not crates_dir.is_dir():
        violations.append('[no-sync-workarounds] missing directory: crates')
        return violations

    connector_defs = 0
    for rs_path in sorted(crates_dir.glob('*/src/**/*.rs')):
        text = rs_path.read_text(encoding='utf-8')
        if 'forget_engine_state_after' not in text:
            continue
        rel = rs_path.relative_to(root).as_posix()
        for lineno, raw_line in enumerate(text.splitlines(), 1):
            if 'forget_engine_state_after' in raw_line and _FORGET_STATE_FN_RE.search(
                _strip_rust_line_comment(raw_line)
            ):
                if rel.startswith('crates/meerschaum-connector/src/'):
                    connector_defs += 1
                else:
                    violations.append(
                        f'[no-sync-workarounds] {rel}:{lineno}: `fn forget_engine_state_after` '
                        'must be defined ONLY in crates/meerschaum-connector/src/'
                    )

    if connector_defs == 0:
        violations.append(
            '[no-sync-workarounds] crates/meerschaum-connector/src/: missing canonical '
            '`fn forget_engine_state_after` definition'
        )

    return violations


# ---------------------------------------------------------------------------
# Rule 6: Canonical schema enforcement in crates/meerschaum-connector-sql
# ---------------------------------------------------------------------------

REQUIRE_SCHEMA_CEILING = 5
_ENSURE_SCHEMA_RE = re.compile(r'\bfn\s+ensure_schema\b')
_BIND_PIPE_RE = re.compile(r'\bfn\s+bind_pipe\b')
_FORBIDDEN_SQL_STATE_RE = re.compile(r'\b(synced_columns|internal_pipe_states)\b')


def check_sql_canonical_schema(root: Path = ROOT) -> list[str]:
    """Rule 6: Enforce canonical schema/binding helpers and forbid legacy SQL state fields."""
    violations: list[str] = []

    mod_rs = root / 'crates' / 'meerschaum-connector-sql' / 'src' / 'instance' / 'mod.rs'
    if not mod_rs.is_file():
        violations.append(
            '[sql-canonical-schema] missing file: crates/meerschaum-connector-sql/src/instance/mod.rs'
        )
    else:
        code_lines = [
            _strip_rust_line_comment(line)
            for line in mod_rs.read_text(encoding='utf-8').splitlines()
        ]
        code_text = '\n'.join(code_lines)
        if not _ENSURE_SCHEMA_RE.search(code_text):
            violations.append(
                '[sql-canonical-schema] crates/meerschaum-connector-sql/src/instance/mod.rs: '
                'missing `fn ensure_schema` definition'
            )
        if not _BIND_PIPE_RE.search(code_text):
            violations.append(
                '[sql-canonical-schema] crates/meerschaum-connector-sql/src/instance/mod.rs: '
                'missing `fn bind_pipe` definition'
            )

    trait_impl_rs = root / 'crates' / 'meerschaum-connector-sql' / 'src' / 'instance' / 'trait_impl.rs'
    if not trait_impl_rs.is_file():
        violations.append(
            '[sql-canonical-schema] missing file: '
            'crates/meerschaum-connector-sql/src/instance/trait_impl.rs'
        )
    else:
        trait_text = trait_impl_rs.read_text(encoding='utf-8')
        count = 0
        if 'require_schema' in trait_text:
            for raw_line in trait_text.splitlines():
                if 'require_schema' in raw_line:
                    count += _strip_rust_line_comment(raw_line).count('self.require_schema().await')
        if count > REQUIRE_SCHEMA_CEILING:
            violations.append(
                '[sql-canonical-schema] crates/meerschaum-connector-sql/src/instance/trait_impl.rs: '
                f'found {count} `self.require_schema().await` calls (ceiling: <= {REQUIRE_SCHEMA_CEILING}); '
                'use `ensure_schema` / `bind_pipe` instead'
            )

    sql_src = root / 'crates' / 'meerschaum-connector-sql' / 'src'
    if not sql_src.is_dir():
        violations.append(
            '[sql-canonical-schema] missing directory: crates/meerschaum-connector-sql/src'
        )
    else:
        for rs_path in sorted(sql_src.rglob('*.rs')):
            text = rs_path.read_text(encoding='utf-8')
            if 'synced_columns' not in text and 'internal_pipe_states' not in text:
                continue
            rel = rs_path.relative_to(root).as_posix()
            for lineno, raw_line in enumerate(text.splitlines(), 1):
                if 'synced_columns' not in raw_line and 'internal_pipe_states' not in raw_line:
                    continue
                match = _FORBIDDEN_SQL_STATE_RE.search(_strip_rust_line_comment(raw_line))
                if match:
                    violations.append(
                        f'[sql-canonical-schema] {rel}:{lineno}: forbidden legacy SQL state '
                        f'identifier `{match.group(1)}` (State::synced_columns / '
                        'State::internal_pipe_states were retired)'
                    )

    return violations


# ---------------------------------------------------------------------------
# Rule 7: Rust integration test binary ceiling per crate
# ---------------------------------------------------------------------------

INTEGRATION_TEST_CEILINGS: dict[str, int] = {
    'meerschaum': 1,
    'meerschaum-cli': 12,
    'meerschaum-compose': 1,
    'meerschaum-connector-api': 1,
    'meerschaum-connector-sql': 12,
    'meerschaum-connector-valkey': 1,
    'meerschaum-core': 1,
    'meerschaum-engine': 2,
    'meerschaum-jobs': 1,
    'meerschaum-kernel': 1,
    'meerschaum-plugin': 1,
    'meerschaum-py': 2,
    'meerschaum-server': 5,
    'meerschaum-web': 1,
    'meerschaum-worker': 1,
}
TOTAL_INTEGRATION_TEST_CEILING = 45


def count_integration_test_binaries(root: Path = ROOT) -> dict[str, int]:
    """Count Cargo integration test binaries (`tests/*.rs` + `tests/*/main.rs`) per crate."""
    crates_dir = root / 'crates'
    counts: dict[str, int] = {}
    if not crates_dir.is_dir():
        return counts
    for crate_dir in sorted(crates_dir.iterdir()):
        if not crate_dir.is_dir():
            continue
        tests_dir = crate_dir / 'tests'
        if not tests_dir.is_dir():
            counts[crate_dir.name] = 0
            continue
        top_rs = sum(1 for p in tests_dir.glob('*.rs') if p.is_file())
        sub_main = sum(1 for p in tests_dir.glob('*/main.rs') if p.is_file())
        counts[crate_dir.name] = top_rs + sub_main
    return counts


def check_integration_test_ceilings(root: Path = ROOT) -> list[str]:
    """Rule 7: Enforce per-crate and workspace-wide Rust integration test binary ceilings."""
    crates_dir = root / 'crates'
    if not crates_dir.is_dir():
        return ['[integration-test-ceilings] missing directory: crates']

    counts = count_integration_test_binaries(root)
    violations: list[str] = []

    for crate, count in sorted(counts.items()):
        ceiling = INTEGRATION_TEST_CEILINGS.get(crate, 0)
        if count > ceiling:
            violations.append(
                f'[integration-test-ceilings] crates/{crate}/tests: {count} integration test '
                f'binaries exceeds ceiling {ceiling}; bundle new integration tests into an '
                'existing suite (`tests/<suite>/main.rs`)'
            )

    total = sum(counts.values())
    if total > TOTAL_INTEGRATION_TEST_CEILING:
        violations.append(
            f'[integration-test-ceilings] workspace total: {total} integration test binaries '
            f'exceeds ceiling {TOTAL_INTEGRATION_TEST_CEILING}'
        )

    return violations


# ---------------------------------------------------------------------------
# Rule 8: Cargo.toml / Cargo.lock binary slimming & lint invariants
# ---------------------------------------------------------------------------

REQUIRED_CLIPPY_DENY_LINTS: tuple[str, ...] = (
    'dbg_macro',
    'todo',
    'unimplemented',
)


def check_cargo_invariants(root: Path = ROOT) -> list[str]:
    """Rule 8: Enforce Cargo.toml profile/dependency/lint invariants and no `ring` in Cargo.lock."""
    violations: list[str] = []

    cargo_toml_path = root / 'Cargo.toml'
    if not cargo_toml_path.is_file():
        return ['[cargo-invariants] missing root Cargo.toml']

    manifest = tomllib.loads(cargo_toml_path.read_text(encoding='utf-8'))

    # [profile.release]
    profiles = manifest.get('profile', {})
    release = profiles.get('release', {})
    if release.get('strip') != 'symbols':
        violations.append(
            f'[cargo-invariants] Cargo.toml: [profile.release] must set `strip = "symbols"` '
            f'(found {release.get("strip")!r})'
        )
    if release.get('panic') == 'abort':
        violations.append(
            '[cargo-invariants] Cargo.toml: [profile.release] must NOT set `panic = "abort"` '
            '(PyO3/cdylib unwinding requires `panic = "unwind"`)'
        )

    # [profile.ci]
    ci_profile = profiles.get('ci', {})
    if ci_profile.get('strip') != 'none':
        violations.append(
            f'[cargo-invariants] Cargo.toml: [profile.ci] must set `strip = "none"` '
            f'(found {ci_profile.get("strip")!r})'
        )

    # [workspace.dependencies] polars, arrow & sqlx slimming invariants
    workspace = manifest.get('workspace', {})
    ws_deps = workspace.get('dependencies', {})
    for dep_name in ('polars', 'arrow'):
        dep = ws_deps.get(dep_name)
        if not isinstance(dep, dict) or dep.get('default-features') is not False:
            violations.append(
                f'[cargo-invariants] Cargo.toml: [workspace.dependencies].{dep_name} must set '
                f'`default-features = false` (found {dep!r})'
            )

    sqlx_dep = ws_deps.get('sqlx')
    if isinstance(sqlx_dep, dict):
        sqlx_features = set(sqlx_dep.get('features', ()))
        if 'tls-rustls' in sqlx_features or 'tls-rustls-ring' in sqlx_features:
            violations.append(
                '[cargo-invariants] Cargo.toml: [workspace.dependencies].sqlx must use '
                '`tls-rustls-aws-lc-rs`, not `tls-rustls` / `tls-rustls-ring` (pulls in `ring`)'
            )

    # [workspace.lints.clippy]
    clippy_lints = workspace.get('lints', {}).get('clippy', {})
    for lint_name in REQUIRED_CLIPPY_DENY_LINTS:
        if clippy_lints.get(lint_name) != 'deny':
            violations.append(
                f'[cargo-invariants] Cargo.toml: [workspace.lints.clippy].{lint_name} must be '
                f'"deny" (found {clippy_lints.get(lint_name)!r})'
            )

    def _check_no_ring_deps(label: str, table: dict) -> None:
        for section in ('dependencies', 'dev-dependencies', 'build-dependencies'):
            deps = table.get(section, {})
            if not isinstance(deps, dict):
                continue
            if 'ring' in deps:
                violations.append(
                    f'[cargo-invariants] {label}: forbidden direct `ring` dependency in [{section}]'
                )
            for dep_name, spec in deps.items():
                if dep_name == 'rcgen':
                    if not isinstance(spec, dict) or spec.get('default-features') is not False:
                        violations.append(
                            f'[cargo-invariants] {label}: `rcgen` in [{section}] must set '
                            '`default-features = false` with `aws_lc_rs` (default enables `ring`)'
                        )
                if isinstance(spec, dict):
                    feats = set(spec.get('features', ()))
                    bad_feats = sorted(
                        f for f in feats if f in {'ring', 'tls-rustls', 'tls-rustls-ring'}
                    )
                    if bad_feats:
                        violations.append(
                            f'[cargo-invariants] {label}: `{dep_name}` in [{section}] enables '
                            f'forbidden ring feature(s) {bad_feats}; consolidate on `aws-lc-rs`'
                        )

    _check_no_ring_deps('Cargo.toml', workspace)

    # Every workspace member inherits [lints] workspace = true and avoids `ring`
    members = workspace.get('members', [])
    if not members:
        violations.append('[cargo-invariants] Cargo.toml: [workspace].members is empty')
    for member in members:
        member_toml_path = root / member / 'Cargo.toml'
        if not member_toml_path.is_file():
            violations.append(f'[cargo-invariants] missing workspace member manifest: {member}/Cargo.toml')
            continue
        member_manifest = tomllib.loads(member_toml_path.read_text(encoding='utf-8'))
        if member_manifest.get('lints', {}).get('workspace') is not True:
            violations.append(
                f'[cargo-invariants] {member}/Cargo.toml: must inherit workspace lints with '
                '`[lints]\\nworkspace = true`'
            )
        _check_no_ring_deps(f'{member}/Cargo.toml', member_manifest)

    return violations


# ---------------------------------------------------------------------------
# Runner
# ---------------------------------------------------------------------------

CHECKS: tuple[tuple[str, Callable[[Path], list[str]]], ...] = (
    ('no-raw-urlopen', check_no_raw_urlopen),
    ('no-retired-viewers', check_no_retired_viewers),
    ('no-duplicate-sql-lexer', check_no_duplicate_sql_lexer),
    ('dash-import-cycles', check_dash_import_cycles),
    ('no-sync-workarounds', check_no_sync_workarounds),
    ('sql-canonical-schema', check_sql_canonical_schema),
    ('integration-test-ceilings', check_integration_test_ceilings),
    ('cargo-invariants', check_cargo_invariants),
)


def run_all_checks(root: Path = ROOT) -> list[str]:
    ast_cache: dict[Path, ast.AST] = {}
    violations: list[str] = []
    for name, check_fn in CHECKS:
        if name in ('no-raw-urlopen', 'no-retired-viewers', 'dash-import-cycles'):
            violations.extend(check_fn(root, ast_cache=ast_cache))
        else:
            violations.extend(check_fn(root))
    return violations


def main() -> int:
    violations = run_all_checks(ROOT)
    if violations:
        for line in violations:
            print(line, file=sys.stderr)
        print(
            f'architecture check failed: {len(violations)} violation(s) across {len(CHECKS)} checks',
            file=sys.stderr,
        )
        return 1
    print(f'architecture ok: {len(CHECKS)} checks passed')
    return 0


if __name__ == '__main__':
    raise SystemExit(main())
