#!/usr/bin/env python3
"""
Structural guardrails for the Meerschaum workspace.

    python3 scripts/check_architecture.py                     # every rule; exits 1 on a violation
    python3 scripts/check_architecture.py --ratchet-base REV  # refuse ratchet entries added or raised since REV
    python3 scripts/check_architecture.py --update-ratchets   # lower ratchet numbers to the tree (never raises)
    python3 scripts/check_architecture.py --require-cargo     # fail instead of skipping without `cargo`

Without `--ratchet-base` the base is `$MRSM_ARCH_RATCHET_BASE`, else the merge base with `origin/main`
when the clone has it; with neither, ratchets only compare the tree with their data files.

Stdlib-only and about a second. The rules:

 1. local-server-http: no raw HTTP client in `meerschaum/api/` outside the hardened transport.
 2. single-home: helpers that exist exactly once (the Console viewer, the SQL guard's lexer, the
    engine-state reset) are not defined a second time elsewhere.
 3. dash-import-cycles: no import cycle in `meerschaum/api/dash/` beyond a shrinking list.
 4. sql-canonical-schema: the SQL instance connector checks its schema through `ensure_schema`.
 5. test-layout: every Rust test module is compiled (declared from its suite's `main.rs`), every
    single-file test binary is listed with its reason, the per-crate binary ceilings hold, and no
    test that shares a binary with other modules changes the process environment.
 6. test-references: every `--test <name>`, `tests/<path>.rs` and `crates/<crate>/src/<path>.rs`
    (or `crates/<crate>/src/<dir>/`) named by CI scripts, workflows and docs exists; a crate's own
    docs may write `src/<path>.rs`, and Markdown may shorten the crate (`cli/tests/<path>.rs`).
    Comments in Rust and Python code are held to the root-relative form.
 7. crate-layering: `crates/README.md`'s dependency table is the workspace graph, and every edge
    points down the documented layer order.
 8. heavy-imports: no module-level import of pandas, polars, sqlalchemy, numpy, dash, plotly or
    fastapi in `meerschaum/`, except in packages that are only ever imported lazily.
 9. ratchets: oversized files, development-history file names, `print(` calls and stale test
    references in docs other agents own. Each is a data file under `scripts/architecture/` that may
    only lose entries or lower numbers.
10. cargo-invariants: release profiles, slim dependency features, workspace lints, and no `ring`
    built into any target (resolved with `cargo tree` when cargo is available).
11. module-paths: the file tree is the module tree: no `#[path]` attribute in `crates/*/src` except
    the platform selections and shared test files listed with their reasons.
"""

from __future__ import annotations

import argparse
import ast
import json
import os
import re
import shutil
import subprocess
import sys
import tomllib
from collections import defaultdict, deque
from pathlib import Path
from typing import Callable, Iterable

ROOT = Path(__file__).resolve().parent.parent
DATA_DIR_NAME = 'scripts/architecture'


def _rel(root: Path, path: Path) -> str:
    return path.relative_to(root).as_posix()


def _parse_py(path: Path, rel: str, cache: dict[Path, ast.AST] | None, source: str | None = None) -> ast.AST:
    if cache is not None and path in cache:
        return cache[path]
    tree = ast.parse(path.read_text(encoding='utf-8') if source is None else source, filename=rel)
    if cache is not None:
        cache[path] = tree
    return tree


_RUST_TOKENS = re.compile(r'"(?:\\.|[^"\\])*"|\'(?:\\.|[^\'\\\n])\'|//[^\n]*|/\*.*?\*/', re.S)


def _strip_rust_comments(text: str) -> str:
    """Blank out `//` and `/* */` comments, keeping string literals and line numbers."""
    def blank(match: re.Match) -> str:
        token = match.group(0)
        return token if token[0] in '"\'' else '\n' * token.count('\n')
    return _RUST_TOKENS.sub(blank, text)


def _crate_dirs(root: Path) -> list[Path]:
    crates = root / 'crates'
    if not crates.is_dir():
        return []
    return [path for path in sorted(crates.iterdir()) if (path / 'Cargo.toml').is_file()]


# ---------------------------------------------------------------------------
# Rule 1: local-server HTTP goes through the hardened transport
# ---------------------------------------------------------------------------

RAW_HTTP_ALLOWLIST: dict[str, str] = {
    'meerschaum/api/host/_server_client.py': 'the hardened local-server transport (no proxies, no redirects)',
    'meerschaum/api/dash/connector_listing.py': 'probes external `api:` connector URLs, never the local server',
}
RAW_HTTP_MODULES = ('http.client', 'requests', 'httpx', 'aiohttp', 'urllib3')
RAW_URLLIB_NAMES = frozenset({'urlopen', 'build_opener', 'install_opener', 'OpenerDirector'})
RAW_HTTP_ATTRIBUTES = RAW_URLLIB_NAMES | {'HTTPConnection', 'HTTPSConnection'}
_RAW_HTTP_HINT = re.compile(
    r'urlopen|build_opener|install_opener|OpenerDirector|HTTPS?Connection|http\.client|'
    r'\b(requests|httpx|aiohttp|urllib3)\b'
)


def _is_raw_http_module(name: str) -> bool:
    return any(name == mod or name.startswith(mod + '.') for mod in RAW_HTTP_MODULES)


def check_local_server_http(root: Path = ROOT, *, ast_cache: dict[Path, ast.AST] | None = None) -> list[str]:
    """Rule 1: no raw HTTP client (urllib openers, `http.client`, requests, httpx, ...) in `meerschaum/api/`."""
    api_dir = root / 'meerschaum' / 'api'
    if not api_dir.is_dir():
        return [f'[local-server-http] missing directory: {_rel(root, api_dir)}']
    violations: list[str] = []
    for path in sorted(api_dir.rglob('*.py')):
        rel = _rel(root, path)
        if rel in RAW_HTTP_ALLOWLIST:
            continue
        source = path.read_text(encoding='utf-8')
        if not _RAW_HTTP_HINT.search(source):
            continue
        hits: dict[int, str] = {}
        for node in ast.walk(_parse_py(path, rel, ast_cache, source)):
            reason = None
            if isinstance(node, ast.Import):
                reason = next((f'imports `{a.name}`' for a in node.names if _is_raw_http_module(a.name)), None)
            elif isinstance(node, ast.ImportFrom) and node.level == 0 and node.module:
                if _is_raw_http_module(node.module):
                    reason = f'imports from `{node.module}`'
                elif node.module == 'http' and any(a.name == 'client' for a in node.names):
                    reason = 'imports `http.client`'
                elif node.module == 'urllib.request' and any(
                    a.name in RAW_URLLIB_NAMES or a.name == '*' for a in node.names
                ):
                    reason = 'imports a `urllib.request` opener'
            elif isinstance(node, ast.Attribute):
                if node.attr in RAW_HTTP_ATTRIBUTES:
                    reason = f'uses `.{node.attr}`'
                elif node.attr == 'client' and isinstance(node.value, ast.Name) and node.value.id == 'http':
                    reason = 'uses `http.client`'
            elif isinstance(node, ast.Call) and isinstance(node.func, ast.Name):
                if node.func.id in RAW_URLLIB_NAMES:
                    reason = f'calls `{node.func.id}(...)`'
                elif (
                    node.func.id == 'attempt_import' and node.args
                    and isinstance(node.args[0], ast.Constant) and isinstance(node.args[0].value, str)
                    and _is_raw_http_module(node.args[0].value)
                ):
                    reason = f'imports `{node.args[0].value}` with `attempt_import`'
            if reason and node.lineno not in hits:
                hits[node.lineno] = reason
        violations.extend(
            f'[local-server-http] {rel}:{line}: {reason}; call the local server through '
            '`meerschaum.api.host._server_client` (the hardened transport); allowlisted: '
            + ', '.join(sorted(RAW_HTTP_ALLOWLIST))
            for line, reason in sorted(hits.items())
        )
    return violations


# ---------------------------------------------------------------------------
# Rule 2: single-home definitions
# ---------------------------------------------------------------------------

#: (language, names, the one directory or file allowed to define them, what they are). A name
#: ending in `*` is a prefix.
SINGLE_HOME_DEFINITIONS: tuple[tuple[str, tuple[str, ...], str, str], ...] = (
    ('py', ('class Viewer',), 'meerschaum/api/dash/viewer.py', 'the Console viewer'),
    ('py', ('def get_viewer', 'def current_viewer'), 'meerschaum/api/dash/viewer.py',
     "resolving a request's Console viewer"),
    ('rs', ('fn dollar_tag_len', 'fn strip_sql_noise', 'fn is_read_only_query', 'fn is_bare_identifier*',
            'fn check_read_only*', 'fn check_internal_tables*'),
     'crates/meerschaum-server/src/sql_guard/', "the SQL guard's lexer and read-only checks"),
    ('rs', ('fn forget_engine_state_after',), 'crates/meerschaum-connector/src/',
     'resetting engine state after a rewrite'),
    ('rs', ('fn thousands', 'fn fmt_thousands', 'fn format_timespan', 'fn quote_plus', 'fn json_float',
            'fn py_round', 'fn ratio_reduce', 'fn ratio_distribute', 'fn collapse_widths'),
     'crates/meerschaum-core/src/pyfmt.rs',
     "Python's number, duration and URL text and rich's table column widths"),
    ('rs', ('fn negation_prefix', 'fn separate_negation_values', 'fn separate_negation'),
     'crates/meerschaum-core/src/static_config.rs', 'splitting negated key filters'),
    ('rs', ('fn fetch_replace',), 'crates/meerschaum-core/src/pipe/mod.rs',
     "a pipe's `fetch: {replace: true}`"),
    ('rs', ('fn trimmed_text',), 'crates/meerschaum-core/src/config/grafana_readonly.rs',
     'a config value as trimmed text'),
    ('rs', ('fn backtrack_params',), 'crates/meerschaum-kernel/src/diff.rs',
     "a backtrack request's `params`"),
    ('rs', ('fn sort_json_keys',), 'crates/meerschaum-kernel/src/serialize.rs',
     'sorting JSON object keys'),
    ('rs', ('fn precedes', 'fn bound_lt', 'fn coerce_tz', 'fn checked_add_interval', 'fn optional_py_str'),
     'crates/meerschaum-connector/src/date_bound.rs',
     'comparing, timezone-coercing and offsetting date bounds'),
    ('rs', ('fn from_attribute_map', 'fn sql_attributes'), 'crates/meerschaum-connector-sql/src/uri.rs',
     "reading a SQL connector's attributes from a map"),
)


def _definition_regex(language: str, names: tuple[str, ...]) -> re.Pattern:
    parts = []
    for name in names:
        keyword, ident = name.split()
        ident = re.escape(ident.rstrip('*')) + (r'\w*' if ident.endswith('*') else '')
        parts.append(rf'{keyword}\s+{ident}\b')
    anchor = '^' if language == 'py' else r'\b'
    return re.compile(anchor + '(?:' + '|'.join(parts) + ')', re.M)


def check_single_home(root: Path = ROOT) -> list[str]:
    """Rule 2: a helper that exists once is defined once, in its home."""
    violations: list[str] = []
    py_files = sorted((root / 'meerschaum').rglob('*.py')) if (root / 'meerschaum').is_dir() else []
    rs_files = sorted(root.glob('crates/*/src/**/*.rs'))
    texts: dict[Path, str] = {}
    for language, names, home, what in SINGLE_HOME_DEFINITIONS:
        regex = _definition_regex(language, names)
        needles = [name.split()[1].rstrip('*') for name in names]
        defined_at_home = False
        for path in py_files if language == 'py' else rs_files:
            if path not in texts:
                texts[path] = path.read_text(encoding='utf-8')
            if not any(needle in texts[path] for needle in needles) or not regex.search(texts[path]):
                continue
            if language == 'rs':
                texts[path] = _strip_rust_comments(texts[path])
            rel = _rel(root, path)
            for match in regex.finditer(texts[path]):
                if rel == home or (home.endswith('/') and rel.startswith(home)):
                    defined_at_home = True
                    continue
                line = texts[path].count('\n', 0, match.start()) + 1
                violations.append(
                    f'[single-home] {rel}:{line}: defines `{match.group(0).split()[-1]}`, '
                    f'which belongs only in {home} ({what}); call that one instead'
                )
        if not defined_at_home and (root / home.rstrip('/')).exists():
            violations.append(f'[single-home] {home}: no longer defines {what} ({", ".join(names)}); update the rule')
    return violations


# ---------------------------------------------------------------------------
# Rule 3: import cycles in meerschaum/api/dash/
# ---------------------------------------------------------------------------

PLUGIN_CLUSTER_EXACT: frozenset[str] = frozenset({
    'meerschaum.api.dash.plugins',
    'meerschaum.api.dash.plugin_sources',
    'meerschaum.api.dash.plugin_upload',
    'meerschaum.api.dash.plugin_publish',
    'meerschaum.api.dash.pages.plugins',
    'meerschaum.api.dash.callbacks.plugins',
})


def _is_plugin_cluster_module(mod: str) -> bool:
    return mod in PLUGIN_CLUSTER_EXACT or mod == 'meerschaum.api.dash._plugins' or mod.startswith(
        'meerschaum.api.dash._plugins.'
    )


def _file_to_module(root: Path, path: Path) -> str:
    parts = list(path.relative_to(root).with_suffix('').parts)
    if parts[-1] == '__init__':
        parts = parts[:-1]
    return '.'.join(parts)


def _import_base(cur_mod: str, is_pkg: bool, node: ast.ImportFrom) -> str | None:
    if not node.level:
        return node.module or ''
    parts = cur_mod.split('.')
    if not is_pkg:
        parts = parts[:-1]
    if node.level > 1:
        if node.level - 1 > len(parts):
            return None
        parts = parts[: -(node.level - 1)]
    return '.'.join(parts + ([node.module] if node.module else []))


def _resolve_import_targets(cur_mod: str, is_pkg: bool, node: ast.Import | ast.ImportFrom, known: set[str]) -> list[str]:
    if isinstance(node, ast.Import):
        return [alias.name for alias in node.names if alias.name in known]
    base = _import_base(cur_mod, is_pkg, node)
    if not base:
        return []
    targets = []
    for alias in node.names:
        if alias.name != '*' and f'{base}.{alias.name}' in known:
            targets.append(f'{base}.{alias.name}')
        elif base in known:
            targets.append(base)
    return targets


def _iter_import_nodes(node: ast.AST):
    """Every `import` statement in statement bodies, function-local ones included."""
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


def _build_dash_import_graph(root: Path, ast_cache: dict[Path, ast.AST] | None = None):
    dash_dir = root / 'meerschaum' / 'api' / 'dash'
    mods = {_file_to_module(root, p): p for p in sorted(dash_dir.rglob('*.py'))}
    known = set(mods)
    graph: dict[str, set[str]] = defaultdict(set)
    edge_locs: dict[tuple[str, str], list[int]] = defaultdict(list)
    for mod, path in mods.items():
        tree = _parse_py(path, _rel(root, path), ast_cache)
        for node in _iter_import_nodes(tree):
            for target in _resolve_import_targets(mod, path.name == '__init__.py', node, known):
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
    queue: deque[list[str]] = deque([[start]])
    seen = {start}
    while queue:
        path = queue.popleft()
        for nxt in sorted(adj.get(path[-1], ())):
            if nxt == goal:
                return path + [nxt]
            if nxt not in seen:
                seen.add(nxt)
                queue.append(path + [nxt])
    return None


def dash_cycle_edges(root: Path, ast_cache: dict[Path, ast.AST] | None = None) -> dict[str, int]:
    """Every import edge inside a cycle of `meerschaum/api/dash/`, as `"a -> b"` keys."""
    mods, graph, _ = _build_dash_import_graph(root, ast_cache)
    edges: dict[str, int] = {}
    for scc in _tarjan_sccs(set(mods), graph):
        members = set(scc)
        for u in scc:
            for v in graph.get(u, ()):
                if v in members:
                    edges[f'{u} -> {v}'] = 1
    return edges


def check_dash_import_cycles(
    root: Path = ROOT,
    *,
    ast_cache: dict[Path, ast.AST] | None = None,
) -> list[str]:
    """Rule 3: `sessions` never reaches `access`, the plugin cluster is acyclic, and cycles only shrink."""
    dash_dir = root / 'meerschaum' / 'api' / 'dash'
    if not dash_dir.is_dir():
        return [f'[dash-import-cycles] missing directory: {_rel(root, dash_dir)}']
    mods, graph, edge_locs = _build_dash_import_graph(root, ast_cache)
    violations: list[str] = []

    sessions_mod, access_mod = 'meerschaum.api.dash.sessions', 'meerschaum.api.dash.access'
    if sessions_mod in mods and access_mod in mods:
        path = _shortest_path(graph, sessions_mod, access_mod)
        if path is not None:
            steps = [f'{_rel(root, mods[u])}:{min(edge_locs[(u, v)])} -> {v}' for u, v in zip(path, path[1:])]
            violations.append(
                '[dash-import-cycles] meerschaum/api/dash/sessions.py must not import '
                f'meerschaum.api.dash.access (directly or transitively): {" ; ".join(steps)}'
            )

    plugin_mods = {m for m in mods if _is_plugin_cluster_module(m)}
    for scc in _tarjan_sccs(plugin_mods, graph):
        members = set(scc)
        edges = [
            f'{_rel(root, mods[u])}:{min(edge_locs[(u, v)])} -> {v}'
            for u in scc for v in sorted(graph.get(u, ())) if v in members
        ]
        violations.append(
            f'[dash-import-cycles] plugin cluster must have 0 cycles; found SCC ({", ".join(scc)}): '
            + ' ; '.join(edges)
        )
    return violations


# ---------------------------------------------------------------------------
# Rule 4: canonical schema checks in the SQL instance connector
# ---------------------------------------------------------------------------

#: Calls of `require_schema` in `trait_impl.rs` (the forwarding impl), however they are spelled.
REQUIRE_SCHEMA_CEILING = 0
_REQUIRE_SCHEMA_CALL = re.compile(r'(?:\.|::)\s*require_schema\s*\(')
_ENSURE_SCHEMA_RE = re.compile(r'\bfn\s+ensure_schema\b')
_BIND_PIPE_RE = re.compile(r'\bfn\s+bind_pipe\b')
_FORBIDDEN_SQL_STATE_RE = re.compile(r'\b(synced_columns|internal_pipe_states)\b')


def check_sql_canonical_schema(root: Path = ROOT) -> list[str]:
    """Rule 4: `ensure_schema` / `bind_pipe` are the schema seam; the forwarding impl adds no checks of its own."""
    violations: list[str] = []
    instance = root / 'crates' / 'meerschaum-connector-sql' / 'src' / 'instance'
    mod_rs, trait_impl = instance / 'mod.rs', instance / 'trait_impl.rs'
    for path in (mod_rs, trait_impl):
        if not path.is_file():
            violations.append(f'[sql-canonical-schema] missing file: {_rel(root, path)}')
    if violations:
        return violations
    code = _strip_rust_comments(mod_rs.read_text(encoding='utf-8'))
    for regex, name in ((_ENSURE_SCHEMA_RE, 'ensure_schema'), (_BIND_PIPE_RE, 'bind_pipe')):
        if not regex.search(code):
            violations.append(f'[sql-canonical-schema] {_rel(root, mod_rs)}: missing `fn {name}` definition')
    count = len(_REQUIRE_SCHEMA_CALL.findall(_strip_rust_comments(trait_impl.read_text(encoding='utf-8'))))
    if count > REQUIRE_SCHEMA_CEILING:
        violations.append(
            f'[sql-canonical-schema] {_rel(root, trait_impl)}: found {count} `require_schema(...)` calls '
            f'(ceiling {REQUIRE_SCHEMA_CEILING}); check the schema with `ensure_schema` / `bind_pipe`'
        )
    for path in sorted((root / 'crates' / 'meerschaum-connector-sql' / 'src').rglob('*.rs')):
        text = path.read_text(encoding='utf-8')
        if 'synced_columns' not in text and 'internal_pipe_states' not in text:
            continue
        stripped = _strip_rust_comments(text)
        for match in _FORBIDDEN_SQL_STATE_RE.finditer(stripped):
            line = stripped.count('\n', 0, match.start()) + 1
            violations.append(
                f'[sql-canonical-schema] {_rel(root, path)}:{line}: `{match.group(1)}` is a second copy of '
                'what the catalog already knows; read it through `ensure_schema`'
            )
    return violations


# ---------------------------------------------------------------------------
# Rule 5: Rust test layout
# ---------------------------------------------------------------------------

#: Integration test binaries per crate (`tests/*.rs` + `tests/*/main.rs`): bundle new tests into a suite.
INTEGRATION_TEST_CEILINGS: dict[str, int] = {
    'meerschaum': 1,
    'meerschaum-cli': 13,
    'meerschaum-compose': 1,
    'meerschaum-connector-api': 1,
    'meerschaum-connector-sql': 12,
    'meerschaum-connector-valkey': 1,
    'meerschaum-core': 2,
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

#: Every single-file test binary, and why it is not a module of its crate's suite.
SINGLE_FILE_TEST_BINARIES: dict[str, str] = {
    'crates/meerschaum/tests/surface.rs': "the front crate's only test binary",
    'crates/meerschaum-cli/tests/generated_tables.rs': 'runs the Python table generators (needs python3)',
    'crates/meerschaum-cli/tests/host_api_terminals.rs': 'custom harness: also the entry point of its confined children',
    'crates/meerschaum-cli/tests/host_plugin_discovery.rs': 'custom harness: also the entry point of its supervisor and workers',
    'crates/meerschaum-cli/tests/native_source_compiler.rs': 'custom harness: also the entry point of its confined compiler',
    'crates/meerschaum-cli/tests/registered_pg_source.rs': 'ignored acceptance on an owned PostgreSQL (scripts/ci/native_backends.py)',
    'crates/meerschaum-cli/tests/secret_config_writers.rs': 'sets a process environment variable that `MRSM{env:...}` reads',
    'crates/meerschaum-cli/tests/team_sessions.rs': 'custom harness that sets process environment variables',
    'crates/meerschaum-connector-sql/tests/duckdb_install.rs': 'downloads the DuckDB library into its own process',
    'crates/meerschaum-connector-sql/tests/duckdb_library.rs': 'loads one DuckDB library for the whole process',
    'crates/meerschaum-connector-sql/tests/duckdb_no_auto_install.rs': 'sets MRSM_NO_AUTO_INSTALL for the whole process',
    'crates/meerschaum-connector-sql/tests/instance_first_user.rs': 'run alone by scripts/ci/native_backends.py',
    'crates/meerschaum-connector-sql/tests/instance_has_rows.rs': 'run alone by scripts/ci/native_backends.py',
    'crates/meerschaum-connector-sql/tests/instance_pipe_data.rs': 'run alone by scripts/ci/native_backends.py',
    'crates/meerschaum-connector-sql/tests/instance_unique_keys.rs': 'run alone by scripts/ci/native_backends.py',
    'crates/meerschaum-connector-sql/tests/instance_user_tokens.rs': 'run alone by scripts/ci/native_backends.py',
    'crates/meerschaum-connector-sql/tests/instance_users.rs': 'run alone by scripts/ci/native_backends.py',
    'crates/meerschaum-core/tests/secret_config.rs': 'sets a process environment variable that `MRSM{env:...}` reads',
    'crates/meerschaum-py/tests/arrow_ffi.rs': 'Arrow C stream check without an interpreter',
    'crates/meerschaum-py/tests/no_interpreter.rs': 'needs a process where Python was never initialized',
}

_MOD_DECL = re.compile(r'mod[ \t]+(\w+)[ \t]*;')
_MOD_PREFIX = re.compile(r'[ \t]*(?:pub(?:\([^)]*\))?[ \t]+)?')
_PATH_ATTR = re.compile(r'#\[path\s*=\s*"([^"]+)"\s*\]')
_INCLUDE = re.compile(r'\binclude!\s*\(\s*"([^"]+)"\s*\)')
_ENV_MUTATION = re.compile(r'\b(set_var|remove_var)\s*\(')


def _test_roots(crate: Path) -> list[Path]:
    tests = crate / 'tests'
    roots = {p for p in tests.glob('*.rs') if p.is_file()} | {p for p in tests.glob('*/main.rs') if p.is_file()}
    manifest = tomllib.loads((crate / 'Cargo.toml').read_text(encoding='utf-8'))
    for target in manifest.get('test', []):
        if 'path' in target:
            roots.add(crate / target['path'])
    return sorted(roots)


def _module_files(root_file: Path, code_cache: dict[Path, str]) -> list[Path]:
    """Every file compiled into the test binary rooted at `root_file` (`mod`, `#[path]`, `include!`)."""
    seen: dict[Path, None] = {}
    stack = [(root_file, True)]
    while stack:
        path, owns_dir = stack.pop()
        if path in seen or not path.is_file():
            continue
        seen[path] = None
        if path not in code_cache:
            # A commented-out `mod x;` does not start its line, so comments need no stripping here.
            code_cache[path] = path.read_text(encoding='utf-8')
        code = code_cache[path]
        child_dir = path.parent if owns_dir else path.parent / path.stem
        for match in _MOD_DECL.finditer(code):
            line_start = code.rfind('\n', 0, match.start()) + 1
            if not _MOD_PREFIX.fullmatch(code, line_start, match.start()):
                continue
            name = match.group(1)
            attrs = []
            for line in reversed(code[max(0, line_start - 800):line_start].splitlines()[-8:]):
                if not line.strip().startswith('#['):
                    break
                attrs.append(line)
            explicit = _PATH_ATTR.search(' '.join(attrs))
            if explicit:
                stack.append(((path.parent / explicit.group(1)).resolve(), explicit.group(1).endswith('mod.rs')))
                continue
            for candidate, owns in ((child_dir / f'{name}.rs', False), (child_dir / name / 'mod.rs', True)):
                if candidate.is_file():
                    stack.append((candidate.resolve(), owns))
                    break
        for match in _INCLUDE.finditer(code):
            stack.append(((path.parent / match.group(1)).resolve(), True))
    return list(seen)


def count_integration_test_binaries(root: Path = ROOT) -> dict[str, int]:
    """Integration test binaries per crate."""
    return {crate.name: len(_test_roots(crate)) if (crate / 'tests').is_dir() else 0 for crate in _crate_dirs(root)}


def check_test_layout(root: Path = ROOT) -> list[str]:
    """Rule 5: every test file is compiled, singletons are intentional, ceilings hold, suites share no env."""
    violations: list[str] = []
    counts: dict[str, int] = {}
    code_cache: dict[Path, str] = {}
    for crate in _crate_dirs(root):
        tests = crate / 'tests'
        if not tests.is_dir():
            counts[crate.name] = 0
            continue
        roots = _test_roots(crate)
        counts[crate.name] = len(roots)
        compiled: set[Path] = set()
        for test_root in roots:
            files = _module_files(test_root.resolve(), code_cache)
            compiled.update(files)
            if test_root.name != 'main.rs':
                rel = _rel(root, test_root)
                if rel not in SINGLE_FILE_TEST_BINARIES:
                    violations.append(
                        f'[test-layout] {rel}: a new single-file test binary; make it a module of a suite '
                        '(`tests/<suite>/main.rs`), or list it with its reason in SINGLE_FILE_TEST_BINARIES'
                    )
                continue
            if len(files) > 1:
                for path in files:
                    if 'set_var' not in code_cache[path] and 'remove_var' not in code_cache[path]:
                        continue
                    stripped = _strip_rust_comments(code_cache[path])
                    for match in _ENV_MUTATION.finditer(stripped):
                        line = stripped.count('\n', 0, match.start()) + 1
                        violations.append(
                            f'[test-layout] {_rel(root, path)}:{line}: `{match.group(1)}` in a suite shared by '
                            f'{len(files)} files: pass the variable to the child (`Command::env`) or move the '
                            'test into a binary of its own'
                        )
        for path in sorted(tests.rglob('*.rs')):
            if 'fixtures' in path.relative_to(tests).parts:
                continue
            if path.resolve() not in compiled:
                violations.append(
                    f'[test-layout] {_rel(root, path)}: not compiled into any test binary; declare it with '
                    f'`mod {path.stem};` in its suite\'s main.rs (or delete it)'
                )
    for rel in sorted(SINGLE_FILE_TEST_BINARIES):
        if (root / 'crates').is_dir() and rel.split('/')[1] in counts and not (root / rel).is_file():
            violations.append(f'[test-layout] {rel}: listed in SINGLE_FILE_TEST_BINARIES but gone; drop the entry')
    for crate, count in sorted(counts.items()):
        ceiling = INTEGRATION_TEST_CEILINGS.get(crate, 0)
        if count > ceiling:
            violations.append(
                f'[test-layout] crates/{crate}/tests: {count} integration test binaries exceeds ceiling '
                f'{ceiling}; bundle new tests into an existing suite (`tests/<suite>/main.rs`)'
            )
        elif count < ceiling:
            violations.append(
                f'[test-layout] crates/{crate}/tests: {count} integration test binaries, below its ceiling '
                f'{ceiling}: lower INTEGRATION_TEST_CEILINGS[{crate!r}] to {count}'
            )
    total = sum(counts.values())
    if total > TOTAL_INTEGRATION_TEST_CEILING:
        violations.append(
            f'[test-layout] workspace total: {total} integration test binaries exceeds ceiling '
            f'{TOTAL_INTEGRATION_TEST_CEILING}'
        )
    return violations


# ---------------------------------------------------------------------------
# Rule 6: test targets and paths named by CI scripts, workflows and docs exist
# ---------------------------------------------------------------------------

REFERENCE_GLOBS = ('scripts/**/*', '.github/**/*', '.config/*', 'docs/**/*.md', 'crates/**/*.md', '*.md')
_TEST_FLAG = re.compile(r'--test[ =]([A-Za-z0-9_-]+)')
_PACKAGE_FLAG = re.compile(r'(?:-p|--package)[ =](meerschaum[\w-]*)')
_TEST_PATH = re.compile(r'(?<![\w/.<*-])((?:crates/([\w-]+)/)?tests/([\w./-]+?)\.rs)\b')
_SOURCE_PATH = re.compile(r'(?<![\w/.<*{$-])((?:crates/([\w-]+)/)?src/[\w./-]+?\.rs)(?![\w/])')
_SOURCE_DIR = re.compile(r'(?<![\w/.<*{$-])(crates/[\w-]+/(?:src|tests)/[\w/-]+/)(?=[`\s),;:]|$)')
# Source and test code is scanned only for root-relative paths: its strings are full of look-alikes.
CODE_REFERENCE_GLOBS = ('crates/**/*.rs', 'crates/**/*.py', 'tests/**/*.py', 'meerschaum/**/*.py')
CODE_REFERENCE_SKIP = frozenset({'tests/test_architecture_guards.py'})
_ROOT_RUST_PATH = re.compile(r'(?<![\w/.<*{$-])(crates/meerschaum[\w-]*/(?:src|tests)/[\w./-]+?\.rs)(?![\w/])')
_REFERENCE_SUFFIXES = {'', '.md', '.py', '.sh', '.toml', '.yaml', '.yml', '.json', '.txt'}


def _reference_files(root: Path) -> list[Path]:
    files: set[Path] = set()
    prefix = len(root.parts)
    for pattern in REFERENCE_GLOBS:
        for path in root.glob(pattern):
            parts = path.parts[prefix:]
            if path.suffix in _REFERENCE_SUFFIXES and path.is_file() and path.suffix in _REFERENCE_SUFFIXES and 'target' not in parts \
                    and 'fixtures' not in parts and not any(p.startswith('.venv') for p in parts) \
                    and parts[:2] != ('scripts', 'architecture'):
                files.add(path)
    return sorted(files)


def stale_test_references(root: Path) -> dict[str, list[str]]:
    """`file -> ["line: reference", ...]` for every reference to a test target, test file or source file that does not exist."""
    crates = _crate_dirs(root)
    targets: dict[str, set[str]] = defaultdict(set)
    for crate in crates:
        for test_root in _test_roots(crate) if (crate / 'tests').is_dir() else []:
            targets[test_root.parent.name if test_root.name == 'main.rs' else test_root.stem].add(crate.name)
        for target in tomllib.loads((crate / 'Cargo.toml').read_text(encoding='utf-8')).get('test', []):
            targets[target['name']].add(crate.name)
    # Docs shorten `crates/meerschaum-<name>/` to `<name>/`.
    shorts = sorted((c.name.removeprefix('meerschaum-') for c in crates if c.name != 'meerschaum'), key=len, reverse=True)
    short_path = re.compile(
        r'(?<![\w/.<*{$-])((' + '|'.join(map(re.escape, shorts)) + r')/((?:src|tests)/[\w./-]+?\.rs))(?![\w/])'
    )
    stale: dict[str, list[str]] = {}
    for path in _reference_files(root):
        try:
            text = path.read_text(encoding='utf-8')
        except UnicodeDecodeError:
            continue
        if '--test' not in text and 'tests/' not in text and 'src/' not in text:
            continue
        rel = _rel(root, path)
        parts = path.relative_to(root).parts
        own_crate = parts[1] if parts[0] == 'crates' and len(parts) > 2 else None
        for lineno, line in enumerate(text.splitlines(), 1):
            packages = set(_PACKAGE_FLAG.findall(line))
            for match in _TEST_FLAG.finditer(line):
                name = match.group(1)
                if not targets.get(name) or (packages and not targets[name] & packages):
                    stale.setdefault(rel, []).append(f'{lineno}: --test {name}')
            for match in _TEST_PATH.finditer(line):
                full, crate, _ = match.groups()
                if crate:
                    exists = (root / full).exists()
                else:
                    candidates = [root / 'crates' / own_crate / full] if own_crate else []
                    exists = any(p.exists() for p in candidates) or (
                        not own_crate and any((c / full).exists() for c in crates)
                    )
                if not exists:
                    stale.setdefault(rel, []).append(f'{lineno}: {full}')
            for match in _SOURCE_PATH.finditer(line):
                full, crate = match.groups()
                # A bare `src/...` is a path inside the crate whose doc names it; elsewhere it is ambiguous.
                base = root if crate else (root / 'crates' / own_crate if own_crate else None)
                if base is not None and not (base / full).exists():
                    stale.setdefault(rel, []).append(f'{lineno}: {full}')
            for match in _SOURCE_DIR.finditer(line):
                if not (root / match.group(1)).is_dir():
                    stale.setdefault(rel, []).append(f'{lineno}: {match.group(1)}')
            for match in short_path.finditer(line) if path.suffix == '.md' else ():
                if not (root / 'crates' / f'meerschaum-{match.group(2)}' / match.group(3)).exists():
                    stale.setdefault(rel, []).append(f'{lineno}: {match.group(1)}')
    prefix = len(root.parts)
    for pattern in CODE_REFERENCE_GLOBS:
        for path in root.glob(pattern):
            parts = path.parts[prefix:]
            if _skip_dir(parts[:-1]) or 'fixtures' in parts or '/'.join(parts) in CODE_REFERENCE_SKIP:
                continue
            try:
                text = path.read_text(encoding='utf-8')
            except (UnicodeDecodeError, OSError):
                continue
            if 'crates/meerschaum' not in text:
                continue
            for lineno, line in enumerate(text.splitlines(), 1):
                for match in _ROOT_RUST_PATH.finditer(line):
                    if not (root / match.group(1)).exists():
                        stale.setdefault('/'.join(parts), []).append(f'{lineno}: {match.group(1)}')
    return stale


def check_test_references(
    root: Path = ROOT,
    *,
    data_dir: Path | None = None,
    stale: dict[str, list[str]] | None = None,
) -> list[str]:
    """Rule 6: `--test <name>`, `tests/<path>.rs` and `src/<path>.rs` in scripts, workflows and docs resolve."""
    allowed = _load_ratchet(data_dir or root / DATA_DIR_NAME, 'stale_test_references')
    violations: list[str] = []
    stale = stale_test_references(root) if stale is None else stale
    for rel, refs in sorted(stale.items()):
        if rel in allowed:
            continue
        violations.extend(
            f'[test-references] {rel}:{ref.split(":", 1)[0]}: `{ref.split(": ", 1)[1]}` names no test target or '
            'file; Rust tests are suites (`--test <suite> <module>::`, `tests/<suite>/<module>.rs`) and a '
            'source file that became a directory is cited by the file that now holds the item'
            for ref in refs
        )
    return violations


# ---------------------------------------------------------------------------
# Rule 7: crate layering
# ---------------------------------------------------------------------------

_LAYER_ROW = re.compile(r'^\|\s*`(meerschaum[\w-]*)`\s*\|\s*(\d+)\s*\|([^|]*)\|')


def workspace_graph(root: Path) -> dict[str, set[str]]:
    """Normal and build dependencies between workspace crates (optional ones included), from the manifests."""
    graph: dict[str, set[str]] = {}
    for crate in _crate_dirs(root):
        manifest = tomllib.loads((crate / 'Cargo.toml').read_text(encoding='utf-8'))
        name = manifest.get('package', {}).get('name', crate.name)
        tables = [manifest.get('dependencies', {}), manifest.get('build-dependencies', {})]
        for target in manifest.get('target', {}).values():
            tables += [target.get('dependencies', {}), target.get('build-dependencies', {})]
        deps: set[str] = set()
        for table in tables:
            for dep, spec in table.items():
                real = spec.get('package', dep) if isinstance(spec, dict) else dep
                if real.startswith('meerschaum'):
                    deps.add(real)
        graph[name] = deps
    return graph


def _short(name: str) -> str:
    return name.removeprefix('meerschaum-') if name != 'meerschaum' else name


def documented_layers(root: Path) -> tuple[dict[str, int], dict[str, set[str]]] | None:
    readme = root / 'crates' / 'README.md'
    if not readme.is_file():
        return None
    layers: dict[str, int] = {}
    deps: dict[str, set[str]] = {}
    for line in readme.read_text(encoding='utf-8').splitlines():
        match = _LAYER_ROW.match(line)
        if match:
            crate, layer, cell = match.groups()
            layers[crate] = int(layer)
            cell = cell.strip()
            deps[crate] = set() if cell in ('', 'none') else {
                ('meerschaum' if d.strip() == 'meerschaum' else 'meerschaum-' + d.strip())
                for d in cell.split(',') if d.strip()
            }
    return layers, deps


def check_crate_layering(root: Path = ROOT) -> list[str]:
    """Rule 7: the README's table is the real graph, and every dependency points to a lower layer."""
    if not _crate_dirs(root):
        return []
    documented = documented_layers(root)
    if documented is None:
        return ['[crate-layering] crates/README.md: missing']
    layers, table = documented
    graph = workspace_graph(root)
    violations: list[str] = []
    fmt = lambda names: ', '.join(sorted(_short(n) for n in names)) or 'none'  # noqa: E731
    for crate in sorted(set(graph) | set(table)):
        if crate not in graph:
            violations.append(f'[crate-layering] crates/README.md: `{crate}` is documented but is not a workspace crate')
            continue
        if crate not in table:
            violations.append(f'[crate-layering] crates/README.md: add a row for `{crate}` (layer, then: {fmt(graph[crate])})')
            continue
        if table[crate] != graph[crate]:
            violations.append(
                f'[crate-layering] crates/README.md: the `{crate}` row says {fmt(table[crate])}; '
                f'its Cargo.toml says {fmt(graph[crate])}'
            )
        for dep in sorted(graph[crate]):
            if dep in layers and crate in layers and layers[dep] >= layers[crate]:
                violations.append(
                    f'[crate-layering] crates/{crate}/Cargo.toml: `{crate}` (layer {layers[crate]}) depends on '
                    f'`{dep}` (layer {layers[dep]}): dependencies point down the layers only'
                )
    return violations


# ---------------------------------------------------------------------------
# Rule 8: heavy imports stay lazy
# ---------------------------------------------------------------------------

HEAVY_PACKAGES = ('pandas', 'polars', 'sqlalchemy', 'numpy', 'dash', 'plotly', 'fastapi')
#: Packages whose every module may import these at module level, because nothing outside the package
#: imports it at module level (checked below): `meerschaum.api` is the API host, loaded by `start api`.
LAZY_PACKAGES: dict[str, frozenset[str]] = {
    'meerschaum.api': frozenset({'dash', 'plotly', 'fastapi'}),
}
_HEAVY_HINT = re.compile(r'\b(?:import|from)\s+(?:' + '|'.join(HEAVY_PACKAGES) + r')\b')
_LAZY_PACKAGE_HINT = re.compile(r'meerschaum\.api\b|import[ \t][^\n]*\bapi\b|from[ \t]+\.+api\b')


def _is_type_checking(test: ast.expr) -> bool:
    return (isinstance(test, ast.Name) and test.id == 'TYPE_CHECKING') or (
        isinstance(test, ast.Attribute) and test.attr == 'TYPE_CHECKING'
    )


def _module_level_imports(tree: ast.Module) -> Iterable[ast.Import | ast.ImportFrom]:
    """Imports that run when the module is imported (not in functions or `if TYPE_CHECKING:`)."""
    stack: list[ast.stmt] = list(reversed(tree.body))
    while stack:
        node = stack.pop()
        if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef)):
            continue
        if isinstance(node, (ast.Import, ast.ImportFrom)):
            yield node
            continue
        if isinstance(node, ast.If) and _is_type_checking(node.test):
            stack.extend(reversed(node.orelse))
            continue
        for attr in ('body', 'orelse', 'finalbody', 'handlers', 'cases'):
            children = getattr(node, attr, None)
            if isinstance(children, list):
                stack.extend(reversed(children))


def _in_package(module: str, package: str) -> bool:
    return module == package or module.startswith(package + '.')


def check_heavy_imports(root: Path = ROOT, *, ast_cache: dict[Path, ast.AST] | None = None) -> list[str]:
    """Rule 8: optional heavy packages are imported inside functions, or only in lazily imported packages."""
    package_dir = root / 'meerschaum'
    if not package_dir.is_dir():
        return []
    violations: list[str] = []
    lazy_prefixes = tuple(LAZY_PACKAGES)
    for path in sorted(package_dir.rglob('*.py')):
        rel = _rel(root, path)
        source = path.read_text(encoding='utf-8')
        module = _file_to_module(root, path)
        lazy = next((p for p in lazy_prefixes if _in_package(module, p)), None)
        if not _HEAVY_HINT.search(source) and (lazy or 'api' not in source or not _LAZY_PACKAGE_HINT.search(source)):
            continue
        tree = _parse_py(path, rel, ast_cache, source)
        for node in _module_level_imports(tree):
            if isinstance(node, ast.Import):
                names = [alias.name for alias in node.names]
            else:
                base = _import_base(module, path.name == '__init__.py', node) or ''
                names = [base] + [f'{base}.{alias.name}' for alias in node.names]
            for name in names:
                top = name.split('.')[0]
                if top in HEAVY_PACKAGES and not (lazy and top in LAZY_PACKAGES[lazy]):
                    violations.append(
                        f'[heavy-imports] {rel}:{node.lineno}: module-level import of `{name}`; import it '
                        'inside the function that needs it (`attempt_import`) so `import meerschaum` stays light'
                    )
                    break
                outside = next((p for p in lazy_prefixes if _in_package(name, p)), None)
                if outside and not lazy:
                    violations.append(
                        f'[heavy-imports] {rel}:{node.lineno}: module-level import of `{name}`, a package that '
                        f'imports {", ".join(sorted(LAZY_PACKAGES[outside]))} on import; import it inside a function'
                    )
                    break
    return violations


# ---------------------------------------------------------------------------
# Rule 9: shrink-only ratchets (scripts/architecture/*.json)
# ---------------------------------------------------------------------------

RUST_LINE_LIMIT = 1700
PYTHON_LINE_LIMIT = 1000
_HISTORY_COMPONENT = re.compile(r'(?:^|_)(?:w\d+[a-z]*|rc\d+|fx\d+|wave\w*)(?:_|$)', re.I)
HISTORY_NAME_ROOTS = ('crates', 'meerschaum', 'tests', 'scripts', 'docs')

RATCHETS: dict[str, str] = {
    'oversize_files': f'Rust files over {RUST_LINE_LIMIT} lines and Python files (meerschaum/, tests/) over '
                      f'{PYTHON_LINE_LIMIT} lines',
    'history_names': 'files and directories named after development history (w<N>_, rc<N>_, fx<N>, wave)',
    'print_calls': '`print(` calls per file in meerschaum/ (use warn / info / dprint)',
    'stale_test_references': 'docs owned elsewhere that still name test or source files or targets that moved',
    'dash_cycle_edges': 'import edges inside a cycle of meerschaum/api/dash/',
}


def _load_ratchet(data_dir: Path, name: str) -> dict[str, int]:
    path = data_dir / f'{name}.json'
    if not path.is_file():
        return {}
    data = json.loads(path.read_text(encoding='utf-8'))
    return {key: int(value) for key, value in data.items() if not key.startswith('_')}


def _write_ratchet(data_dir: Path, name: str, entries: dict[str, int]) -> None:
    data_dir.mkdir(parents=True, exist_ok=True)
    body = {'_about': RATCHETS[name] + '. This list may only shrink: lower a number or drop an entry.'}
    body.update(sorted(entries.items()))
    (data_dir / f'{name}.json').write_text(json.dumps(body, indent=1) + '\n', encoding='utf-8')


def _skip_dir(parts: tuple[str, ...]) -> bool:
    return any(p in ('target', '__pycache__', 'node_modules') or p.startswith('.') for p in parts)


def measure_ratchets(
    root: Path,
    ast_cache: dict[Path, ast.AST] | None = None,
    stale: dict[str, list[str]] | None = None,
) -> dict[str, dict[str, int]]:
    oversize: dict[str, int] = {}
    for pattern, limit in (('crates/**/*.rs', RUST_LINE_LIMIT), ('meerschaum/**/*.py', PYTHON_LINE_LIMIT),
                           ('tests/**/*.py', PYTHON_LINE_LIMIT)):
        for path in root.glob(pattern):
            if _skip_dir(path.relative_to(root).parts[:-1]):
                continue
            data = path.read_bytes()
            lines = data.count(b'\n') + (1 if data and not data.endswith(b'\n') else 0)
            if lines > limit:
                oversize[_rel(root, path)] = lines
    history: dict[str, int] = {}
    for top in HISTORY_NAME_ROOTS:
        base = root / top
        if not base.is_dir():
            continue
        for dirpath, dirnames, filenames in os.walk(base):
            rel_dir = Path(dirpath).relative_to(root)
            dirnames[:] = [d for d in dirnames if not _skip_dir((d,))]
            for name in dirnames + filenames:
                if _HISTORY_COMPONENT.search(Path(name).stem if name in filenames else name):
                    history[(rel_dir / name).as_posix()] = 1
            # A named directory stands for everything inside it.
            dirnames[:] = [d for d in dirnames if not _HISTORY_COMPONENT.search(d)]
    prints: dict[str, int] = {}
    for path in sorted((root / 'meerschaum').rglob('*.py')) if (root / 'meerschaum').is_dir() else []:
        source = path.read_text(encoding='utf-8')
        if 'print(' not in source:
            continue
        rel = _rel(root, path)
        count = _count_print_calls(_parse_py(path, rel, ast_cache, source), source)
        if count:
            prints[rel] = count
    return {
        'oversize_files': oversize,
        'history_names': history,
        'print_calls': prints,
        'stale_test_references': {
            rel: len(refs) for rel, refs in (stale_test_references(root) if stale is None else stale).items()
        },
        'dash_cycle_edges': dash_cycle_edges(root, ast_cache) if (root / 'meerschaum/api/dash').is_dir() else {},
    }


_PRINT_TEXT = re.compile(r'(?<![\w.])print\s*\(')


def _count_print_calls(tree: ast.AST, source: str) -> int:
    """`print(...)` calls, walking only the nodes that span a line where the text appears."""
    lines = sorted({source.count('\n', 0, m.start()) + 1 for m in _PRINT_TEXT.finditer(source)})
    count, stack = 0, [tree]
    while stack:
        node = stack.pop()
        if isinstance(node, ast.Call) and isinstance(node.func, ast.Name) and node.func.id == 'print':
            count += 1
        for child in ast.iter_child_nodes(node):
            start = getattr(child, 'lineno', None)
            if start is None or any(start <= line <= (child.end_lineno or start) for line in lines):
                stack.append(child)
    return count


def _git_ratchet(root: Path, base: str, name: str) -> dict[str, int] | None:
    try:
        text = subprocess.run(
            ['git', 'show', f'{base}:{DATA_DIR_NAME}/{name}.json'],
            cwd=root, capture_output=True, text=True, timeout=10, check=True,
        ).stdout
    except (OSError, subprocess.SubprocessError):
        return None
    return {k: int(v) for k, v in json.loads(text).items() if not k.startswith('_')}


def check_ratchets(
    root: Path = ROOT,
    *,
    data_dir: Path | None = None,
    base: str | None = None,
    ast_cache: dict[Path, ast.AST] | None = None,
    stale: dict[str, list[str]] | None = None,
) -> list[str]:
    """Rule 9: each list is exactly today's tree; nothing joins it, and a number never rises."""
    data_dir = data_dir or root / DATA_DIR_NAME
    measured = measure_ratchets(root, ast_cache, stale)
    data_rel = _rel(root, data_dir) if data_dir.is_relative_to(root) else str(data_dir)
    violations: list[str] = []
    for name, about in RATCHETS.items():
        ceilings = _load_ratchet(data_dir, name)
        current = measured[name]
        where = f'{data_rel}/{name}.json'
        for key in sorted(set(current) | set(ceilings)):
            now, ceiling = current.get(key), ceilings.get(key)
            if ceiling is None:
                violations.append(
                    f'[ratchets] {key}: {now} is new to {about}; fix it instead (split the file, rename it, '
                    f'use warn/info, update the reference): {where} only shrinks'
                )
            elif now is None:
                violations.append(
                    f'[ratchets] {key}: no longer among {about}; drop its entry from {where} '
                    '(or run `scripts/check_architecture.py --update-ratchets`)'
                )
            elif now > ceiling:
                violations.append(f'[ratchets] {key}: {now}, above its ceiling {ceiling} in {where}; bring it back down')
            elif now < ceiling:
                violations.append(
                    f'[ratchets] {key}: {now}, below its ceiling {ceiling}: lower it to {now} in {where} '
                    '(or run `scripts/check_architecture.py --update-ratchets`)'
                )
        if base:
            before = _git_ratchet(root, base, name)
            for key in sorted(set(ceilings) - set(before or {})) if before is not None else ():
                violations.append(f'[ratchets] {where}: `{key}` was added since {base}; these lists only shrink')
            for key in sorted(set(ceilings) & set(before or {})):
                if ceilings[key] > before[key]:
                    violations.append(
                        f'[ratchets] {where}: `{key}` was raised from {before[key]} to {ceilings[key]} since {base}'
                    )
    return violations


def update_ratchets(root: Path = ROOT, data_dir: Path | None = None) -> list[str]:
    """Lower every ratchet to the tree and drop entries that are gone; never add or raise one."""
    data_dir = data_dir or root / DATA_DIR_NAME
    measured = measure_ratchets(root)
    changes: list[str] = []
    for name in RATCHETS:
        ceilings = _load_ratchet(data_dir, name)
        lowered = {k: min(v, measured[name][k]) for k, v in ceilings.items() if k in measured[name]}
        if lowered != ceilings:
            _write_ratchet(data_dir, name, lowered)
            changes.append(f'{name}: {len(ceilings) - len(lowered)} dropped, '
                           f'{sum(1 for k in lowered if lowered[k] < ceilings[k])} lowered')
    return changes


# ---------------------------------------------------------------------------
# Rule 10: Cargo invariants
# ---------------------------------------------------------------------------

REQUIRED_CLIPPY_DENY_LINTS: tuple[str, ...] = ('dbg_macro', 'todo', 'unimplemented')
REQUIRED_RUST_LINTS: dict[str, str] = {'unsafe_code': 'deny'}
#: Crates whose own lib.rs allows `unsafe` against the workspace's `unsafe_code = "deny"`.
UNSAFE_CRATES: dict[str, str] = {
    'meerschaum-plugin': 'the C ABI of native plugins',
    'meerschaum-py': 'the PyO3 extension and its Arrow C stream boundary',
    'meerschaum-jobs': 'daemon processes (fork, setsid, file descriptors)',
}


def start_ring_resolution(root: Path) -> subprocess.Popen | str:
    """Start `cargo tree -i ring` (resolution only, no build), or say why it cannot run."""
    cargo = shutil.which('cargo')
    if cargo is None or not (root / 'Cargo.lock').is_file():
        return 'cargo not found' if cargo is None else 'no Cargo.lock'
    try:
        return subprocess.Popen(
            [cargo, 'tree', '--locked', '--offline', '--workspace', '--target', 'all', '--all-features',
             '-e', 'normal,build,dev', '--prefix', 'none', '-i', 'ring'],
            cwd=root, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True,
        )
    except OSError as error:
        return str(error)


def ring_activation(root: Path, started: subprocess.Popen | str | None = None) -> tuple[bool, str]:
    """Whether `cargo tree` resolves `ring` into any target, feature set or dependency kind.

    `Cargo.lock` still lists `ring`: it records the optional dependencies of rustls, rustls-webpki,
    quinn-proto and object_store whether or not a feature turns them on. Only the resolved graph says
    whether it is built. Returns `(checked, output)`.
    """
    started = start_ring_resolution(root) if started is None else started
    if isinstance(started, str):
        return False, started
    try:
        stdout, stderr = started.communicate(timeout=60)
    except subprocess.TimeoutExpired:
        started.kill()
        return False, 'cargo tree timed out'
    if started.returncode != 0:
        if 'did not match any packages' in stderr:
            return True, ''
        return False, stderr.strip().splitlines()[-1] if stderr.strip() else 'cargo tree failed'
    return True, stdout.strip()


def check_cargo_invariants(
    root: Path = ROOT,
    *,
    require_cargo: bool = False,
    resolve: bool = True,
    ring_resolution: subprocess.Popen | str | None = None,
) -> list[str]:
    """Rule 10: profiles, slim dependency features, workspace lints, and no `ring` in the build."""
    violations: list[str] = []
    cargo_toml_path = root / 'Cargo.toml'
    if not cargo_toml_path.is_file():
        return ['[cargo-invariants] missing root Cargo.toml']
    manifest = tomllib.loads(cargo_toml_path.read_text(encoding='utf-8'))

    profiles = manifest.get('profile', {})
    release = profiles.get('release', {})
    if release.get('strip') != 'symbols':
        violations.append(
            f'[cargo-invariants] Cargo.toml: [profile.release] must set `strip = "symbols"` (found {release.get("strip")!r})'
        )
    if release.get('panic') == 'abort':
        violations.append(
            '[cargo-invariants] Cargo.toml: [profile.release] must NOT set `panic = "abort"` '
            '(PyO3/cdylib unwinding requires `panic = "unwind"`)'
        )
    if profiles.get('ci', {}).get('strip') != 'none':
        violations.append(
            f'[cargo-invariants] Cargo.toml: [profile.ci] must set `strip = "none"` '
            f'(found {profiles.get("ci", {}).get("strip")!r})'
        )

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
    if isinstance(sqlx_dep, dict) and {'tls-rustls', 'tls-rustls-ring'} & set(sqlx_dep.get('features', ())):
        violations.append(
            '[cargo-invariants] Cargo.toml: [workspace.dependencies].sqlx must use '
            '`tls-rustls-aws-lc-rs`, not `tls-rustls` / `tls-rustls-ring` (pulls in `ring`)'
        )

    lints = workspace.get('lints', {})
    for lint_name in REQUIRED_CLIPPY_DENY_LINTS:
        if lints.get('clippy', {}).get(lint_name) != 'deny':
            violations.append(
                f'[cargo-invariants] Cargo.toml: [workspace.lints.clippy].{lint_name} must be "deny" '
                f'(found {lints.get("clippy", {}).get(lint_name)!r})'
            )
    for lint_name, level in REQUIRED_RUST_LINTS.items():
        if lints.get('rust', {}).get(lint_name) != level:
            violations.append(
                f'[cargo-invariants] Cargo.toml: [workspace.lints.rust].{lint_name} must be "{level}" '
                f'(found {lints.get("rust", {}).get(lint_name)!r})'
            )

    def no_ring(label: str, table: dict) -> None:
        for section in ('dependencies', 'dev-dependencies', 'build-dependencies'):
            deps = table.get(section, {})
            if not isinstance(deps, dict):
                continue
            if 'ring' in deps:
                violations.append(f'[cargo-invariants] {label}: forbidden direct `ring` dependency in [{section}]')
            for dep_name, spec in deps.items():
                if dep_name == 'rcgen' and (not isinstance(spec, dict) or spec.get('default-features') is not False):
                    violations.append(
                        f'[cargo-invariants] {label}: `rcgen` in [{section}] must set '
                        '`default-features = false` with `aws_lc_rs` (default enables `ring`)'
                    )
                if isinstance(spec, dict):
                    bad = sorted({'ring', 'tls-rustls', 'tls-rustls-ring'} & set(spec.get('features', ())))
                    if bad:
                        violations.append(
                            f'[cargo-invariants] {label}: `{dep_name}` in [{section}] enables forbidden ring '
                            f'feature(s) {bad}; consolidate on `aws-lc-rs`'
                        )

    no_ring('Cargo.toml', workspace)
    members = workspace.get('members', [])
    if not members:
        violations.append('[cargo-invariants] Cargo.toml: [workspace].members is empty')
    for member in members:
        member_toml = root / member / 'Cargo.toml'
        if not member_toml.is_file():
            violations.append(f'[cargo-invariants] missing workspace member manifest: {member}/Cargo.toml')
            continue
        member_manifest = tomllib.loads(member_toml.read_text(encoding='utf-8'))
        if member_manifest.get('lints', {}).get('workspace') is not True:
            violations.append(
                f'[cargo-invariants] {member}/Cargo.toml: must inherit workspace lints with `[lints]\\nworkspace = true`'
            )
        no_ring(f'{member}/Cargo.toml', member_manifest)
        name = member_manifest.get('package', {}).get('name', Path(member).name)
        lib_rs = root / member / 'src' / 'lib.rs'
        if lib_rs.is_file():
            allows = bool(re.search(r'^#!\[allow\(unsafe_code\)\]', lib_rs.read_text(encoding='utf-8'), re.M))
            if allows and name not in UNSAFE_CRATES:
                violations.append(
                    f'[cargo-invariants] {member}/src/lib.rs: `#![allow(unsafe_code)]` outside UNSAFE_CRATES; '
                    'allow it on the one module that needs it, with `// SAFETY:` comments'
                )

    if resolve:
        checked, output = ring_activation(root, ring_resolution)
        if checked and output:
            violations.append(
                '[cargo-invariants] `ring` is built into the workspace (cargo tree -i ring):\n    '
                + output.replace('\n', '\n    ')
            )
        elif not checked and require_cargo:
            violations.append(f'[cargo-invariants] could not resolve the dependency graph for `ring`: {output}')
        elif not checked:
            print(f'note: `ring` activation not checked ({output}); run with cargo for the full check', file=sys.stderr)
    return violations


# ---------------------------------------------------------------------------
# Rule 11: the file tree is the module tree
# ---------------------------------------------------------------------------

#: Every `#[path]` attribute in `crates/*/src`, keyed `<file>::<module>`, and why the module cannot
#: sit where `mod <module>;` resolves without one.
MODULE_PATH_ATTRIBUTES: dict[str, str] = {
    'crates/meerschaum-core/src/team_scope/sandbox.rs::imp':
        'platform selection: Linux compiles the Landlock implementation, other systems an inline stub',
    'crates/meerschaum-web/src/lib.rs::test_memory':
        "test-only: shares the server's in-memory test instance (crates/meerschaum-server/tests/common/memory.rs)",
}
_PATH_ATTR_START = re.compile(r'#\[\s*path\s*=')
_NEXT_MOD = re.compile(r'\bmod\s+(\w+)')


def check_module_paths(root: Path = ROOT) -> list[str]:
    """Rule 11: no `#[path]` in `crates/*/src` outside MODULE_PATH_ATTRIBUTES."""
    violations: list[str] = []
    found: set[str] = set()
    for crate in _crate_dirs(root):
        for path in sorted((crate / 'src').rglob('*.rs')):
            text = path.read_text(encoding='utf-8')
            if '#[' not in text or 'path' not in text:
                continue
            code = _strip_rust_comments(text)
            rel = _rel(root, path)
            for match in _PATH_ATTR_START.finditer(code):
                module = _NEXT_MOD.search(code, match.end())
                name = module.group(1) if module else '?'
                key = f'{rel}::{name}'
                found.add(key)
                if key in MODULE_PATH_ATTRIBUTES:
                    continue
                line = code.count('\n', 0, match.start()) + 1
                violations.append(
                    f'[module-paths] {rel}:{line}: `#[path]` on `mod {name}`: move the file to where '
                    f'`mod {name};` resolves without it, or list `{key}` with its reason in MODULE_PATH_ATTRIBUTES'
                )
    if (root / 'crates').is_dir():
        for key in sorted(set(MODULE_PATH_ATTRIBUTES) - found):
            violations.append(f'[module-paths] {key}: listed in MODULE_PATH_ATTRIBUTES but gone; drop the entry')
    return violations


# ---------------------------------------------------------------------------
# Runner
# ---------------------------------------------------------------------------

CHECKS: tuple[tuple[str, Callable[..., list[str]]], ...] = (
    ('local-server-http', check_local_server_http),
    ('single-home', check_single_home),
    ('dash-import-cycles', check_dash_import_cycles),
    ('sql-canonical-schema', check_sql_canonical_schema),
    ('test-layout', check_test_layout),
    ('test-references', check_test_references),
    ('crate-layering', check_crate_layering),
    ('heavy-imports', check_heavy_imports),
    ('ratchets', check_ratchets),
    ('cargo-invariants', check_cargo_invariants),
    ('module-paths', check_module_paths),
)
_USES_AST = {'local-server-http', 'dash-import-cycles', 'heavy-imports', 'ratchets'}


def default_ratchet_base(root: Path) -> str | None:
    """`MRSM_ARCH_RATCHET_BASE`, else the merge base with `origin/main` when the clone has it."""
    if os.environ.get('MRSM_ARCH_RATCHET_BASE'):
        return os.environ['MRSM_ARCH_RATCHET_BASE']
    try:
        return subprocess.run(
            ['git', 'merge-base', 'HEAD', 'origin/main'],
            cwd=root, capture_output=True, text=True, timeout=10, check=True,
        ).stdout.strip() or None
    except (OSError, subprocess.SubprocessError):
        return None


def run_all_checks(
    root: Path = ROOT,
    *,
    ratchet_base: str | None = None,
    require_cargo: bool = False,
    resolve_cargo: bool = True,
) -> list[str]:
    ast_cache: dict[Path, ast.AST] = {}
    violations: list[str] = []
    # The one external command runs while the other rules read the tree.
    ring_resolution = start_ring_resolution(root) if resolve_cargo else None
    stale = stale_test_references(root)  # rules 6 and 9 both read it
    for name, check in CHECKS:
        kwargs: dict = {'ast_cache': ast_cache} if name in _USES_AST else {}
        if name in ('test-references', 'ratchets'):
            kwargs['stale'] = stale
        if name == 'ratchets':
            kwargs['base'] = ratchet_base
        if name == 'cargo-invariants':
            kwargs.update(require_cargo=require_cargo, resolve=resolve_cargo, ring_resolution=ring_resolution)
        violations.extend(check(root, **kwargs))
    return violations


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__.split('\n\n')[0])
    parser.add_argument('--ratchet-base', help='also refuse ratchet entries added or raised since this git revision')
    parser.add_argument('--update-ratchets', action='store_true', help='lower ratchet numbers to the tree')
    parser.add_argument('--require-cargo', action='store_true', help='fail when `cargo tree` cannot run')
    args = parser.parse_args(argv)
    if args.update_ratchets:
        for line in update_ratchets(ROOT) or ['ratchets already match the tree']:
            print(line)
        return 0
    base = args.ratchet_base or default_ratchet_base(ROOT)
    violations = run_all_checks(ROOT, ratchet_base=base, require_cargo=args.require_cargo)
    if violations:
        for line in violations:
            print(line, file=sys.stderr)
        print(f'architecture check failed: {len(violations)} violation(s) across {len(CHECKS)} checks', file=sys.stderr)
        return 1
    print(f'architecture ok: {len(CHECKS)} checks passed')
    return 0


if __name__ == '__main__':
    raise SystemExit(main())
