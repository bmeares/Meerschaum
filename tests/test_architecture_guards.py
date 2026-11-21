"""
Tests for the automated architectural guardrails (`scripts/check_architecture.py`).

Verifies both:
1. The live repository passes all 8 architectural checks cleanly (and the CLI exits 0).
2. Every guardrail catches synthetic regressions in isolated temp directories so
   the checks cannot silently become no-ops.
"""

from __future__ import annotations

import importlib.util
import subprocess
import sys
from pathlib import Path

import pytest

REPO = Path(__file__).resolve().parents[1]
GUARD_SCRIPT = REPO / 'scripts' / 'check_architecture.py'


def _load_guard_module():
    spec = importlib.util.spec_from_file_location('check_architecture', GUARD_SCRIPT)
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


guards = _load_guard_module()


def _write(base: Path, rel: str, content: str) -> Path:
    path = base / rel
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(content, encoding='utf-8')
    return path


# ---------------------------------------------------------------------------
# Live repository verification
# ---------------------------------------------------------------------------


def test_live_repo_passes_all_architecture_guardrails():
    violations = guards.run_all_checks(REPO)
    assert violations == [], '\n'.join(violations)


def test_check_architecture_cli_exits_zero():
    result = subprocess.run(
        [sys.executable, '-I', str(GUARD_SCRIPT)],
        cwd=REPO,
        capture_output=True,
        text=True,
        timeout=30,
    )
    assert result.returncode == 0, result.stdout + result.stderr
    assert 'architecture ok: 8 checks passed' in result.stdout


def test_ci_fmt_step_runs_check_architecture():
    ci_sh = (REPO / 'scripts' / 'rust' / 'ci.sh').read_text(encoding='utf-8')
    body = ci_sh[ci_sh.index('step_fmt()'):ci_sh.index('step_clippy()')]
    assert 'scripts/check_architecture.py' in body


# ---------------------------------------------------------------------------
# Synthetic violation tests: Rule 1 (no raw urllib.request.urlopen)
# ---------------------------------------------------------------------------


def test_rule1_catches_raw_urlopen_and_respects_allowlist(tmp_path):
    _write(
        tmp_path,
        'meerschaum/api/host/_server_client.py',
        'import urllib.request\ndef ok(req):\n    return urllib.request.urlopen(req)\n',
    )
    _write(
        tmp_path,
        'meerschaum/api/dash/connector_listing.py',
        'import urllib.request\ndef probe(url):\n    return urllib.request.urlopen(url)\n',
    )
    assert guards.check_no_raw_urlopen(tmp_path) == []

    _write(
        tmp_path,
        'meerschaum/api/routes/bad_route.py',
        'from urllib.request import urlopen\n'
        'import urllib.request\n'
        'def call(url):\n'
        '    urlopen(url)\n'
        '    return urllib.request.urlopen(url)\n',
    )
    violations = guards.check_no_raw_urlopen(tmp_path)
    assert len(violations) == 3
    assert any('bad_route.py:1:' in v for v in violations)
    assert any('bad_route.py:4:' in v for v in violations)
    assert any('bad_route.py:5:' in v for v in violations)


# ---------------------------------------------------------------------------
# Synthetic violation tests: Rule 2 (no retired shallow viewer helpers)
# ---------------------------------------------------------------------------


def test_rule2_catches_retired_viewer_module_and_helpers(tmp_path):
    _write(tmp_path, 'meerschaum/api/dash/ok.py', 'def get_viewer():\n    return None\n')
    assert guards.check_no_retired_viewers(tmp_path) == []

    _write(tmp_path, 'meerschaum/api/dash/pipes_access.py', 'def helper():\n    pass\n')
    _write(
        tmp_path,
        'meerschaum/api/dash/regress.py',
        'from meerschaum.api.dash import pipes_access\n'
        'from meerschaum.api.dash.users import source_viewer, fresh_viewer\n'
        'def f(users):\n'
        '    users.viewer()\n'
        '    source_viewer()\n'
        '    fresh_viewer()\n',
    )
    violations = guards.check_no_retired_viewers(tmp_path)
    joined = '\n'.join(violations)
    assert 'pipes_access.py must not exist' in joined
    assert 'pipes_access' in joined
    assert 'users.viewer' in joined
    assert 'source_viewer' in joined
    assert 'fresh_viewer' in joined


# ---------------------------------------------------------------------------
# Synthetic violation tests: Rule 3 (no duplicate SQL lexer or error patches)
# ---------------------------------------------------------------------------


def test_rule3_catches_duplicate_sql_lexer_and_error_patches(tmp_path):
    _write(
        tmp_path,
        'crates/meerschaum-server/src/mcp/security.rs',
        '// Comments mentioning strip_sql_noise or is_read_only_query are fine.\n'
        'pub fn validate_mcp_sql(sql: &str) -> bool { !sql.is_empty() }\n',
    )
    _write(tmp_path, 'crates/meerschaum-cli/src/lib.rs', 'pub fn ok() {}\n')
    assert guards.check_no_duplicate_sql_lexer(tmp_path) == []

    _write(
        tmp_path,
        'crates/meerschaum-server/src/mcp/security.rs',
        'fn strip_sql_noise(s: &str) -> String { s.to_string() }\n'
        'pub(crate) fn is_read_only_query(s: &str) -> bool { !s.is_empty() }\n',
    )
    _write(
        tmp_path,
        'crates/meerschaum-cli/src/err.rs',
        'fn patch(msg: String) -> String {\n'
        '    msg.replace("for an API request", "for CLI").replace("a SQL definition", "a pipe")\n'
        '}\n',
    )
    violations = guards.check_no_duplicate_sql_lexer(tmp_path)
    joined = '\n'.join(violations)
    assert 'security.rs:1:' in joined and 'strip_sql_noise' in joined
    assert 'security.rs:2:' in joined and 'is_read_only_query' in joined
    assert 'err.rs:2:' in joined
    assert '.replace("for an API request"' in joined
    assert '.replace("a SQL definition"' in joined


# ---------------------------------------------------------------------------
# Synthetic violation tests: Rule 4 (dash import cycles)
# ---------------------------------------------------------------------------


def test_rule4_catches_sessions_to_access_and_plugin_and_new_cycles(tmp_path):
    # Clean acyclic dash layout
    _write(tmp_path, 'meerschaum/api/dash/__init__.py', '')
    _write(tmp_path, 'meerschaum/api/dash/sessions.py', 'def current():\n    return None\n')
    _write(tmp_path, 'meerschaum/api/dash/access.py', 'from . import sessions\n')
    _write(tmp_path, 'meerschaum/api/dash/_plugins/__init__.py', 'from . import _fs\n')
    _write(tmp_path, 'meerschaum/api/dash/_plugins/_fs.py', 'X = 1\n')
    _write(tmp_path, 'meerschaum/api/dash/plugins.py', 'from ._plugins import _fs\n')
    _write(tmp_path, 'meerschaum/api/dash/plugin_sources.py', 'from ._plugins import _fs\n')
    assert guards.check_dash_import_cycles(tmp_path) == []

    # 1. Transitive function-local import from sessions -> helper -> access
    _write(
        tmp_path,
        'meerschaum/api/dash/session_helper.py',
        'def load():\n    from meerschaum.api.dash import access\n    return access\n',
    )
    _write(
        tmp_path,
        'meerschaum/api/dash/sessions.py',
        'def current():\n    from . import session_helper\n    return session_helper.load()\n',
    )
    # 2. Cycle inside plugin cluster via function-local import
    _write(
        tmp_path,
        'meerschaum/api/dash/plugins.py',
        'from . import plugin_sources\n',
    )
    _write(
        tmp_path,
        'meerschaum/api/dash/plugin_sources.py',
        'def back():\n    from . import plugins\n    return plugins\n',
    )
    # 3. Brand-new unrelated cycle in dash
    _write(tmp_path, 'meerschaum/api/dash/alpha.py', 'from . import beta\n')
    _write(tmp_path, 'meerschaum/api/dash/beta.py', 'from . import alpha\n')

    violations = guards.check_dash_import_cycles(tmp_path)
    joined = '\n'.join(violations)
    assert 'sessions.py must not import meerschaum.api.dash.access' in joined
    assert 'plugin cluster must have 0 cycles' in joined
    assert 'alpha -> meerschaum.api.dash.beta' in joined or 'beta -> meerschaum.api.dash.alpha' in joined


# ---------------------------------------------------------------------------
# Synthetic violation tests: Rule 5 (no sync workarounds)
# ---------------------------------------------------------------------------


def test_rule5_catches_inplace_fetch_opt_and_duplicate_forget_state(tmp_path):
    _write(tmp_path, 'crates/meerschaum-connector/src/pipe_ops.rs', 'pub fn forget_engine_state_after() {}\n')
    _write(tmp_path, 'crates/meerschaum-engine/src/lib.rs', 'pub fn sync() {}\n')
    _write(tmp_path, 'crates/meerschaum-cli/src/lib.rs', 'pub fn cli() {}\n')
    _write(tmp_path, 'crates/meerschaum-py/src/lib.rs', 'pub fn py() {}\n')
    assert guards.check_no_sync_workarounds(tmp_path) == []

    _write(tmp_path, 'crates/meerschaum-cli/src/bad.rs', 'struct Inplace;\n')
    _write(tmp_path, 'crates/meerschaum-engine/src/bad.rs', 'fn check() -> bool { is_fetch_optimization }\n')
    _write(
        tmp_path,
        'crates/meerschaum-connector-sql/src/dup.rs',
        'fn forget_engine_state_after() {}\n',
    )
    violations = guards.check_no_sync_workarounds(tmp_path)
    joined = '\n'.join(violations)
    assert 'struct Inplace' in joined
    assert 'is_fetch_optimization' in joined
    assert 'crates/meerschaum-connector-sql/src/dup.rs:1:' in joined


# ---------------------------------------------------------------------------
# Synthetic violation tests: Rule 6 (SQL canonical schema)
# ---------------------------------------------------------------------------


def test_rule6_catches_missing_helpers_require_schema_bloat_and_legacy_state(tmp_path):
    _write(
        tmp_path,
        'crates/meerschaum-connector-sql/src/instance/mod.rs',
        'pub(crate) async fn ensure_schema(&self) {}\n'
        'pub(crate) async fn bind_pipe(&self) {}\n',
    )
    _write(
        tmp_path,
        'crates/meerschaum-connector-sql/src/instance/trait_impl.rs',
        'async fn f() { self.require_schema().await; }\n',
    )
    assert guards.check_sql_canonical_schema(tmp_path) == []

    # Break all three invariants
    _write(tmp_path, 'crates/meerschaum-connector-sql/src/instance/mod.rs', 'pub fn empty() {}\n')
    _write(
        tmp_path,
        'crates/meerschaum-connector-sql/src/instance/trait_impl.rs',
        ''.join('let _ = self.require_schema().await;\n' for _ in range(6)),
    )
    _write(
        tmp_path,
        'crates/meerschaum-connector-sql/src/state.rs',
        'struct State { synced_columns: Vec<String>, internal_pipe_states: usize }\n',
    )
    violations = guards.check_sql_canonical_schema(tmp_path)
    joined = '\n'.join(violations)
    assert 'missing `fn ensure_schema`' in joined
    assert 'missing `fn bind_pipe`' in joined
    assert 'found 6 `self.require_schema().await` calls' in joined
    assert 'synced_columns' in joined


# ---------------------------------------------------------------------------
# Synthetic violation tests: Rule 7 (integration test binary ceilings)
# ---------------------------------------------------------------------------


def test_rule7_catches_per_crate_and_workspace_ceiling_violations(tmp_path):
    _write(tmp_path, 'crates/meerschaum-compose/tests/integration/main.rs', 'fn main() {}\n')
    _write(tmp_path, 'crates/meerschaum-web/tests/integration/main.rs', 'fn main() {}\n')
    assert guards.check_integration_test_ceilings(tmp_path) == []

    # Exceed meerschaum-compose ceiling (1 -> 2)
    _write(tmp_path, 'crates/meerschaum-compose/tests/extra.rs', '#[test] fn t() {}\n')
    violations = guards.check_integration_test_ceilings(tmp_path)
    assert len(violations) == 1
    assert 'crates/meerschaum-compose/tests: 2 integration test binaries exceeds ceiling 1' in violations[0]


# ---------------------------------------------------------------------------
# Synthetic violation tests: Rule 8 (Cargo.toml / Cargo.lock invariants)
# ---------------------------------------------------------------------------


def _write_valid_cargo_tree(tmp_path: Path) -> None:
    _write(
        tmp_path,
        'Cargo.toml',
        '[workspace]\n'
        'members = ["crates/foo"]\n'
        '\n'
        '[workspace.lints.clippy]\n'
        'dbg_macro = "deny"\n'
        'todo = "deny"\n'
        'unimplemented = "deny"\n'
        '\n'
        '[workspace.dependencies]\n'
        'arrow = { version = "60", default-features = false, features = ["ipc"] }\n'
        'polars = { version = "0.55", default-features = false, features = ["ipc"] }\n'
        '\n'
        '[profile.release]\n'
        'strip = "symbols"\n'
        '\n'
        '[profile.ci]\n'
        'strip = "none"\n',
    )
    _write(
        tmp_path,
        'crates/foo/Cargo.toml',
        '[package]\nname = "foo"\nversion = "0.1.0"\n\n[lints]\nworkspace = true\n',
    )
    _write(
        tmp_path,
        'Cargo.lock',
        'version = 4\n\n[[package]]\nname = "aws-lc-rs"\nversion = "1.0.0"\n',
    )


def test_rule8_catches_cargo_slimming_lint_and_ring_regressions(tmp_path):
    _write_valid_cargo_tree(tmp_path)
    assert guards.check_cargo_invariants(tmp_path) == []

    # Break every invariant in Cargo.toml, member Cargo.toml, and Cargo.lock
    _write(
        tmp_path,
        'Cargo.toml',
        '[workspace]\n'
        'members = ["crates/foo"]\n'
        '\n'
        '[workspace.lints.clippy]\n'
        'dbg_macro = "warn"\n'
        '\n'
        '[workspace.dependencies]\n'
        'arrow = "60"\n'
        'polars = { version = "0.55", features = ["ipc"] }\n'
        'sqlx = { version = "0.9", features = ["tls-rustls"] }\n'
        '\n'
        '[profile.release]\n'
        'strip = "none"\n'
        'panic = "abort"\n'
        '\n'
        '[profile.ci]\n'
        'strip = "symbols"\n',
    )
    _write(
        tmp_path,
        'crates/foo/Cargo.toml',
        '[package]\nname = "foo"\nversion = "0.1.0"\n\n[dev-dependencies]\nrcgen = "0.14"\n',
    )

    violations = guards.check_cargo_invariants(tmp_path)
    joined = '\n'.join(violations)
    assert 'strip = "symbols"' in joined
    assert 'panic = "abort"' in joined
    assert 'strip = "none"' in joined
    assert 'polars' in joined
    assert 'arrow' in joined
    assert 'sqlx' in joined
    assert 'rcgen' in joined
    assert 'dbg_macro' in joined
    assert 'todo' in joined
    assert 'unimplemented' in joined
    assert 'crates/foo/Cargo.toml' in joined
