"""
Tests for the structural guardrails (`scripts/check_architecture.py`).

1. The live repository passes every rule, and the command exits 0.
2. Every rule fires on a small violating tree, so none can silently become a no-op.
"""

from __future__ import annotations

import importlib.util
import json
import os
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


def _crate(base: Path, name: str, deps: tuple[str, ...] = (), extra: str = '') -> None:
    lines = '\n'.join(f'{dep} = {{ path = "../{dep}" }}' for dep in deps)
    _write(base, f'crates/{name}/Cargo.toml',
           f'[package]\nname = "{name}"\nversion = "0.1.0"\n\n[dependencies]\n{lines}\n{extra}')


# ---------------------------------------------------------------------------
# The live repository
# ---------------------------------------------------------------------------


def test_live_repo_passes_all_architecture_guardrails():
    violations = guards.run_all_checks(REPO)
    assert violations == [], '\n'.join(violations)


def test_check_architecture_cli_exits_zero_quickly():
    result = subprocess.run(
        [sys.executable, '-I', str(GUARD_SCRIPT)],
        cwd=REPO, capture_output=True, text=True, timeout=60,
    )
    assert result.returncode == 0, result.stdout + result.stderr
    assert f'architecture ok: {len(guards.CHECKS)} checks passed' in result.stdout


def test_ci_guardrails_step_runs_check_architecture():
    ci_sh = (REPO / 'scripts' / 'rust' / 'ci.sh').read_text(encoding='utf-8')
    body = ci_sh[ci_sh.index('step_guardrails()'):]
    body = body[:body.index('\n}\n')]
    assert 'scripts/check_architecture.py' in body


# ---------------------------------------------------------------------------
# Rule 1: local-server HTTP
# ---------------------------------------------------------------------------


def test_local_server_http_catches_every_raw_client(tmp_path):
    _write(tmp_path, 'meerschaum/api/host/_server_client.py',
           'import urllib.request\nopener = urllib.request.build_opener()\n')
    _write(tmp_path, 'meerschaum/api/dash/connector_listing.py',
           'import urllib.request\ndef probe(url):\n    return urllib.request.urlopen(url)\n')
    _write(tmp_path, 'meerschaum/api/ok.py', 'import urllib.parse\nx = urllib.parse.quote("a")\n')
    assert guards.check_local_server_http(tmp_path) == []

    _write(
        tmp_path, 'meerschaum/api/routes/bad.py',
        'from urllib.request import urlopen\n'                          # 1
        'import urllib.request\n'
        'import http.client\n'                                          # 3
        'import requests\n'                                             # 4
        'from meerschaum.utils.packages import attempt_import\n'
        'def call(url):\n'
        '    urlopen(url)\n'                                            # 7
        '    urllib.request.build_opener().open(url)\n'                 # 8
        '    http.client.HTTPConnection("localhost")\n'                 # 9
        '    return attempt_import("httpx")\n',                         # 10
    )
    lines = {v.split(':')[1] for v in guards.check_local_server_http(tmp_path)}
    assert lines == {'1', '3', '4', '7', '8', '9', '10'}


# ---------------------------------------------------------------------------
# Rule 2: single-home definitions
# ---------------------------------------------------------------------------


def test_single_home_catches_a_second_definition(tmp_path):
    _write(tmp_path, 'meerschaum/api/dash/viewer.py',
           'class Viewer:\n    pass\ndef current_viewer():\n    return Viewer()\n')
    _write(tmp_path, 'crates/meerschaum-server/src/sql_guard/lexer.rs',
           'fn dollar_tag_len() {}\nfn check_read_only_tokens() {}\n')
    _write(tmp_path, 'crates/meerschaum-connector/src/pipe_ops.rs', 'pub fn forget_engine_state_after() {}\n')
    _write(tmp_path, 'crates/meerschaum-server/src/mcp/security.rs',
           '// fn is_read_only_query used to live here\npub fn validate() {}\n')
    assert guards.check_single_home(tmp_path) == []

    _write(tmp_path, 'meerschaum/api/dash/pages/x.py', 'def current_viewer():\n    return None\n')
    _write(tmp_path, 'crates/meerschaum-server/src/mcp/security.rs', 'pub(crate) fn is_read_only_query() {}\n')
    _write(tmp_path, 'crates/meerschaum-engine/src/state.rs', 'fn forget_engine_state_after() {}\n')
    joined = '\n'.join(guards.check_single_home(tmp_path))
    assert 'meerschaum/api/dash/pages/x.py:1: defines `current_viewer`' in joined
    assert 'crates/meerschaum-server/src/mcp/security.rs:1: defines `is_read_only_query`' in joined
    assert 'crates/meerschaum-engine/src/state.rs:1: defines `forget_engine_state_after`' in joined


def test_single_home_catches_a_second_copy_of_a_shared_rust_helper(tmp_path):
    homes = {
        'crates/meerschaum-core/src/pyfmt.rs': 'pub fn thousands() {}\npub fn quote_plus() {}\n',
        'crates/meerschaum-core/src/static_config.rs': 'pub fn separate_negation_values() {}\n',
        'crates/meerschaum-core/src/pipe/mod.rs': 'impl Pipe {\n    pub fn fetch_replace(&self) {}\n}\n',
        'crates/meerschaum-core/src/config/grafana_readonly.rs': 'pub fn trimmed_text() {}\n',
        'crates/meerschaum-kernel/src/diff.rs': 'pub fn backtrack_params() {}\n',
        'crates/meerschaum-kernel/src/serialize.rs': 'pub fn sort_json_keys() {}\n',
        'crates/meerschaum-connector/src/date_bound.rs': 'impl DateBound {\n    pub fn precedes(&self) {}\n}\n',
        'crates/meerschaum-connector/src/pipe_ops.rs': 'pub fn forget_engine_state_after() {}\n',
        'crates/meerschaum-connector-sql/src/uri.rs': 'impl X {\n    pub fn from_attribute_map() {}\n}\n',
    }
    for path, text in homes.items():
        _write(tmp_path, path, text)
    # A helper named like a retired copy, but of another name, is not a copy.
    _write(tmp_path, 'crates/meerschaum-cli/src/native/show.rs', 'fn thousands_of_rows() {}\n')
    assert guards.check_single_home(tmp_path) == []

    _write(tmp_path, 'crates/meerschaum-connector-api/src/client/pipes.rs',
           'fn thousands(n: usize) -> String {\n    n.to_string()\n}\n')
    _write(tmp_path, 'crates/meerschaum-engine/src/selection.rs', 'fn negation_prefix() {}\n')
    _write(tmp_path, 'crates/meerschaum-server/src/pipe_methods.rs', 'fn bound_lt() {}\n')
    _write(tmp_path, 'crates/meerschaum-cli/src/native/session.rs', 'fn sql_attributes() {}\n')
    joined = '\n'.join(guards.check_single_home(tmp_path))
    assert 'crates/meerschaum-connector-api/src/client/pipes.rs:1: defines `thousands`' in joined
    assert 'crates/meerschaum-engine/src/selection.rs:1: defines `negation_prefix`' in joined
    assert 'crates/meerschaum-server/src/pipe_methods.rs:1: defines `bound_lt`' in joined
    assert 'crates/meerschaum-cli/src/native/session.rs:1: defines `sql_attributes`' in joined

    _write(tmp_path, 'crates/meerschaum-kernel/src/diff.rs', 'pub fn filter_existing() {}\n')
    assert any('no longer defines a backtrack request' in v for v in guards.check_single_home(tmp_path))


# ---------------------------------------------------------------------------
# Rule 3: dash import cycles
# ---------------------------------------------------------------------------


def test_dash_cycles_catch_sessions_to_access_and_plugin_cycles(tmp_path):
    _write(tmp_path, 'meerschaum/api/dash/__init__.py', '')
    _write(tmp_path, 'meerschaum/api/dash/sessions.py', 'def current():\n    return None\n')
    _write(tmp_path, 'meerschaum/api/dash/access.py', 'from . import sessions\n')
    _write(tmp_path, 'meerschaum/api/dash/plugins.py', 'X = 1\n')
    _write(tmp_path, 'meerschaum/api/dash/plugin_sources.py', 'from . import plugins\n')
    assert guards.check_dash_import_cycles(tmp_path) == []

    _write(tmp_path, 'meerschaum/api/dash/session_helper.py',
           'def load():\n    from meerschaum.api.dash import access\n    return access\n')
    _write(tmp_path, 'meerschaum/api/dash/sessions.py',
           'def current():\n    from . import session_helper\n    return session_helper.load()\n')
    _write(tmp_path, 'meerschaum/api/dash/plugins.py', 'def back():\n    from . import plugin_sources\n')
    joined = '\n'.join(guards.check_dash_import_cycles(tmp_path))
    assert 'sessions.py must not import meerschaum.api.dash.access' in joined
    assert 'plugin cluster must have 0 cycles' in joined


# ---------------------------------------------------------------------------
# Rule 4: SQL canonical schema
# ---------------------------------------------------------------------------


def test_sql_schema_counts_require_schema_across_line_breaks(tmp_path):
    _write(tmp_path, 'crates/meerschaum-connector-sql/src/instance/mod.rs',
           'pub(crate) async fn ensure_schema(&self) {}\npub(crate) async fn bind_pipe(&self) {}\n')
    _write(tmp_path, 'crates/meerschaum-connector-sql/src/instance/trait_impl.rs',
           '// self.require_schema() is gone\nasync fn f() { self.ensure_schema().await; }\n')
    assert guards.check_sql_canonical_schema(tmp_path) == []

    _write(tmp_path, 'crates/meerschaum-connector-sql/src/instance/trait_impl.rs',
           'async fn f(&self) {\n    self\n        .require_schema()\n        .await?;\n'
           '    Self::require_schema(self).await?;\n}\n')
    _write(tmp_path, 'crates/meerschaum-connector-sql/src/state.rs', 'struct State { synced_columns: u8 }\n')
    joined = '\n'.join(guards.check_sql_canonical_schema(tmp_path))
    assert 'found 2 `require_schema(...)` calls (ceiling 0)' in joined
    assert 'state.rs:1: `synced_columns`' in joined


# ---------------------------------------------------------------------------
# Rule 5: test layout
# ---------------------------------------------------------------------------


def _suite_tree(tmp_path: Path) -> None:
    _crate(tmp_path, 'meerschaum-web')
    _write(tmp_path, 'crates/meerschaum-web/tests/web_suite/main.rs',
           '#[path = "../common/mod.rs"]\nmod common;\n\nmod pages;\n#[cfg(unix)]\npub mod stream;\n')
    _write(tmp_path, 'crates/meerschaum-web/tests/web_suite/pages.rs', '#[test]\nfn renders() {}\n')
    _write(tmp_path, 'crates/meerschaum-web/tests/web_suite/stream.rs', 'mod nested;\n')
    _write(tmp_path, 'crates/meerschaum-web/tests/web_suite/stream/nested.rs', '#[test]\nfn n() {}\n')
    _write(tmp_path, 'crates/meerschaum-web/tests/common/mod.rs', 'pub fn helper() {}\n')
    _write(tmp_path, 'crates/meerschaum-web/tests/fixtures/data.rs', '// read as text by a test\n')


def test_test_layout_catches_an_undeclared_module(tmp_path, monkeypatch):
    _suite_tree(tmp_path)
    monkeypatch.setattr(guards, 'SINGLE_FILE_TEST_BINARIES', {})
    monkeypatch.setattr(guards, 'INTEGRATION_TEST_CEILINGS', {'meerschaum-web': 1})
    assert guards.check_test_layout(tmp_path) == []

    _write(tmp_path, 'crates/meerschaum-web/tests/web_suite/forgotten.rs', '#[test]\nfn never_runs() {}\n')
    violations = guards.check_test_layout(tmp_path)
    assert violations == [
        '[test-layout] crates/meerschaum-web/tests/web_suite/forgotten.rs: not compiled into any test binary; '
        "declare it with `mod forgotten;` in its suite's main.rs (or delete it)"
    ]


def test_test_layout_catches_new_binaries_ceilings_and_shared_env(tmp_path, monkeypatch):
    _suite_tree(tmp_path)
    monkeypatch.setattr(guards, 'SINGLE_FILE_TEST_BINARIES', {})
    monkeypatch.setattr(guards, 'INTEGRATION_TEST_CEILINGS', {'meerschaum-web': 1})
    _write(tmp_path, 'crates/meerschaum-web/tests/loose.rs', '#[test]\nfn t() {}\n')
    _write(tmp_path, 'crates/meerschaum-web/tests/web_suite/pages.rs',
           '#[test]\nfn renders() {\n    // std::env::set_var in a comment is fine\n'
           '    unsafe { std::env::set_var("X", "1") };\n}\n')
    joined = '\n'.join(guards.check_test_layout(tmp_path))
    assert 'crates/meerschaum-web/tests/loose.rs: a new single-file test binary' in joined
    assert '2 integration test binaries exceeds ceiling 1' in joined
    assert 'web_suite/pages.rs:4: `set_var` in a suite shared by' in joined

    monkeypatch.setattr(guards, 'INTEGRATION_TEST_CEILINGS', {'meerschaum-web': 3})
    monkeypatch.setattr(guards, 'SINGLE_FILE_TEST_BINARIES', {'crates/meerschaum-web/tests/loose.rs': 'r'})
    assert any("lower INTEGRATION_TEST_CEILINGS['meerschaum-web'] to 2" in v
               for v in guards.check_test_layout(tmp_path))


# ---------------------------------------------------------------------------
# Rule 6: test references
# ---------------------------------------------------------------------------


def test_test_references_catch_moved_targets_and_paths(tmp_path):
    _suite_tree(tmp_path)
    _write(tmp_path, 'crates/meerschaum-web/README.md',
           'Run `cargo test -p meerschaum-web --test web_suite pages::`; see `tests/web_suite/pages.rs`.\n')
    _write(tmp_path, 'scripts/run.sh', 'cargo test -p meerschaum-web --test web_suite\n')
    assert guards.check_test_references(tmp_path) == []

    _write(tmp_path, 'crates/meerschaum-web/README.md',
           'Run `cargo test -p meerschaum-web --test pages`; see `tests/pages.rs`.\n')
    _write(tmp_path, '.github/workflows/ci.yaml', 'run: cargo test -p meerschaum-core --test web_suite\n')
    joined = '\n'.join(guards.check_test_references(tmp_path))
    assert 'crates/meerschaum-web/README.md:1: `--test pages`' in joined
    assert 'crates/meerschaum-web/README.md:1: `tests/pages.rs`' in joined
    assert '.github/workflows/ci.yaml:1: `--test web_suite`' in joined  # a target of another crate

    _write(tmp_path, 'scripts/architecture/stale_test_references.json',
           json.dumps({'crates/meerschaum-web/README.md': 2, '.github/workflows/ci.yaml': 1}))
    assert guards.check_test_references(tmp_path) == []



def test_test_references_catch_moved_source_files(tmp_path):
    _suite_tree(tmp_path)
    _write(tmp_path, 'crates/meerschaum-web/src/pages/mod.rs', '')
    _write(tmp_path, 'docs/agents/map.md',
           'Pages: `crates/meerschaum-web/src/pages/mod.rs` in `crates/meerschaum-web/src/pages/`; '
           'a bare `src/lib.rs` outside a crate is not checked, nor is `crates/<crate>/src/<path>.rs`.\n')
    _write(tmp_path, 'crates/meerschaum-web/README.md', 'See `src/pages/mod.rs`.\n')
    assert guards.check_test_references(tmp_path) == []

    _write(tmp_path, 'docs/agents/map.md',
           'Pages: `crates/meerschaum-web/src/pages.rs`, sessions in `crates/meerschaum-web/src/sessions/`.\n')
    _write(tmp_path, 'crates/meerschaum-web/README.md', 'See `src/pages.rs`.\n')
    joined = '\n'.join(guards.check_test_references(tmp_path))
    assert 'docs/agents/map.md:1: `crates/meerschaum-web/src/pages.rs`' in joined
    assert 'docs/agents/map.md:1: `crates/meerschaum-web/src/sessions/`' in joined
    assert 'crates/meerschaum-web/README.md:1: `src/pages.rs`' in joined

    _write(tmp_path, 'tests/test_pages.py', '# Mirrors `crates/meerschaum-web/tests/pages.rs`.\n')
    _write(tmp_path, 'crates/meerschaum-web/src/lib.rs', '//! See `crates/meerschaum-web/src/pages/mod.rs`.\n')
    joined = '\n'.join(guards.check_test_references(tmp_path))
    assert 'tests/test_pages.py:1: `crates/meerschaum-web/tests/pages.rs`' in joined
    assert 'src/lib.rs' not in joined

    _write(tmp_path, 'docs/agents/map.md', 'Tests: `web/tests/web_suite/pages.rs`, `web/tests/pages.rs`.\n')
    joined = '\n'.join(guards.check_test_references(tmp_path))
    assert 'docs/agents/map.md:1: `web/tests/pages.rs`' in joined
    assert 'web_suite/pages.rs' not in joined

    # The guard's own lists name files that moved; they are not references.
    _write(tmp_path, 'scripts/architecture/oversize_files.json',
           json.dumps({'crates/meerschaum-web/src/gone.rs': 1800}))
    assert 'gone.rs' not in '\n'.join(guards.check_test_references(tmp_path))

# ---------------------------------------------------------------------------
# Rule 7: crate layering
# ---------------------------------------------------------------------------


_LAYER_README = (
    '| Crate | Layer | Normal workspace dependencies | Notes |\n|---|---|---|---|\n'
    '| `meerschaum-server` | 3 | core, kernel | |\n'
    '| `meerschaum-kernel` | 2 | core | |\n'
    '| `meerschaum-core` | 1 | none | |\n'
)


def test_crate_layering_catches_table_drift_and_upward_edges(tmp_path):
    _crate(tmp_path, 'meerschaum-core')
    _crate(tmp_path, 'meerschaum-kernel', ('meerschaum-core',))
    _crate(tmp_path, 'meerschaum-server', ('meerschaum-core', 'meerschaum-kernel'),
           '\n[dev-dependencies]\nmeerschaum-web = { path = "../meerschaum-web" }\n')
    _write(tmp_path, 'crates/README.md', _LAYER_README)
    assert guards.check_crate_layering(tmp_path) == []

    # An undocumented edge, and one that points up (core -> kernel).
    _crate(tmp_path, 'meerschaum-core', ('meerschaum-kernel',))
    _crate(tmp_path, 'meerschaum-server', ('meerschaum-core',))
    joined = '\n'.join(guards.check_crate_layering(tmp_path))
    assert 'the `meerschaum-core` row says none; its Cargo.toml says kernel' in joined
    assert 'the `meerschaum-server` row says core, kernel; its Cargo.toml says core' in joined
    assert '`meerschaum-core` (layer 1) depends on `meerschaum-kernel` (layer 2)' in joined


# ---------------------------------------------------------------------------
# Rule 8: heavy imports
# ---------------------------------------------------------------------------


def test_heavy_imports_catch_module_level_imports(tmp_path):
    _write(tmp_path, 'meerschaum/__init__.py', 'from meerschaum.utils import x\n')
    _write(tmp_path, 'meerschaum/utils/x.py',
           'from typing import TYPE_CHECKING\nif TYPE_CHECKING:\n    import pandas\n'
           'def f():\n    import pandas as pd\n    from meerschaum.api import app\n    return pd, app\n')
    _write(tmp_path, 'meerschaum/api/__init__.py', 'import fastapi\nfrom meerschaum.api import dash\n')
    _write(tmp_path, 'meerschaum/api/dash/__init__.py', 'import dash\nfrom plotly import graph_objects\n')
    assert guards.check_heavy_imports(tmp_path) == []

    _write(tmp_path, 'meerschaum/utils/y.py', 'try:\n    import numpy as np\nexcept ImportError:\n    np = None\n')
    _write(tmp_path, 'meerschaum/core/z.py', 'class Z:\n    from polars import DataFrame\n')
    _write(tmp_path, 'meerschaum/api/w.py', 'import sqlalchemy\n')
    _write(tmp_path, 'meerschaum/actions/v.py', 'from meerschaum.api import app\n')
    _write(tmp_path, 'meerschaum/actions/u.py', 'from .. import api\n')
    joined = '\n'.join(guards.check_heavy_imports(tmp_path))
    assert 'meerschaum/utils/y.py:2: module-level import of `numpy`' in joined
    assert 'meerschaum/core/z.py:2: module-level import of `polars`' in joined
    assert 'meerschaum/api/w.py:1: module-level import of `sqlalchemy`' in joined
    assert 'meerschaum/actions/v.py:1: module-level import of `meerschaum.api`' in joined
    assert 'meerschaum/actions/u.py:1: module-level import of `meerschaum`' not in joined
    assert 'meerschaum/actions/u.py:1: module-level import of `meerschaum.api`' in joined


# ---------------------------------------------------------------------------
# Rule 9: ratchets
# ---------------------------------------------------------------------------


def _ratchet_tree(tmp_path: Path) -> Path:
    _write(tmp_path, 'meerschaum/big.py', 'x = 1\n' * (guards.PYTHON_LINE_LIMIT + 5))
    _write(tmp_path, 'meerschaum/noisy.py', 'print(1)\nprint(2)\n"""print(3) in a docstring"""\n')
    _write(tmp_path, 'tests/test_w12_feature.py', 'def test_x():\n    pass\n')
    data = tmp_path / 'scripts' / 'architecture'
    current = guards.measure_ratchets(tmp_path)
    for name in guards.RATCHETS:
        guards._write_ratchet(data, name, current[name])
    return data


def test_ratchets_refuse_new_and_grown_entries_and_ask_to_lower(tmp_path):
    data = _ratchet_tree(tmp_path)
    assert json.loads((data / 'print_calls.json').read_text())['meerschaum/noisy.py'] == 2
    assert guards.check_ratchets(tmp_path) == []

    _write(tmp_path, 'meerschaum/noisy.py', 'print(1)\nprint(2)\nprint(3)\n')
    _write(tmp_path, 'meerschaum/other.py', 'print("new")\n')
    _write(tmp_path, 'tests/test_rc40_more.py', '')
    _write(tmp_path, 'meerschaum/big.py', 'x = 1\n' * (guards.PYTHON_LINE_LIMIT + 2))
    joined = '\n'.join(guards.check_ratchets(tmp_path))
    assert 'meerschaum/noisy.py: 3, above its ceiling 2' in joined
    assert 'meerschaum/other.py: 1 is new to `print(` calls' in joined
    assert 'tests/test_rc40_more.py: 1 is new to files and directories named after development history' in joined
    assert f'meerschaum/big.py: {guards.PYTHON_LINE_LIMIT + 2}, below its ceiling {guards.PYTHON_LINE_LIMIT + 5}: ' \
           f'lower it to {guards.PYTHON_LINE_LIMIT + 2}' in joined

    (tmp_path / 'tests/test_w12_feature.py').unlink()
    guards.update_ratchets(tmp_path)
    assert json.loads((data / 'oversize_files.json').read_text())['meerschaum/big.py'] == guards.PYTHON_LINE_LIMIT + 2
    assert 'tests/test_w12_feature.py' not in json.loads((data / 'history_names.json').read_text())
    # Updating never adds or raises an entry.
    assert json.loads((data / 'print_calls.json').read_text())['meerschaum/noisy.py'] == 2
    assert 'meerschaum/other.py' not in json.loads((data / 'print_calls.json').read_text())


def test_ratchets_refuse_entries_added_since_the_base(tmp_path):
    data = _ratchet_tree(tmp_path)
    env = {**os.environ, 'GIT_AUTHOR_NAME': 't', 'GIT_AUTHOR_EMAIL': 't@example.com',
           'GIT_COMMITTER_NAME': 't', 'GIT_COMMITTER_EMAIL': 't@example.com'}
    git = lambda *args: subprocess.run(['git', *args], cwd=tmp_path, env=env, check=True, capture_output=True)  # noqa: E731
    git('init', '-q')
    git('add', '-A')
    git('commit', '-qm', 'base')
    base = subprocess.run(['git', 'rev-parse', 'HEAD'], cwd=tmp_path, capture_output=True, text=True).stdout.strip()
    assert guards.check_ratchets(tmp_path, base=base) == []

    # A new print, with its entry added to the list by hand: the tree agrees, the base does not.
    _write(tmp_path, 'meerschaum/other.py', 'print("new")\n')
    _write(tmp_path, 'meerschaum/noisy.py', 'print(1)\nprint(2)\nprint(3)\n')
    entries = json.loads((data / 'print_calls.json').read_text())
    entries.update({'meerschaum/other.py': 1, 'meerschaum/noisy.py': 3})
    (data / 'print_calls.json').write_text(json.dumps(entries))
    joined = '\n'.join(guards.check_ratchets(tmp_path, base=base))
    assert '`meerschaum/other.py` was added since' in joined
    assert '`meerschaum/noisy.py` was raised from 2 to 3 since' in joined
    assert 'is new to' not in joined and 'above its ceiling' not in joined


# ---------------------------------------------------------------------------
# Rule 10: Cargo invariants
# ---------------------------------------------------------------------------


def _write_valid_cargo_tree(tmp_path: Path) -> None:
    _write(
        tmp_path, 'Cargo.toml',
        '[workspace]\nmembers = ["crates/foo"]\n\n'
        '[workspace.lints.rust]\nunsafe_code = "deny"\n\n'
        '[workspace.lints.clippy]\ndbg_macro = "deny"\ntodo = "deny"\nunimplemented = "deny"\n\n'
        '[workspace.dependencies]\n'
        'arrow = { version = "60", default-features = false, features = ["ipc"] }\n'
        'polars = { version = "0.55", default-features = false, features = ["ipc"] }\n\n'
        '[profile.release]\nstrip = "symbols"\n\n[profile.ci]\nstrip = "none"\n',
    )
    _write(tmp_path, 'crates/foo/Cargo.toml', '[package]\nname = "foo"\nversion = "0.1.0"\n\n[lints]\nworkspace = true\n')
    _write(tmp_path, 'crates/foo/src/lib.rs', '#![deny(unsafe_code)]\n')


def test_cargo_invariants_catch_manifest_regressions(tmp_path):
    _write_valid_cargo_tree(tmp_path)
    assert guards.check_cargo_invariants(tmp_path, resolve=False) == []

    _write(
        tmp_path, 'Cargo.toml',
        '[workspace]\nmembers = ["crates/foo"]\n\n[workspace.lints.clippy]\ndbg_macro = "warn"\n\n'
        '[workspace.dependencies]\narrow = "60"\npolars = { version = "0.55", features = ["ipc"] }\n'
        'sqlx = { version = "0.9", features = ["tls-rustls"] }\n\n'
        '[profile.release]\nstrip = "none"\npanic = "abort"\n\n[profile.ci]\nstrip = "symbols"\n',
    )
    _write(tmp_path, 'crates/foo/Cargo.toml', '[package]\nname = "foo"\nversion = "0.1.0"\n\n[dev-dependencies]\nrcgen = "0.14"\n')
    _write(tmp_path, 'crates/foo/src/lib.rs', '#![allow(unsafe_code)]\n')
    joined = '\n'.join(guards.check_cargo_invariants(tmp_path, resolve=False))
    for needle in ('strip = "symbols"', 'panic = "abort"', 'strip = "none"', 'polars', 'arrow', 'sqlx', 'rcgen',
                   'dbg_macro', 'todo', 'unimplemented', '[workspace.lints.rust].unsafe_code',
                   'crates/foo/Cargo.toml: must inherit', 'crates/foo/src/lib.rs: `#![allow(unsafe_code)]`'):
        assert needle in joined, needle


def test_cargo_invariants_resolve_ring_activation_not_the_lockfile(tmp_path, monkeypatch):
    _write_valid_cargo_tree(tmp_path)
    # The lockfile may list `ring` (an optional dependency nothing turns on): not a violation.
    _write(tmp_path, 'Cargo.lock', 'version = 4\n\n[[package]]\nname = "ring"\nversion = "0.17.14"\n')
    bin_dir = tmp_path / 'bin'
    cargo = _write(bin_dir, 'cargo', '#!/bin/sh\nprintf "%s" "$FAKE_TREE"\n')
    cargo.chmod(0o755)
    monkeypatch.setenv('PATH', str(bin_dir))
    monkeypatch.setenv('FAKE_TREE', '')
    assert guards.check_cargo_invariants(tmp_path) == []

    monkeypatch.setenv('FAKE_TREE', 'ring v0.17.14\nrustls v0.23.46\n')
    joined = '\n'.join(guards.check_cargo_invariants(tmp_path))
    assert '`ring` is built into the workspace' in joined and 'rustls v0.23.46' in joined

    monkeypatch.setenv('PATH', str(tmp_path / 'empty'))
    assert guards.check_cargo_invariants(tmp_path) == []
    assert any('could not resolve' in v for v in guards.check_cargo_invariants(tmp_path, require_cargo=True))
