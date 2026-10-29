"""PR lane selection preserves conservative full coverage for uncertain and release inputs."""

import importlib.util
from pathlib import Path

import pytest

spec = importlib.util.spec_from_file_location('ci_test_plan', Path(__file__).resolve().parents[1] / 'scripts/ci/test_plan.py')
test_plan = importlib.util.module_from_spec(spec)
spec.loader.exec_module(test_plan)


def shards(plan):
    return {entry['shard'] for entry in plan['matrix']['include']}


def test_docs_changes_do_not_start_database_or_build_jobs():
    result = test_plan.plan(['docs/zensical/get-started/index.md', 'README.md'])
    assert result['pytest'] is False


def test_normal_pr_retains_core_and_postgresql_coverage():
    result = test_plan.plan(['meerschaum/core/Pipe/_sync.py'])
    assert result['pytest'] and shards(result) == {'core', 'postgres'}


@pytest.mark.parametrize('path,lane', [
    ('crates/meerschaum-connector-sql/src/driver/mysql.rs', 'mysql'),
    ('tests/test_mariadb.py', 'mysql'),
    ('meerschaum/connectors/valkey/__init__.py', 'mssql-valkey'),
    ('requirements/sql-mssql.txt', 'mssql-valkey'),
    ('crates/meerschaum-connector-sql/src/driver/oracle.rs', 'oracle'),
])
def test_backend_changes_add_their_required_lane(path, lane):
    assert {'core', 'postgres', lane} <= shards(test_plan.plan([path]))


@pytest.mark.parametrize('path', [
    'some/unclassified/input.bin', 'scripts/ci/artifacts.py', 'tests/conftest.py',
    'scripts/test.sh', 'Cargo.lock', '.github/workflows/release.yaml',
])
def test_unknown_inputs_and_harness_changes_require_every_lane(path):
    assert shards(test_plan.plan([path])) == set(test_plan.LANES)


def test_full_validation_is_exhaustive_and_has_one_shared_worker_budget():
    result = test_plan.plan([], full=True)
    assert result['pytest'] and result['full']
    assert shards(result) == set(test_plan.LANES)
    assert sum(x['workers'] for x in result['matrix']['include']) == 16


def test_explicit_diagnostic_lane_cannot_weaken_full_validation():
    with pytest.raises(ValueError, match='every test lane'):
        test_plan.plan([], full=True, requested='core')
    assert shards(test_plan.plan([], requested='oracle')) == {'oracle'}
    with pytest.raises(ValueError, match='Unknown shard'):
        test_plan.plan([], requested='typo')
