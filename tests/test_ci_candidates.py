"""Only successful same-repository candidates can replace PR checks."""
from copy import deepcopy
import importlib.util
import json
from pathlib import Path

import pytest

spec = importlib.util.spec_from_file_location('test_candidates', Path(__file__).resolve().parents[1] / 'scripts/ci/candidates.py')
candidates = importlib.util.module_from_spec(spec)
spec.loader.exec_module(candidates)
HEAD = 'a' * 40
REPO = 'owner/project'
RUN = {'id': 123, 'head_sha': HEAD, 'conclusion': 'success', 'event': 'workflow_dispatch',
       'path': '.github/workflows/release.yaml', 'head_repository': {'full_name': REPO}}


@pytest.mark.parametrize('field,value', [
    ('head_sha', 'b' * 40), ('conclusion', 'failure'), ('event', 'push'),
    ('path', '.github/workflows/pytest.yaml'), ('head_repository', {'full_name': 'fork/project'}),
])
def test_an_untrusted_or_wrong_source_run_cannot_supply_proof(monkeypatch, field, value):
    monkeypatch.setenv('GITHUB_REPOSITORY', REPO)
    run = deepcopy(RUN)
    run[field] = value
    monkeypatch.setattr(candidates, 'api', lambda path: run)
    with pytest.raises(ValueError, match='successful trusted run'):
        candidates.trusted_run('123', 'release.yaml', commit=HEAD)


def test_expired_payloads_are_not_available(monkeypatch):
    monkeypatch.setenv('GITHUB_REPOSITORY', REPO)
    monkeypatch.setattr(candidates, 'api', lambda path: {'artifacts': [
        {'name': 'release-candidate-manifest', 'expired': False},
        {'name': 'wheels-ubuntu-latest', 'expired': True},
    ]})
    assert candidates.available('123') == {'release-candidate-manifest'}


@pytest.mark.parametrize('damage', [None, 'expired_payload', 'wrong_run', 'wrong_repo', 'wrong_tree'])
def test_pr_reuse_falls_back_unless_all_proof_matches(monkeypatch, tmp_path, damage):
    monkeypatch.setenv('GITHUB_REPOSITORY', REPO)
    record = {'run_id': '123', 'repository': REPO, 'source': {'commit': HEAD, 'tree': 'b' * 40}}
    if damage == 'wrong_run': record['run_id'] = '999'
    if damage == 'wrong_repo': record['repository'] = 'fork/project'
    def api(path):
        return {'workflow_runs': [RUN]} if '/workflows/' in path else RUN
    monkeypatch.setattr(candidates, 'api', api)
    monkeypatch.setattr(candidates.gate, 'expected_catalog', lambda: ({'wheel': {}}, {}))
    monkeypatch.setattr(candidates, 'available', lambda run: {'release-candidate-manifest'} | (
        set() if damage == 'expired_payload' else {'wheel'}))
    def download(run, directory, *names):
        directory.mkdir(parents=True)
        (directory / 'release-candidate.json').write_text(json.dumps(record))
    monkeypatch.setattr(candidates, 'download', download)
    def equivalent(path):
        if damage == 'wrong_tree': raise ValueError('different merge tree')
        return json.loads(path.read_text())
    monkeypatch.setattr(candidates.gate, 'equivalent', equivalent)
    result = candidates.reuse(HEAD, tmp_path)
    assert (result is not None) == (damage is None)
