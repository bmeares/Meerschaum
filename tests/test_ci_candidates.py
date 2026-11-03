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


def test_only_unfinished_candidates_at_the_exact_commit_are_waited_for(monkeypatch):
    monkeypatch.setenv('GITHUB_REPOSITORY', REPO)
    runs = [
        {'id': 3, 'head_sha': HEAD, 'status': 'in_progress'},
        {'id': 2, 'head_sha': HEAD, 'status': 'queued'},
        {'id': 1, 'head_sha': HEAD, 'status': 'completed'},
        {'id': 4, 'head_sha': 'b' * 40, 'status': 'in_progress'},
    ]
    paths = []
    def api(path):
        paths.append(path)
        return {'workflow_runs': runs}
    monkeypatch.setattr(candidates, 'api', api)
    assert candidates.in_flight(HEAD) == [2, 3]
    assert f'head_sha={HEAD}' in paths[0] and 'event=workflow_dispatch' in paths[0]
    assert 'status=' not in paths[0]
    with pytest.raises(ValueError, match='full head SHA'):
        candidates.in_flight('main')


def _same_tree(monkeypatch, checkout_tree='c' * 40):
    monkeypatch.setenv('GITHUB_REPOSITORY', REPO)
    monkeypatch.setattr(candidates, 'api', lambda path: {'commit': {'tree': {'sha': 'c' * 40}}})
    monkeypatch.setattr(candidates.gate.artifacts, 'checkout', lambda: {'commit': HEAD, 'tree': checkout_tree})


def test_a_merge_tree_that_cannot_reuse_the_candidate_does_not_wait(monkeypatch):
    _same_tree(monkeypatch, checkout_tree='d' * 40)
    monkeypatch.setattr(candidates, 'in_flight', lambda head: pytest.fail('must not poll'))
    candidates.wait_for_candidates(HEAD, 75, sleep=lambda seconds: pytest.fail('must not wait'))


def test_waiting_is_bounded_and_stops_when_the_candidate_finishes(monkeypatch):
    _same_tree(monkeypatch)
    now = [0.0]
    sleeps = []
    def sleep(seconds):
        sleeps.append(seconds)
        now[0] += seconds
    pending = iter([[7], [7], []])
    monkeypatch.setattr(candidates, 'in_flight', lambda head: next(pending))
    candidates.wait_for_candidates(HEAD, 10, poll=30, clock=lambda: now[0], sleep=sleep)
    assert sleeps == [30, 30]
    monkeypatch.setattr(candidates, 'in_flight', lambda head: [7])
    sleeps.clear()
    now[0] = 0.0
    candidates.wait_for_candidates(HEAD, 2, poll=30, clock=lambda: now[0], sleep=sleep)
    assert sum(sleeps) == 120  # Never past the deadline: then the ordinary checks run.


def test_a_failed_wait_falls_back_to_the_ordinary_checks(monkeypatch, tmp_path):
    monkeypatch.setenv('GITHUB_REPOSITORY', REPO)
    output = tmp_path / 'output'
    def broken(head, minutes):
        raise candidates.subprocess.CalledProcessError(1, ['gh'])
    monkeypatch.setattr(candidates, 'wait_for_candidates', broken)
    monkeypatch.setattr(candidates, 'reuse', lambda head, directory: pytest.fail('no reuse after a failed wait'))
    monkeypatch.setattr('sys.argv', ['candidates.py', '--head-sha', HEAD, '--github-output', str(output),
                                     '--wait-minutes', '75'])
    candidates.main()
    assert 'reused=false' in output.read_text()


@pytest.mark.parametrize('workflow', ['rust.yaml', 'pytest.yaml'])
def test_push_and_pr_checks_wait_for_a_running_candidate_within_their_timeout(workflow):
    import yaml
    path = Path(__file__).resolve().parents[1] / '.github/workflows' / workflow
    job = yaml.safe_load(path.read_text())['jobs']['changes']
    step = next(s for s in job['steps'] if s.get('id') == 'reuse')
    assert "github.event_name == 'push' && github.ref_type == 'branch'" in step['if']
    assert 'github.event.pull_request.head.repo.full_name == github.repository' in step['if']
    assert step['env']['HEAD_SHA'] == '${{ github.event.pull_request.head.sha || github.sha }}'
    wait = float(step['run'].split('--wait-minutes ')[1].split()[0])
    assert 0 < wait and wait + 10 <= job['timeout-minutes']
