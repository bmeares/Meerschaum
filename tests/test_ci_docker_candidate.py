"""Docker promotion validates every image before publishing any of the catalog."""
from copy import deepcopy
import importlib.util
import json
from pathlib import Path
import subprocess

import pytest

ROOT = Path(__file__).resolve().parents[1]
spec = importlib.util.spec_from_file_location('test_docker_candidate', ROOT / 'scripts/ci/docker_candidate.py')
docker = importlib.util.module_from_spec(spec)
spec.loader.exec_module(docker)
SOURCE = {'commit': 'a' * 40, 'tree': 'b' * 40}


@pytest.fixture
def receipt(tmp_path, monkeypatch):
    monkeypatch.setattr(docker.gate.artifacts, 'checkout', lambda: SOURCE)
    monkeypatch.setattr(docker, 'version', lambda: '5.0.0rc25')
    images = {group: {'id': 'sha256:' + f'{i:064x}', 'local_tag': f'candidate-{group}'}
              for i, group in enumerate(docker.GROUPS, 1)}
    record = {'schema': 1, 'source': SOURCE, 'version': '5.0.0rc25', 'platform': 'linux/amd64',
              'sdk': {'source': SOURCE, 'candidate_run_id': '123', 'platform': 'linux/amd64'},
              'run_id': '456', 'images': images,
              'checks': {'runtime_smokes': 'success', 'size_gate': 'success', 'facade_smoke': 'success'}}
    path = tmp_path / 'docker-candidate.json'
    path.write_text(json.dumps(record))
    def inspect(reference):
        return {'Id': reference, 'Os': 'linux', 'Architecture': 'amd64',
                'Config': {'Labels': {'org.opencontainers.image.revision': SOURCE['commit']}}}
    monkeypatch.setattr(docker, 'inspect_image', inspect)
    return path, record


def test_publication_tags_the_tested_ids_without_a_build(receipt, monkeypatch):
    path, record = receipt
    calls = []
    monkeypatch.setattr(docker.subprocess, 'run', lambda args, **kw: calls.append(args))
    def registry(*args):
        assert args[:5] == ('docker', 'buildx', 'imagetools', 'inspect', '--raw')
        group = args[-1].removeprefix('bmeares/meerschaum:5.0.0rc25-').removesuffix('-amd64')
        return {'config': {'digest': record['images'][group]['id']}}
    monkeypatch.setattr(docker, 'command', registry)
    docker.promote(path, 'linux/amd64', '123')
    assert len(calls) == 10
    for index, group in enumerate(sorted(docker.GROUPS)):
        # JSON key ordering is irrelevant: every recorded ID is used directly.
        assert any(c[:3] == ['docker', 'tag', record['images'][group]['id']] for c in calls)
    assert all(c[1] in ('tag', 'push') for c in calls)
    assert all('latest' not in ' '.join(c) for c in calls)


def test_one_missing_image_prevents_every_registry_mutation(receipt, monkeypatch):
    path, record = receipt
    original = docker.inspect_image
    missing = record['images']['rust-full']['id']
    def inspect(reference):
        if reference == missing:
            raise subprocess.CalledProcessError(1, ['docker', 'image', 'inspect'])
        return original(reference)
    monkeypatch.setattr(docker, 'inspect_image', inspect)
    monkeypatch.setattr(docker.subprocess, 'run', lambda *a, **k: pytest.fail('Publication started before validation'))
    with pytest.raises(docker.MissingImages):
        docker.promote(path, 'linux/amd64', '123')


@pytest.mark.parametrize('damage', ['source', 'sdk', 'platform', 'checks', 'catalog', 'id', 'local_source'])
def test_invalid_candidate_cannot_be_promoted(receipt, monkeypatch, damage):
    path, record = receipt
    record = deepcopy(record)
    if damage == 'source': record['source'] = {'commit': 'c' * 40, 'tree': 'd' * 40}
    elif damage == 'sdk': record['sdk']['candidate_run_id'] = '999'
    elif damage == 'platform': record['platform'] = 'linux/arm64'
    elif damage == 'checks': record['checks']['size_gate'] = 'skipped'
    elif damage == 'catalog': record['images'].pop('slim')
    elif damage == 'id': record['images']['full']['id'] = 'invalid'
    else:
        original = docker.inspect_image
        def inspect(ref):
            image = original(ref)
            image['Config']['Labels']['org.opencontainers.image.revision'] = 'e' * 40
            return image
        monkeypatch.setattr(docker, 'inspect_image', inspect)
    path.write_text(json.dumps(record))
    monkeypatch.setattr(docker.subprocess, 'run', lambda *a, **k: pytest.fail('An invalid candidate was published'))
    with pytest.raises(ValueError):
        docker.promote(path, 'linux/amd64', '123')


def test_a_registry_digest_mismatch_fails_publication(receipt, monkeypatch):
    path, _ = receipt
    monkeypatch.setattr(docker.subprocess, 'run', lambda *a, **k: None)
    monkeypatch.setattr(docker, 'command', lambda *a: {'config': {'digest': 'sha256:' + '0' * 64}})
    with pytest.raises(ValueError, match='pushed.*config digest'):
        docker.promote(path, 'linux/amd64', '123')


def test_promotion_checks_the_freshly_verified_sdk_receipt(receipt, tmp_path):
    path, record = receipt
    sdk = tmp_path / 'sdk'
    sdk.mkdir()
    info = deepcopy(record['sdk'])
    info['binary_sha256'] = 'e' * 64
    (sdk / 'sdk-info.json').write_text(json.dumps(info))
    with pytest.raises(ValueError, match='SDK checksums'):
        docker.validate_record(path, 'linux/amd64', '123', sdk=sdk)
