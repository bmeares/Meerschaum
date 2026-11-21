"""Docker promotion validates every image before publishing any of the catalog."""
from copy import deepcopy
import hashlib
import importlib.util
import json
from pathlib import Path
import subprocess
import zipfile

import pytest

ROOT = Path(__file__).resolve().parents[1]
spec = importlib.util.spec_from_file_location('test_docker_candidate', ROOT / 'scripts/ci/docker_candidate.py')
docker = importlib.util.module_from_spec(spec)
spec.loader.exec_module(docker)
SOURCE = {'commit': 'a' * 40, 'tree': 'b' * 40}


@pytest.mark.parametrize('missing_case', [False, True])
def test_executor_checks_use_the_sealed_artifacts_and_require_all_cases(tmp_path, monkeypatch, missing_case):
    (tmp_path / 'wheels').mkdir()
    wheel = tmp_path / 'wheels' / 'test.whl'
    with zipfile.ZipFile(wheel, 'w') as archive:
        archive.writestr('meerschaum/_core/_native.abi3.so', b'test extension')
    cli = tmp_path / 'mrsm'
    cli.write_bytes(b'test binary')
    info = {'wheel_sha256': docker.gate.artifacts.sha256(wheel),
            'binary_sha256': docker.gate.artifacts.sha256(cli)}
    monkeypatch.setenv('TMUX', 'test-parent')
    monkeypatch.setenv('MRSM_SQL_MAIN', 'inherited-test-value')

    def run(args, *, env, **kwargs):
        assert set(args[args.index('--test-threads=1') + 1:]) == set(docker.EXECUTOR_CASES)
        assert 'TMUX' not in env and 'MRSM_SQL_MAIN' not in env
        assert Path(env['MRSM_ROOT_DIR']).is_dir() and Path(env['MRSM_PLUGINS_DIR']).is_dir()
        assert env['MRSM_TEST_EXECUTOR_IMAGE'] == 'fixture-id'
        assert env['MRSM_TEST_EXECUTOR_NATIVE_SHA256'] == info['binary_sha256']
        assert env['MRSM_TEST_EXECUTOR_SDK_SHA256'] == hashlib.sha256(b'test extension').hexdigest()
        count = len(docker.EXECUTOR_CASES) - int(missing_case)
        return subprocess.CompletedProcess(args, 0, f'test result: ok. {count} passed;')

    monkeypatch.setattr(docker.subprocess, 'run', run)
    if missing_case:
        with pytest.raises(ValueError, match='every selected case'):
            docker.check_executor_image('fixture-id', tmp_path, info)
    else:
        docker.check_executor_image('fixture-id', tmp_path, info)


REPOSITORY = 'ghcr.io/owner/repo/candidate'


def _digest(n):
    return 'sha256:' + f'{n:064x}'


@pytest.fixture
def staged(tmp_path, monkeypatch):
    """A candidate's receipts and a fake staging registry holding exactly its digests."""
    monkeypatch.setattr(docker.gate.artifacts, 'checkout', lambda: SOURCE)
    monkeypatch.setattr(docker, 'version', lambda: '5.0.0rc25')
    monkeypatch.setattr(docker.candidates, 'repository', lambda: 'Owner/Repo')
    monkeypatch.setenv('GITHUB_RUN_ID', '456')
    images = {}
    for i, target in enumerate(('slim', 'full', 'rust', 'rust-full'), 1):
        images[target] = {'digest': _digest(i), 'config': _digest(100 + i)}
    images['minimal'] = dict(images['slim'])
    record = {'schema': 2, 'source': SOURCE, 'version': '5.0.0rc25', 'platform': 'linux/amd64',
              'sdk': {'source': SOURCE, 'candidate_run_id': '123', 'platform': 'linux/amd64'},
              'run_id': '456', 'repository': REPOSITORY, 'images': images,
              'checks': {'runtime_smokes': 'success', 'size_gate': 'success', 'facade_smoke': 'success'}}
    executors = {'schema': 2, 'source': SOURCE, 'run_id': '456', 'platform': 'linux/amd64',
                 'image': images['full'], 'installed_team_executors': 'success'}
    receipts = tmp_path / 'receipts'
    (receipts / 'candidate').mkdir(parents=True)
    (receipts / 'executors').mkdir()
    (receipts / 'candidate' / 'docker-candidate.json').write_text(json.dumps(record))
    (receipts / 'executors' / 'docker-executors.json').write_text(json.dumps(executors))
    registry = {}
    for entry in images.values():
        registry[f'{REPOSITORY}@{entry["digest"]}'] = entry
    user = {'value': '1000:1000'}

    def registry_image(reference):
        entry = registry.get(reference)
        if entry is None:
            raise subprocess.CalledProcessError(1, ['docker', 'buildx', 'imagetools', 'inspect'])
        manifest = {'mediaType': 'application/vnd.oci.image.manifest.v1+json',
                    'config': {'digest': entry['config']}}
        config = {'os': 'linux', 'architecture': 'amd64',
                  'config': {'User': user['value'],
                             'Labels': {'org.opencontainers.image.revision': SOURCE['commit']}}}
        return manifest, config

    monkeypatch.setattr(docker, 'registry_image', registry_image)
    return receipts, record, registry, user


def test_publication_copies_the_tested_digests_without_a_build(staged, monkeypatch):
    receipts, record, registry, _ = staged
    calls = []

    def run(args, **kwargs):
        calls.append(args)
        tag, source = args[-2], args[-1]
        registry[tag] = registry[source]

    monkeypatch.setattr(docker.subprocess, 'run', run)
    monkeypatch.setattr(docker, 'registry_digest', lambda tag: next(
        e['digest'] for e in record['images'].values() if registry[tag] is not None and e['config'] == registry[tag]['config']))
    docker.promote(receipts, 'linux/amd64', '123')
    assert len(calls) == len(docker.GROUPS)
    for group in docker.GROUPS:
        assert ['docker', 'buildx', 'imagetools', 'create', '--prefer-index=false',
                '-t', f'bmeares/meerschaum:5.0.0rc25-{group}-amd64',
                f'{REPOSITORY}@{record["images"][group]["digest"]}'] in calls
    flat = ' '.join(' '.join(c) for c in calls)
    assert 'latest' not in flat and ' push' not in flat and 'buildx build' not in flat


def test_one_missing_digest_prevents_every_registry_mutation(staged, monkeypatch):
    receipts, record, registry, _ = staged
    del registry[f'{REPOSITORY}@{record["images"]["rust-full"]["digest"]}']
    monkeypatch.setattr(docker.subprocess, 'run', lambda *a, **k: pytest.fail('Publication started before validation'))
    with pytest.raises(docker.MissingImages, match='rust-full'):
        docker.promote(receipts, 'linux/amd64', '123')


@pytest.mark.parametrize('damage', [
    'source', 'sdk', 'platform', 'checks', 'catalog', 'digest', 'repository', 'aliases', 'schema',
    'no_executors', 'executors_other_image', 'executors_other_run', 'runner_user', 'registry_index',
])
def test_invalid_candidate_cannot_be_promoted(staged, monkeypatch, damage):
    receipts, record, registry, user = staged
    record = deepcopy(record)
    executors_path = receipts / 'executors' / 'docker-executors.json'
    executors = json.loads(executors_path.read_text())
    if damage == 'source': record['source'] = {'commit': 'c' * 40, 'tree': 'd' * 40}
    elif damage == 'sdk': record['sdk']['candidate_run_id'] = '999'
    elif damage == 'platform': record['platform'] = 'linux/arm64'
    elif damage == 'checks': record['checks']['size_gate'] = 'skipped'
    elif damage == 'catalog': record['images'].pop('slim')
    elif damage == 'digest': record['images']['full']['digest'] = 'invalid'
    elif damage == 'repository': record['repository'] = 'ghcr.io/someone-else/repo/candidate'
    elif damage == 'aliases': record['images']['minimal'] = {'digest': _digest(77), 'config': _digest(78)}
    elif damage == 'schema': record['schema'] = 1
    elif damage == 'no_executors': executors_path.unlink()
    elif damage == 'executors_other_image': executors['image'] = record['images']['rust']
    elif damage == 'executors_other_run': executors['run_id'] = '457'
    elif damage == 'runner_user': user['value'] = '1001:1001'
    else:
        original = docker.registry_image
        monkeypatch.setattr(docker, 'registry_image', lambda ref: (
            {**original(ref)[0], 'manifests': []}, original(ref)[1]))
    (receipts / 'candidate' / 'docker-candidate.json').write_text(json.dumps(record))
    if executors_path.exists():
        executors_path.write_text(json.dumps(executors))
    monkeypatch.setattr(docker.subprocess, 'run', lambda *a, **k: pytest.fail('An invalid candidate was published'))
    with pytest.raises((ValueError, OSError)):
        docker.promote(receipts, 'linux/amd64', '123')


def test_arm64_publication_needs_no_executor_receipt(staged, monkeypatch):
    receipts, record, registry, _ = staged
    record = deepcopy(record)
    record['platform'] = record['sdk']['platform'] = 'linux/arm64'
    (receipts / 'candidate' / 'docker-candidate.json').write_text(json.dumps(record))
    (receipts / 'executors' / 'docker-executors.json').unlink()
    original = docker.registry_image
    monkeypatch.setattr(docker, 'registry_image', lambda ref: (
        original(ref)[0], {**original(ref)[1], 'architecture': 'arm64'}))
    assert docker.check_publication(receipts, 'linux/arm64', '123')['platform'] == 'linux/arm64'


def test_a_registry_digest_mismatch_fails_publication(staged, monkeypatch):
    receipts, _, _, _ = staged
    monkeypatch.setattr(docker.subprocess, 'run', lambda *a, **k: None)
    monkeypatch.setattr(docker, 'registry_digest', lambda tag: _digest(999))
    with pytest.raises(ValueError, match='published.*digest'):
        docker.promote(receipts, 'linux/amd64', '123')


def test_promotion_checks_the_freshly_verified_sdk_receipt(staged, tmp_path):
    receipts, record, _, _ = staged
    sdk = tmp_path / 'sdk'
    sdk.mkdir()
    info = deepcopy(record['sdk'])
    info['binary_sha256'] = 'e' * 64
    (sdk / 'sdk-info.json').write_text(json.dumps(info))
    with pytest.raises(ValueError, match='SDK checksums'):
        docker.check_publication(receipts, 'linux/amd64', '123', sdk=sdk)


def _metadata(tmp_path, record):
    metadata = tmp_path / 'metadata'
    metadata.mkdir()
    for target in ('slim', 'full', 'rust', 'rust-full'):
        entry = record['images'][target]
        (metadata / f'{target}.json').write_text(json.dumps({
            'containerimage.digest': entry['digest'], 'containerimage.config.digest': entry['config'],
        }))
    return metadata


def test_the_receipt_records_the_pushed_digests_after_checking_the_registry(staged, tmp_path):
    _, record, _, _ = staged
    sdk = tmp_path / 'sdk'
    sdk.mkdir()
    (sdk / 'sdk-info.json').write_text(json.dumps(record['sdk']))
    output = tmp_path / 'docker-candidate.json'
    written = docker.record_images(output, 'linux/amd64', sdk, _metadata(tmp_path, record), REPOSITORY)
    assert written['images'] == record['images'] and written['schema'] == 2
    assert json.loads(output.read_text())['repository'] == REPOSITORY
    with pytest.raises(ValueError, match='GHCR namespace'):
        docker.record_images(output, 'linux/amd64', sdk, tmp_path / 'metadata', 'ghcr.io/elsewhere/candidate')


def test_the_receipt_refuses_an_image_built_as_the_runner_user(staged, tmp_path):
    _, record, _, user = staged
    user['value'] = '1001:1001'
    sdk = tmp_path / 'sdk'
    sdk.mkdir()
    (sdk / 'sdk-info.json').write_text(json.dumps(record['sdk']))
    with pytest.raises(ValueError, match='user'):
        docker.record_images(tmp_path / 'out.json', 'linux/amd64', sdk, _metadata(tmp_path, record), REPOSITORY)


@pytest.mark.parametrize('local_id', ['config', 'digest', 'other'])
def test_executors_run_on_the_pulled_tested_digest(staged, tmp_path, monkeypatch, local_id):
    receipts, record, _, _ = staged
    sdk = tmp_path / 'sdk'
    sdk.mkdir()
    (sdk / 'sdk-info.json').write_text(json.dumps(record['sdk']))
    full = record['images']['full']
    pulled, tested = [], []
    monkeypatch.setattr(docker.subprocess, 'run', lambda args, **k: pulled.append(args))
    image_id = full.get(local_id, _digest(555))
    monkeypatch.setattr(docker, 'command', lambda *args: [{'Id': image_id}])
    monkeypatch.setattr(docker, 'check_executor_image', lambda image, s, info: tested.append(image))
    output = tmp_path / 'docker-executors.json'
    path = receipts / 'candidate' / 'docker-candidate.json'
    if local_id == 'other':
        with pytest.raises(ValueError, match='differs'):
            docker.check_executors(path, output, 'linux/amd64', '123', sdk)
        assert tested == []
        return
    receipt = docker.check_executors(path, output, 'linux/amd64', '123', sdk)
    assert pulled == [['docker', 'pull', '--quiet', '--platform', 'linux/amd64', f'{REPOSITORY}@{full["digest"]}']]
    assert tested == [image_id]
    assert receipt['image'] == full and docker.executor_record(output, record) == receipt
