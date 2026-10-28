"""Seal and recheck complete release catalogs without building or publishing artifacts."""

import importlib.util
import io
import json
from pathlib import Path
import shutil
from types import SimpleNamespace
import tarfile
import zipfile

import pytest

from tests.test_ci_artifacts import git, producer


REPO = Path(__file__).resolve().parents[1]
spec = importlib.util.spec_from_file_location('ci_release_gate', REPO / 'scripts/ci/release_gate.py')
gate = importlib.util.module_from_spec(spec)
spec.loader.exec_module(gate)

WHEEL_PLATFORMS = {
    'x86_64-unknown-linux-gnu': 'manylinux_2_28_x86_64',
    'aarch64-unknown-linux-gnu': 'manylinux_2_28_aarch64',
    'x86_64-unknown-linux-musl': 'musllinux_1_2_x86_64',
    'aarch64-unknown-linux-musl': 'musllinux_1_2_aarch64',
    'aarch64-apple-darwin': 'macosx_11_0_arm64',
    'x86_64-apple-darwin': 'macosx_10_12_x86_64',
    'x86_64-pc-windows-msvc': 'win_amd64',
}
WHEEL_NAMES = (
    'wheels-ubuntu-latest', 'wheels-ubuntu-24.04-arm',
    'wheels-musllinux-x86_64', 'wheels-musllinux-aarch64',
    'wheels-macos-latest', 'wheels-macos-15-intel', 'wheels-windows-latest',
)
BINARY_TARGETS = (
    'x86_64-unknown-linux-gnu', 'aarch64-unknown-linux-gnu',
    'aarch64-apple-darwin', 'x86_64-apple-darwin', 'x86_64-pc-windows-msvc',
)
REQUIRED_NAMES = (*WHEEL_NAMES, *(f'mrsm-5.0.0-{target}' for target in BINARY_TARGETS), 'sdist')
CHECKS = {'wheels': 'success', 'rust': 'success', 'python': 'success', 'docs': 'success'}


def add_artifact(candidate, name, expected):
    directory = candidate.directory / name
    directory.mkdir()
    if expected['kind'] == 'wheel':
        abi = 'cp314-cp314t' if name.endswith('-cp314t') else 'cp311-abi3'
        tag = f'{abi}-{WHEEL_PLATFORMS[expected["target"]]}'
        with zipfile.ZipFile(directory / f'meerschaum-5.0.0-{tag}.whl', 'w') as archive:
            archive.writestr('meerschaum-5.0.0.dist-info/METADATA',
                             'Name: meerschaum\nVersion: 5.0.0\n')
            archive.writestr('meerschaum-5.0.0.dist-info/WHEEL', f'Wheel-Version: 1.0\nTag: {tag}\n')
            archive.writestr('meerschaum/_core/_native', b'synthetic-extension')
    elif expected['kind'] == 'binary':
        (directory / expected['program']).write_bytes(b'synthetic-native-program')
    else:
        with tarfile.open(directory / 'meerschaum-5.0.0.tar.gz', 'w:gz') as archive:
            data = (candidate.repo / 'pyproject.toml').read_bytes()
            entry = tarfile.TarInfo('meerschaum-5.0.0/pyproject.toml')
            entry.size = len(data)
            archive.addfile(entry, io.BytesIO(data))
    return gate.artifacts.create_manifest(
        directory, directory / 'provenance.json', kind=expected['kind'], profile='release',
        target=expected['target'], toolchain_file=candidate.compiler, repo=candidate.repo,
    )


@pytest.fixture
def candidate(producer, tmp_path):
    repo, compiler, directory = producer
    (repo / 'Cargo.toml').write_text('[workspace.package]\nversion = "5.0.0"\n')
    git(repo, 'add', 'Cargo.toml')
    git(repo, 'commit', '-qm', 'Final-version fixture')
    required, optional = gate.expected_catalog(repo)
    result = SimpleNamespace(repo=repo, compiler=compiler, directory=directory,
                             manifest=tmp_path / 'release-candidate.json',
                             required=required, optional=optional)
    for name, expected in required.items():
        add_artifact(result, name, expected)
    return result


def seal(candidate, checks=None):
    return gate.create(candidate.directory, candidate.manifest,
                       dict(CHECKS) if checks is None else checks, repo=candidate.repo)


def verify(candidate):
    return gate.verify(candidate.directory, candidate.manifest, repo=candidate.repo)


@pytest.mark.parametrize('experimental', [False, True])
def test_complete_final_catalog_seals_and_verifies(candidate, experimental):
    assert set(candidate.required) == set(REQUIRED_NAMES)
    assert {entry['target'] for entry in candidate.required.values() if entry['kind'] == 'wheel'} == set(WHEEL_PLATFORMS)
    assert {entry['target'] for entry in candidate.required.values() if entry['kind'] == 'binary'} == set(BINARY_TARGETS)
    assert set(candidate.optional) == {'wheels-ubuntu-latest-cp314t'}
    if experimental:
        for name, expected in candidate.optional.items():
            add_artifact(candidate, name, expected)
    record = seal(candidate)
    assert verify(candidate) == record
    assert record['source'] == gate.artifacts.checkout(candidate.repo)
    assert record['checks'] == CHECKS
    assert {Path(name).parts[0] for name in record['files']} == (
        set(REQUIRED_NAMES) | (set(candidate.optional) if experimental else set())
    )
    assert all(len(digest) == 64 for digest in record['files'].values())


@pytest.mark.parametrize('missing', REQUIRED_NAMES)
def test_no_required_platform_or_source_distribution_can_be_missing(candidate, missing):
    shutil.rmtree(candidate.directory / missing)
    with pytest.raises(gate.artifacts.ArtifactError, match='Missing required'):
        seal(candidate)
    assert not candidate.manifest.exists()


@pytest.mark.parametrize('name', CHECKS)
@pytest.mark.parametrize('result', ['failure', 'cancelled', 'skipped', None])
def test_every_full_gate_must_pass_before_a_candidate_can_be_sealed(candidate, name, result):
    checks = dict(CHECKS)
    if result is None:
        checks.pop(name)
    else:
        checks[name] = result
    with pytest.raises(gate.artifacts.ArtifactError, match='every full gate'):
        seal(candidate, checks)
    assert not candidate.manifest.exists()


@pytest.mark.parametrize('name', ['wheels-ubuntu-latest', 'mrsm-5.0.0-x86_64-unknown-linux-gnu', 'sdist'])
def test_development_profiles_cannot_masquerade_as_shipping_artifacts(candidate, name):
    path = candidate.directory / name / 'provenance.json'
    record = json.loads(path.read_text())
    record['build']['profile'] = 'ci'
    path.write_text(json.dumps(record))
    with pytest.raises(gate.artifacts.ArtifactError, match='profile'):
        seal(candidate)


def test_platform_labels_cannot_substitute_a_different_target(candidate):
    path = candidate.directory / 'wheels-ubuntu-latest/provenance.json'
    record = json.loads(path.read_text())
    record['build']['target'] = 'aarch64-unknown-linux-gnu'
    path.write_text(json.dumps(record))
    with pytest.raises(gate.artifacts.ArtifactError, match='target'):
        seal(candidate)


def test_payload_corruption_is_rejected_after_a_successful_gate(candidate):
    seal(candidate)
    wheel = next((candidate.directory / 'wheels-ubuntu-latest').glob('*.whl'))
    wheel.write_bytes(wheel.read_bytes() + b'changed')
    with pytest.raises(gate.artifacts.ArtifactError, match='checksum'):
        verify(candidate)


def test_reissuing_valid_provenance_does_not_change_the_sealed_bytes(candidate):
    seal(candidate)
    name = 'mrsm-5.0.0-x86_64-unknown-linux-gnu'
    directory = candidate.directory / name
    (directory / name).write_bytes(b'another-synthetic-native-program')
    gate.artifacts.create_manifest(directory, directory / 'provenance.json', kind='binary',
                                   profile='release', target='x86_64-unknown-linux-gnu',
                                   toolchain_file=candidate.compiler, repo=candidate.repo)
    gate.artifacts.verify_manifest(directory / 'provenance.json', profile='release', repo=candidate.repo)
    with pytest.raises(gate.artifacts.ArtifactError, match='differ from the fully tested'):
        verify(candidate)


@pytest.mark.parametrize('change_tree', [False, True])
def test_same_version_or_same_tree_at_another_commit_is_not_the_tested_source(candidate, change_tree):
    seal(candidate)
    if change_tree:
        (candidate.repo / 'new-source.txt').write_text('changed source at the same version\n')
        git(candidate.repo, 'add', 'new-source.txt')
    git(candidate.repo, 'commit', '--allow-empty', '-qm', 'Another actual commit')
    with pytest.raises(gate.artifacts.ArtifactError, match='different commit/tree'):
        verify(candidate)


@pytest.mark.parametrize('name', ['python', 'docs'])
def test_a_modified_candidate_cannot_drop_mandatory_backend_or_docs_checks(candidate, name):
    seal(candidate)
    record = json.loads(candidate.manifest.read_text())
    record['checks'].pop(name)
    candidate.manifest.write_text(json.dumps(record))
    with pytest.raises(gate.artifacts.ArtifactError, match='incomplete or failed checks'):
        verify(candidate)


def test_optional_wheel_cannot_be_added_after_the_candidate_was_tested(candidate):
    seal(candidate)
    for name, expected in candidate.optional.items():
        add_artifact(candidate, name, expected)
    with pytest.raises(gate.artifacts.ArtifactError, match='differ from the fully tested'):
        verify(candidate)


def test_raw_candidates_are_not_accepted_as_validated_release_artifacts(candidate):
    (candidate.directory / 'wheel-candidate-wheels-ubuntu-latest').mkdir()
    with pytest.raises(gate.artifacts.ArtifactError, match='Unexpected release artifacts'):
        seal(candidate)
