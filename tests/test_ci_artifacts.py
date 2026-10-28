"""Reject stale, incomplete or tampered CI artifacts and preserve bundled wheel libraries."""

import importlib.util
import json
import os
import subprocess
from pathlib import Path
import zipfile

import pytest

REPO = Path(__file__).resolve().parents[1]
spec = importlib.util.spec_from_file_location('ci_artifacts', REPO / 'scripts/ci/artifacts.py')
artifacts = importlib.util.module_from_spec(spec)
spec.loader.exec_module(artifacts)


def git(repo, *args):
    return subprocess.check_output(['git', *args], cwd=repo, text=True).strip()


@pytest.fixture
def producer(tmp_path):
    repo = tmp_path / 'repo'
    repo.mkdir()
    (repo / 'Cargo.toml').write_text('[workspace.package]\nversion = "5.0.0-rc.15"\n')
    (repo / 'Cargo.lock').write_text('version = 4\n')
    (repo / 'rust-toolchain.toml').write_text('[toolchain]\nchannel = "1.95"\n')
    (repo / 'pyproject.toml').write_text('[tool.maturin]\nfeatures = ["oracle", "extension-module"]\n')
    (repo / 'meerschaum').mkdir()
    (repo / 'meerschaum/__init__.py').write_text('VERSION = "5.0.0rc15"\n')
    git(repo, 'init', '-q')
    git(repo, 'config', 'user.email', 'ci-fixture@example.invalid')
    git(repo, 'config', 'user.name', 'CI fixture')
    git(repo, 'add', '.')
    git(repo, 'commit', '-qm', 'Fixture checkout')
    compiler = tmp_path / 'compiler.txt'
    compiler.write_text('rustc 1.95.0 (fixture)\nhost: x86_64-unknown-linux-gnu\n')
    payload = tmp_path / 'artifact'
    payload.mkdir()
    return repo, compiler, payload


def binary_manifest(producer, profile='release'):
    repo, compiler, payload = producer
    program = payload / 'mrsm'
    program.write_bytes(b'\x7fELFfixture-binary')
    manifest = payload / 'provenance.json'
    artifacts.create_manifest(payload, manifest, kind='binary', profile=profile,
                              target='x86_64-unknown-linux-gnu', toolchain_file=compiler, repo=repo)
    return program, manifest


def test_downloaded_binary_provenance_ignores_transport_mtime(producer):
    program, manifest = binary_manifest(producer)
    os.utime(program, (1, 1))
    record = artifacts.verify_manifest(manifest, kind='binary', profile='release',
                                       artifact=program, repo=producer[0])
    assert record['source']['commit'] == git(producer[0], 'rev-parse', 'HEAD')
    assert record['build']['rustc'] == producer[1].read_text().strip()


@pytest.mark.parametrize('change', ['bytes', 'missing', 'wrong-profile', 'wrong-target', 'other-program'])
def test_an_artifact_cannot_silently_change_or_substitute_its_payload(producer, change):
    program, manifest = binary_manifest(producer)
    kwargs = {'kind': 'binary', 'profile': 'release', 'artifact': program, 'repo': producer[0]}
    if change == 'bytes':
        program.write_bytes(b'\x7fELFdifferent-binary')
    elif change == 'missing':
        program.unlink()
    elif change == 'wrong-profile':
        kwargs['profile'] = 'ci'
    elif change == 'wrong-target':
        kwargs['target'] = 'aarch64-unknown-linux-gnu'
    else:
        other = program.with_name('other-mrsm')
        other.write_bytes(program.read_bytes())
        kwargs['artifact'] = other
    with pytest.raises(artifacts.ArtifactError):
        artifacts.verify_manifest(manifest, **kwargs)


def test_same_version_different_checkout_cannot_reuse_an_artifact(producer):
    _, manifest = binary_manifest(producer)
    repo = producer[0]
    (repo / 'meerschaum/__init__.py').write_text('VERSION = "5.0.0rc15"\nFIX = True\n')
    git(repo, 'add', '.')
    git(repo, 'commit', '-qm', 'Another tree at the same version')
    with pytest.raises(artifacts.ArtifactError, match='commit/tree'):
        artifacts.verify_manifest(manifest, profile='release', repo=repo)


def test_synthetic_merge_identity_is_not_replaced_by_branch_head(producer):
    _, manifest = binary_manifest(producer)
    repo = producer[0]
    # An empty commit even has the same tree. The exact tested revision remains explicit.
    git(repo, 'commit', '--allow-empty', '-qm', 'Synthetic merge identity')
    with pytest.raises(artifacts.ArtifactError, match='commit/tree'):
        artifacts.verify_manifest(manifest, profile='release', repo=repo)


def test_dirty_sources_are_never_described_as_the_committed_build(producer):
    _, manifest = binary_manifest(producer)
    (producer[0] / 'Cargo.lock').write_text('version = 4\n# changed\n')
    with pytest.raises(artifacts.ArtifactError, match='Tracked checkout'):
        artifacts.verify_manifest(manifest, repo=producer[0])


@pytest.mark.parametrize('unsafe', ['../outside', '/tmp/outside', 'nested\\outside'])
def test_provenance_paths_cannot_escape_the_artifact(producer, unsafe):
    _, manifest = binary_manifest(producer)
    record = json.loads(manifest.read_text())
    record['files'] = {unsafe: next(iter(record['files'].values()))}
    manifest.write_text(json.dumps(record))
    with pytest.raises(artifacts.ArtifactError, match='Unsafe artifact path'):
        artifacts.verify_manifest(manifest, repo=producer[0])


def test_artifact_symlinks_cannot_substitute_an_external_file(producer):
    program, manifest = binary_manifest(producer)
    external = producer[0].parent / 'external'
    external.write_bytes(program.read_bytes())
    program.unlink()
    program.symlink_to(external)
    with pytest.raises(artifacts.ArtifactError, match='unsafe artifact'):
        artifacts.verify_manifest(manifest, repo=producer[0])


@pytest.mark.parametrize('symlink', [False, True])
def test_extra_unverified_files_or_directories_are_rejected(producer, symlink):
    _, manifest = binary_manifest(producer)
    if symlink:
        (manifest.parent / 'external-directory').symlink_to(producer[0], target_is_directory=True)
    else:
        (manifest.parent / 'unverified.whl').write_bytes(b'not-the-tested-wheel')
    with pytest.raises(artifacts.ArtifactError, match='unverified|symlink'):
        artifacts.verify_manifest(manifest, repo=producer[0])


def wheel_manifest(producer, *, changed_source=False, extra=None):
    repo, compiler, payload = producer
    wheel = payload / 'meerschaum-5.0.0rc15-cp311-abi3-manylinux_2_28_x86_64.whl'
    with zipfile.ZipFile(wheel, 'w') as archive:
        archive.writestr('meerschaum-5.0.0rc15.dist-info/METADATA',
                         'Name: meerschaum\nVersion: 5.0.0rc15\n')
        archive.writestr('meerschaum-5.0.0rc15.dist-info/WHEEL',
                         'Wheel-Version: 1.0\nTag: cp311-abi3-manylinux_2_28_x86_64\n')
        archive.writestr('meerschaum/__init__.py',
                         'WRONG = True\n' if changed_source else (repo / 'meerschaum/__init__.py').read_text())
        archive.writestr('meerschaum/_core/_native.abi3.so', b'extension')
        archive.writestr('meerschaum.libs/libexample-123.so', b'bundled-library')
        if extra:
            archive.writestr(extra, b'unsafe')
    manifest = payload / 'provenance.json'
    artifacts.create_manifest(payload, manifest, kind='wheel', profile='release',
                              target='x86_64-unknown-linux-gnu', toolchain_file=compiler, repo=repo)
    return manifest


def test_wheel_overlay_preserves_complete_native_payload_and_tracked_sources(producer):
    manifest = wheel_manifest(producer)
    artifacts.overlay_wheel(manifest, profile='release', repo=producer[0])
    assert (producer[0] / 'meerschaum/_core/_native.abi3.so').read_bytes() == b'extension'
    assert (producer[0] / 'meerschaum.libs/libexample-123.so').read_bytes() == b'bundled-library'
    assert not git(producer[0], 'diff', '--name-only')


def test_wheel_overlay_refuses_different_python_sources(producer):
    manifest = wheel_manifest(producer, changed_source=True)
    original = (producer[0] / 'meerschaum/__init__.py').read_bytes()
    with pytest.raises(artifacts.ArtifactError, match='Tracked source differs'):
        artifacts.overlay_wheel(manifest, profile='release', repo=producer[0])
    assert (producer[0] / 'meerschaum/__init__.py').read_bytes() == original


def test_wheel_producer_refuses_a_missing_tracked_python_module(producer):
    repo = producer[0]
    (repo / 'meerschaum/new_feature.py').write_text('FEATURE = True\n')
    git(repo, 'add', '.')
    git(repo, 'commit', '-qm', 'A public module that must be distributed')
    with pytest.raises(artifacts.ArtifactError, match='omits tracked Python modules.*new_feature'):
        wheel_manifest(producer)
    assert not (producer[2] / 'provenance.json').exists()


def test_missing_module_cannot_be_masked_by_a_source_checkout_or_payload_checksum(producer):
    manifest = wheel_manifest(producer)
    wheel = next(manifest.parent.glob('*.whl'))
    with zipfile.ZipFile(wheel) as archive:
        members = {member.filename: archive.read(member) for member in archive.infolist()
                   if member.filename != 'meerschaum/__init__.py'}
    with zipfile.ZipFile(wheel, 'w') as archive:
        for name, data in members.items():
            archive.writestr(name, data)
    # Even a descriptor containing the new complete-payload checksum cannot turn
    # a packaging omission into a valid artifact of this checkout.
    record = json.loads(manifest.read_text())
    record['files'][wheel.name] = {'sha256': artifacts.sha256(wheel), 'bytes': wheel.stat().st_size}
    manifest.write_text(json.dumps(record))
    with pytest.raises(artifacts.ArtifactError, match='omits tracked Python modules'):
        artifacts.overlay_wheel(manifest, profile='release', repo=producer[0])
    assert not (producer[0] / 'meerschaum/_core/_native.abi3.so').exists()
    assert not (producer[0] / 'meerschaum.libs').exists()


def test_wheel_member_cannot_write_outside_checkout(producer):
    manifest = wheel_manifest(producer, extra='../escaped')
    with pytest.raises(artifacts.ArtifactError, match='Unsafe wheel member'):
        artifacts.overlay_wheel(manifest, profile='release', repo=producer[0])
    assert not (producer[0].parent / 'escaped').exists()


def test_ci_binary_uses_manifest_instead_of_file_age(producer, monkeypatch):
    from tests import _binary
    from scripts.ci import artifacts as imported_artifacts
    program, manifest = binary_manifest(producer)
    monkeypatch.setattr(_binary, 'REPO', producer[0])
    monkeypatch.setenv('CI', 'true')
    monkeypatch.setenv(_binary.MANIFEST_ENV, str(manifest))
    monkeypatch.setenv('MRSM_TEST_BIN_PROFILE', 'release')
    from meerschaum import _core
    monkeypatch.setattr(_binary, '_binary_version', lambda _: _core.__version__)
    monkeypatch.setattr(imported_artifacts, 'REPO', producer[0])
    os.utime(program, (1, 1))
    _binary.check_binary(str(program))
    program.write_bytes(b'changed')
    with pytest.raises(_binary.StaleBinaryError, match='checksum'):
        _binary.check_binary(str(program))


def test_ci_binary_cannot_fall_back_to_mtime_without_manifest(producer, monkeypatch):
    from tests import _binary
    from meerschaum import _core
    program, _ = binary_manifest(producer)
    monkeypatch.setenv('CI', 'true')
    monkeypatch.delenv(_binary.MANIFEST_ENV, raising=False)
    def forbidden_probe(_):
        pytest.fail('An unverified program must never be executed to probe its version.')
    monkeypatch.setattr(_binary, '_binary_version', forbidden_probe)
    with pytest.raises(_binary.StaleBinaryError, match='CI requires'):
        _binary.check_binary(str(program))
