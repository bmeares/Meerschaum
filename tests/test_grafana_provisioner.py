"""The host upgrade installs only managed JSON, with preflight, backups and rollback."""

import json
from pathlib import Path
from types import SimpleNamespace

import pytest

from scripts import provision_grafana as provision


@pytest.fixture
def directories(tmp_path):
    destination = tmp_path / 'provisioning' / 'dashboards'
    destination.mkdir(parents=True)
    resources = {}
    old = {}
    for name, uid in provision.MANAGED.items():
        resources[name] = json.dumps({'uid': uid, 'title': 'new'}).encode()
        old[name] = json.dumps({'uid': uid, 'title': 'old'}).encode()
        (destination / name).write_bytes(old[name])
        (destination / name).chmod(0o640)
    (destination / 'user-dashboard.json').write_text('{"uid":"user"}')
    for name in ('compose.yaml', 'datasource.yaml', 'dashboard.yaml'):
        (tmp_path / 'provisioning' / name).write_text('keep ' + name)
    return destination, resources, old, tmp_path / 'backups'


def test_review_is_read_only_and_only_managed_files_are_replaced(directories):
    destination, resources, old, backups = directories
    entries = provision.plan(destination, resources)
    assert all(entry['changed'] for entry in entries)
    assert {name: (destination / name).read_bytes() for name in old} == old
    assert not backups.exists()
    backup = provision.install(destination, entries, backups)
    assert backup.parent == backups
    for name, data in resources.items():
        assert (destination / name).read_bytes() == data
        assert (destination / name).stat().st_mode & 0o777 == 0o640
        assert (backup / name).read_bytes() == old[name]
    assert (destination / 'user-dashboard.json').read_text() == '{"uid":"user"}'
    for name in ('compose.yaml', 'datasource.yaml', 'dashboard.yaml'):
        assert (destination.parent / name).read_text() == 'keep ' + name
    assert provision.install(destination, provision.plan(destination, resources), backups) is None


def test_any_uid_collision_blocks_the_entire_preflight(directories):
    destination, resources, old, backups = directories
    final = list(provision.MANAGED)[-1]
    (destination / final).write_text('{"uid":"custom"}')
    before = {path.name: path.read_bytes() for path in destination.iterdir()}
    with pytest.raises(ValueError, match='different dashboard UID'):
        provision.plan(destination, resources)
    assert before == {path.name: path.read_bytes() for path in destination.iterdir()}
    assert not backups.exists()


def test_failed_atomic_update_rolls_back_previous_managed_writes(directories, monkeypatch):
    destination, resources, old, backups = directories
    write = provision.atomic_write
    calls = 0

    def fail_second(*args):
        nonlocal calls
        calls += 1
        if calls == 2:
            raise OSError('simulated rename failure')
        return write(*args)

    monkeypatch.setattr(provision, 'atomic_write', fail_second)
    with pytest.raises(OSError, match='simulated'):
        provision.install(destination, provision.plan(destination, resources), backups)
    assert {name: (destination / name).read_bytes() for name in old} == old


def test_backups_and_symlinks_cannot_escape_the_reviewed_boundary(directories, tmp_path):
    destination, resources, old, backups = directories
    entries = provision.plan(destination, resources)
    with pytest.raises(ValueError, match='outside'):
        provision.install(destination, entries, destination / 'backup')
    name = next(iter(provision.MANAGED))
    (destination / name).unlink()
    outside = tmp_path / 'outside.json'
    outside.write_bytes(old[name])
    (destination / name).symlink_to(outside)
    with pytest.raises(ValueError, match='symlink'):
        provision.plan(destination, resources)
    assert outside.read_bytes() == old[name]


def test_native_emitter_gets_only_its_scratch_root_and_installed_import_path(tmp_path, monkeypatch):
    expected = {name: json.dumps({'uid': uid}) for name, uid in provision.MANAGED.items()}
    captured = {}

    def emit(command, **kwargs):
        captured.update(kwargs)
        assert command[0] == '/installed/python'
        return SimpleNamespace(returncode=0, stdout='RESOURCES=' + json.dumps(expected), stderr='')

    monkeypatch.setenv('MRSM_ROOT_DIR', '/do/not/open')
    monkeypatch.setenv('MRSM_SQL_PRIVATE', 'not inherited')
    monkeypatch.setenv('PYTHONPATH', '/source/tree')
    monkeypatch.setenv('PYTHONHOME', '/unrelated/python')
    monkeypatch.setenv('TMUX', '/user/tmux')
    monkeypatch.setattr(provision.subprocess, 'run', emit)
    resources = provision.installed_resources('/installed/python', '5.0.0rc16', tmp_path)
    assert set(resources) == set(provision.MANAGED)
    env = captured['env']
    assert Path(env['MRSM_ROOT_DIR']).parent == tmp_path
    assert not {'MRSM_SQL_PRIVATE', 'PYTHONPATH', 'PYTHONHOME', 'TMUX', 'TMUX_PANE'} & env.keys()
    assert not Path(captured['cwd']).exists()  # Only the emitter's temporary root was removed.


def test_distribution_provenance_refuses_matching_version_source_or_editable_paths(tmp_path):
    package = tmp_path / 'site-packages' / 'meerschaum'
    native = package / '_core' / '_native.abi3.so'
    native.parent.mkdir(parents=True)
    native.touch()
    module = package / '__init__.py'
    module.touch()
    versions = ['5.0.0rc16', '5.0.0rc16', '5.0.0-rc.16']
    provision.validate_artifact_provenance(module, native, package, versions, '5.0.0rc16')
    source = tmp_path / 'source' / '__init__.py'
    source.parent.mkdir()
    source.touch()
    with pytest.raises(ValueError, match='installed distribution'):
        provision.validate_artifact_provenance(source, native, package, versions, '5.0.0rc16')
    with pytest.raises(ValueError, match='installed distribution'):
        provision.validate_artifact_provenance(module, source, package, versions, '5.0.0rc16')
    with pytest.raises(ValueError, match='versions differ'):
        provision.validate_artifact_provenance(module, native, package, ['5.0.0rc15', *versions[1:]], '5.0.0rc16')
    with pytest.raises(ValueError, match='editable/source'):
        provision.validate_artifact_provenance(module, native, package, versions, '5.0.0rc16', editable=True)
    with pytest.raises(ValueError, match='installed wheel'):
        provision.validate_artifact_provenance(module, native, package, versions, '5.0.0rc16', installed_wheel=False)
