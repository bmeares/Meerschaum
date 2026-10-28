#!/usr/bin/env python3
"""Review or install only the built-in Grafana JSON from an installed Meerschaum artifact.

Compose, provider and datasource configuration on the destination host are never regenerated.
The default is a read-only plan; --install backs up and atomically replaces the managed JSON.
"""

import argparse
from datetime import datetime, timezone
import hashlib
import inspect
import json
import os
from pathlib import Path
import subprocess
import sys
import tempfile
import uuid

MANAGED = {
    'meerschaum-performance.json': 'mrsm-performance',
    'meerschaum-pipes.json': 'mrsm-pipe-performance',
    'meerschaum-pipelines.json': 'mrsm-pipeline-performance',
}


def validate_artifact_provenance(package_file, extension_file, distribution_root, versions, expected,
                                 editable=False, installed_wheel=True):
    """Reject a matching-version source tree or editable extension shadowing an install."""
    root = Path(distribution_root).resolve()
    if editable or not installed_wheel:
        raise ValueError('An installed wheel is required; editable/source distributions are refused')
    if any(str(version).replace('-rc.', 'rc') != expected.replace('-rc.', 'rc') for version in versions):
        raise ValueError('Installed distribution/package/native versions differ from expected artifact')
    for label, filename in (('package', package_file), ('native extension', extension_file)):
        if not filename:
            raise ValueError(f'Missing installed {label} path')
        path = Path(filename).resolve()
        if not path.is_file() or not path.is_relative_to(root):
            raise ValueError(f'The {label} must be inside the installed distribution: {path}')


def installed_resources(python, expected_version, scratch_parent):
    """Emit resources in a child with no live configuration or source-tree import path."""
    scratch_parent.mkdir(parents=True, exist_ok=True)
    with tempfile.TemporaryDirectory(prefix='grafana-artifact-', dir=scratch_parent) as name:
        root = Path(name)
        (root / 'plugins').mkdir()
        env = {
            key: value for key, value in os.environ.items()
            if not key.startswith('MRSM_') and key not in ('PYTHONPATH', 'PYTHONHOME', 'TMUX', 'TMUX_PANE')
        }
        env.update(MRSM_ROOT_DIR=str(root), MRSM_PLUGINS_DIR=str(root / 'plugins'),
                   MRSM_NO_AUTO_INSTALL='1', MRSM_NO_UPDATE_CHECK='1')
        code = '''
import json, sys
from importlib import metadata
from pathlib import Path
''' + inspect.getsource(validate_artifact_provenance) + '''
import meerschaum as mrsm
from meerschaum import _core
distribution = metadata.distribution('meerschaum')
origin = json.loads(distribution.read_text('direct_url.json') or '{}')
validate_artifact_provenance(mrsm.__file__, _core.EXTENSION_PATH,
    distribution.locate_file('meerschaum'),
    [metadata.version('meerschaum'), mrsm.__version__, _core.__version__], sys.argv[1],
    editable=bool(origin.get('dir_info', {}).get('editable')),
    installed_wheel=distribution.read_text('WHEEL') is not None)
_core.sync_files(['stack'])
from meerschaum.config import paths
directory = paths.GRAFANA_DASHBOARD_PATH.parent
names = json.loads(sys.argv[2])
print('RESOURCES=' + json.dumps({name: (directory / name).read_text() for name in names}))
'''
        result = subprocess.run([python, '-c', code, expected_version, json.dumps(list(MANAGED))],
                                cwd=root, env=env, capture_output=True, text=True, timeout=90)
        if result.returncode:
            raise RuntimeError('Installed artifact resource emission failed:\n' + result.stderr[-3000:])
        line = next((line[10:] for line in result.stdout.splitlines() if line.startswith('RESOURCES=')), None)
        if line is None:
            raise RuntimeError('Installed artifact did not emit dashboard resources')
        resources = json.loads(line)
        return {name: text.encode('utf-8') for name, text in resources.items()}


def plan(destination, resources):
    """Preflight every UID before any destination or backup mutation."""
    if not destination.is_dir():
        raise ValueError(f'The existing mounted dashboard directory is required: {destination}')
    if set(resources) != set(MANAGED):
        raise ValueError('The installed artifact must supply exactly the three managed dashboards')
    result = []
    for name, uid in MANAGED.items():
        data = resources[name]
        if json.loads(data).get('uid') != uid:
            raise ValueError(f'Unexpected built-in dashboard UID: {name}')
        target = destination / name
        if target.is_symlink():
            raise ValueError(f'Refusing a managed filename which is a symlink: {target}')
        previous = target.read_bytes() if target.exists() else None
        if previous is not None:
            try:
                old_uid = json.loads(previous).get('uid')
            except (ValueError, AttributeError) as error:
                raise ValueError(f'Refusing an unreadable/custom dashboard collision: {target}') from error
            if old_uid != uid:
                raise ValueError(f'Keeping a different dashboard UID at managed filename: {target}')
            if target.stat().st_uid != os.getuid():
                raise ValueError(f'Refusing to change dashboard ownership: {target}')
        result.append({'name': name, 'uid': uid, 'changed': previous != data,
                       'before': previous, 'after': data,
                       'mode': target.stat().st_mode & 0o777 if previous is not None else 0o644})
    return result


def atomic_write(path, data, mode):
    fd, temporary = tempfile.mkstemp(prefix='.mrsm-dashboard-', suffix='.tmp', dir=path.parent)
    try:
        with os.fdopen(fd, 'wb') as stream:
            os.fchmod(stream.fileno(), mode)
            stream.write(data)
            stream.flush()
            os.fsync(stream.fileno())
        os.replace(temporary, path)
    finally:
        if os.path.exists(temporary):
            os.unlink(temporary)


def install(destination, entries, backup_parent):
    """Back up outside the watched folder; roll back all managed writes on failure."""
    destination, backup_parent = destination.resolve(), backup_parent.resolve()
    if backup_parent == destination or destination in backup_parent.parents:
        raise ValueError('Dashboard backups must be outside the provisioner-watched directory')
    changed = [entry for entry in entries if entry['changed']]
    if not changed:
        return None
    stamp = datetime.now(timezone.utc).strftime('%Y%m%dT%H%M%SZ')
    backup = backup_parent / (stamp + '-' + uuid.uuid4().hex[:12])
    backup.mkdir(parents=True, mode=0o700)
    for entry in changed:
        if entry['before'] is not None:
            (backup / entry['name']).write_bytes(entry['before'])
    manifest = [{key: entry[key] for key in ('name', 'uid', 'changed')} for entry in entries]
    (backup / 'manifest.json').write_text(json.dumps(manifest, indent=2) + '\n')
    written = []
    try:
        for entry in changed:
            # Recheck the reviewed bytes immediately before replacing them.
            target = destination / entry['name']
            if target.is_symlink() or (target.read_bytes() if target.exists() else None) != entry['before']:
                raise RuntimeError(f'Dashboard changed after preflight: {target}')
            atomic_write(target, entry['after'], entry['mode'])
            written.append(entry)
    except BaseException:
        for entry in reversed(written):
            target = destination / entry['name']
            if entry['before'] is None:
                target.unlink()
            else:
                atomic_write(target, entry['before'], entry['mode'])
        raise
    return backup


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--destination', required=True, type=Path)
    parser.add_argument('--python', default=sys.executable, help='Interpreter of the installed artifact')
    parser.add_argument('--expected-version', required=True)
    parser.add_argument('--scratch-parent', type=Path, default=Path.home() / '.cache' / 'mrsm-grafana-provision')
    parser.add_argument('--backup-dir', type=Path, help='Outside the dashboard directory; required with --install')
    parser.add_argument('--install', action='store_true')
    args = parser.parse_args()
    try:
        resources = installed_resources(args.python, args.expected_version, args.scratch_parent)
        destination = args.destination.expanduser().resolve()
        entries = plan(destination, resources)
        backup = None
        if args.install:
            if args.backup_dir is None:
                raise ValueError('--install requires an explicit --backup-dir outside the watched folder')
            backup = install(destination, entries, args.backup_dir.expanduser())
        print(json.dumps({'installed': args.install, 'destination': str(destination),
                          'backup': str(backup) if backup else None,
                          'dashboards': [{'name': entry['name'], 'uid': entry['uid'],
                                          'changed': entry['changed'],
                                          'sha256': hashlib.sha256(entry['after']).hexdigest()}
                                         for entry in entries]}, indent=2))
        return 0
    except (ValueError, RuntimeError, OSError, subprocess.SubprocessError) as error:
        print(f'provision_grafana.py: {error}', file=sys.stderr)
        return 2


if __name__ == '__main__':
    raise SystemExit(main())
