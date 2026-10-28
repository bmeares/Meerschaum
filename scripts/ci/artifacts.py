#!/usr/bin/env python3
"""Record and verify the checkout and complete payload of a reusable CI artifact.

An artifact belongs to the actual checked-out commit, including a PR's synthetic merge
commit. Matching a version or a file modification time is insufficient. This manifest
is provenance within a trusted Actions run, not an attestation from an external signer.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import platform
import re
import runpy
import shutil
import subprocess
import sys
import tomllib
import zipfile
from email.parser import BytesParser
from pathlib import Path, PurePosixPath
from typing import Any

REPO = Path(__file__).resolve().parents[2]
SCHEMA = 1


class ArtifactError(ValueError):
    """An artifact is incomplete or does not belong to this checkout/configuration."""


def sha256(path: Path) -> str:
    with path.open('rb') as stream:
        return hashlib.file_digest(stream, 'sha256').hexdigest()


def _git(repo: Path, *args: str) -> str:
    return subprocess.check_output(['git', *args], cwd=repo, text=True).strip()


def checkout(repo: Path = REPO, *, clean: bool = True) -> dict[str, str]:
    if clean and _git(repo, 'status', '--porcelain', '--untracked-files=no'):
        raise ArtifactError('Tracked checkout files differ from the commit being tested.')
    return {'commit': _git(repo, 'rev-parse', 'HEAD'), 'tree': _git(repo, 'rev-parse', 'HEAD^{tree}')}


def _relative_file(base: Path, name: str) -> Path:
    relative = PurePosixPath(name)
    if not name or relative.is_absolute() or '..' in relative.parts or '\\' in name:
        raise ArtifactError(f'Unsafe artifact path: {name!r}')
    path = base.joinpath(*relative.parts)
    if path.is_symlink() or not path.is_file() or not path.resolve().is_relative_to(base.resolve()):
        raise ArtifactError(f'Missing or unsafe artifact file: {name!r}')
    return path


def _wheel_info(path: Path, cargo_version: str, *, repo: Path = REPO) -> dict[str, Any]:
    convert = runpy.run_path(str(REPO / 'scripts/check_packaging.py'))['cargo_to_pep440']
    with zipfile.ZipFile(path) as archive:
        # Source-reading backend tests must not mask modules omitted from the installed
        # distribution. Check the whole tracked Python package, including the CLI shim,
        # before either producing provenance or accepting a downloaded wheel.
        expected = {
            name for name in _git(repo, 'ls-files', '-z', '--',
                                  'meerschaum', '_meerschaum_mrsm.py').split('\0')
            if name.endswith('.py')
        }
        if missing := expected - set(archive.namelist()):
            raise ArtifactError(f'Wheel omits tracked Python modules: {sorted(missing)}')
        metadata_names = [name for name in archive.namelist() if name.endswith('.dist-info/METADATA')]
        if len(metadata_names) != 1:
            raise ArtifactError('Expected exactly one wheel METADATA file.')
        metadata = BytesParser().parsebytes(archive.read(metadata_names[0]))
        if metadata['Name'] != 'meerschaum' or metadata['Version'] != convert(cargo_version):
            raise ArtifactError('Wheel distribution/version differs from the Cargo checkout.')
        wheel_metadata = archive.read(metadata_names[0].removesuffix('METADATA') + 'WHEEL').decode()
        tags = [line.removeprefix('Tag: ') for line in wheel_metadata.splitlines() if line.startswith('Tag: ')]
        if not tags:
            raise ArtifactError('The wheel has no Python/ABI/platform tags.')
    return {'version': metadata['Version'], 'tags': tags}


def create_manifest(
    artifact_dir: Path,
    output: Path,
    *,
    kind: str,
    profile: str,
    target: str,
    toolchain_file: Path | None = None,
    features: list[str] | None = None,
    repo: Path = REPO,
) -> dict[str, Any]:
    source = checkout(repo)
    artifact_dir = artifact_dir.resolve()
    output = output.resolve()
    if kind not in ('wheel', 'binary', 'sdist') or profile not in ('debug', 'ci', 'release'):
        raise ArtifactError('Unsupported artifact kind/profile.')
    if not target or not output.is_relative_to(artifact_dir):
        raise ArtifactError('Provide a target and a manifest inside the artifact directory.')
    cargo = tomllib.loads((repo / 'Cargo.toml').read_text())
    version = cargo['workspace']['package']['version']
    compiler = (
        toolchain_file.read_text().strip() if toolchain_file is not None
        else subprocess.check_output(['rustc', '-Vv'], cwd=repo, text=True).strip()
    )
    if not compiler.startswith('rustc '):
        raise ArtifactError('Record rustc -Vv from the actual build environment.')
    if features is None:
        project = tomllib.loads((repo / 'pyproject.toml').read_text())
        features = project.get('tool', {}).get('maturin', {}).get('features', []) if kind == 'wheel' else ['default']
    files = {}
    wheels = []
    for path in sorted(artifact_dir.rglob('*')):
        if path == output or not path.is_file():
            continue
        name = path.relative_to(artifact_dir).as_posix()
        path = _relative_file(artifact_dir, name)
        files[name] = {'sha256': sha256(path), 'bytes': path.stat().st_size}
        if path.suffix == '.whl':
            wheels.append(path)
    if not files:
        raise ArtifactError('Refusing to create provenance for an empty artifact.')
    if kind == 'wheel' and len(wheels) != 1:
        raise ArtifactError('A wheel artifact must contain exactly one complete wheel.')
    manifest = {
        'schema': SCHEMA,
        'source': source,
        'build': {
            'kind': kind, 'profile': profile, 'target': target, 'features': sorted(features),
            'rustc': compiler, 'python': platform.python_version(), 'cargo_version': version,
            'cargo_lock_sha256': sha256(repo / 'Cargo.lock'),
            'toolchain_sha256': sha256(repo / 'rust-toolchain.toml'),
        },
        'actions': {key: os.environ.get('GITHUB_' + key.upper(), '') for key in ('repository', 'run_id', 'run_attempt')},
        'files': files,
    }
    if wheels:
        manifest['wheel'] = _wheel_info(wheels[0], version, repo=repo)
    output.parent.mkdir(parents=True, exist_ok=True)
    output.write_text(json.dumps(manifest, indent=2, sort_keys=True) + '\n')
    return manifest


def verify_manifest(
    path: Path,
    *,
    profile: str | None = None,
    kind: str | None = None,
    target: str | None = None,
    artifact: Path | None = None,
    repo: Path = REPO,
) -> dict[str, Any]:
    try:
        if path.is_symlink():
            raise ArtifactError('The provenance manifest may not be a symlink.')
        manifest = json.loads(path.read_text())
        if manifest['schema'] != SCHEMA:
            raise ArtifactError('Unknown artifact provenance schema.')
        if manifest['source'] != checkout(repo):
            raise ArtifactError('Artifact commit/tree differs from the actual checkout (including PR merges).')
        build = manifest['build']
        for field, expected in [('profile', profile), ('kind', kind), ('target', target)]:
            if expected is not None and build[field] != expected:
                raise ArtifactError(f'Artifact {field} is {build[field]!r}; expected {expected!r}.')
        if build['cargo_lock_sha256'] != sha256(repo / 'Cargo.lock'):
            raise ArtifactError('Artifact dependency lock differs from this checkout.')
        if build['toolchain_sha256'] != sha256(repo / 'rust-toolchain.toml'):
            raise ArtifactError('Artifact toolchain input differs from this checkout.')
        if not manifest['files'] or not isinstance(manifest['files'], dict):
            raise ArtifactError('Artifact manifest has no payload.')
        verified = set()
        for name, digest in manifest['files'].items():
            payload = _relative_file(path.parent, name)
            if payload.stat().st_size != digest['bytes'] or sha256(payload) != digest['sha256']:
                raise ArtifactError(f'Artifact checksum differs: {name}')
            verified.add(payload.resolve())
        actual = set()
        for candidate in path.parent.rglob('*'):
            if candidate.is_symlink():
                raise ArtifactError(f'Unexpected artifact symlink: {candidate.name}')
            if candidate.is_file() and candidate.resolve() != path.resolve():
                actual.add(candidate.resolve())
        if actual != verified:
            raise ArtifactError('Artifact directory contains unverified files.')
        if artifact is not None and artifact.resolve() not in verified:
            raise ArtifactError('Requested program is not a verified payload of this artifact.')
        if build['kind'] == 'wheel':
            wheels = [p for p in verified if p.suffix == '.whl']
            if len(wheels) != 1 or _wheel_info(wheels[0], build['cargo_version'], repo=repo) != manifest['wheel']:
                raise ArtifactError('Wheel metadata differs from its manifest.')
        return manifest
    except (KeyError, TypeError, OSError, json.JSONDecodeError, zipfile.BadZipFile) as exc:
        raise ArtifactError(f'Invalid or incomplete artifact manifest {path}: {exc}') from exc


def overlay_wheel(manifest_path: Path, *, profile: str, repo: Path = REPO) -> None:
    """Expose a verified wheel's full native payload to source-reading tests.

    The consumer first installs the complete wheel into its venv. Tests also import the
    checked-out package, so preserve bundled libraries and their relative paths here.
    Existing tracked Python files must already equal the wheel; they are never replaced.
    """
    manifest = verify_manifest(manifest_path, profile=profile, kind='wheel', repo=repo)
    wheel = next(_relative_file(manifest_path.parent, n) for n in manifest['files'] if n.endswith('.whl'))
    tracked = set(_git(repo, 'ls-files').splitlines())
    with zipfile.ZipFile(wheel) as archive:
        for member in archive.infolist():
            name = member.filename
            if member.is_dir() or '.dist-info/' in name or '.data/' in name:
                continue
            relative = PurePosixPath(name)
            if relative.is_absolute() or '..' in relative.parts or '\\' in name:
                raise ArtifactError(f'Unsafe wheel member: {name!r}')
            destination = repo.joinpath(*relative.parts)
            if not destination.resolve().is_relative_to(repo.resolve()) or destination.is_symlink():
                raise ArtifactError(f'Unsafe wheel destination: {name!r}')
            if name in tracked:
                if destination.read_bytes() != archive.read(member):
                    raise ArtifactError(f'Tracked source differs from the installed wheel: {name}')
                continue
            destination.parent.mkdir(parents=True, exist_ok=True)
            with archive.open(member) as source, destination.open('wb') as target_stream:
                shutil.copyfileobj(source, target_stream)


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    commands = parser.add_subparsers(dest='command', required=True)
    create = commands.add_parser('create')
    create.add_argument('--artifact-dir', type=Path, required=True)
    create.add_argument('--output', type=Path, required=True)
    create.add_argument('--kind', choices=('wheel', 'binary', 'sdist'), required=True)
    create.add_argument('--profile', choices=('debug', 'ci', 'release'), required=True)
    create.add_argument('--target', required=True)
    create.add_argument('--toolchain-file', type=Path)
    create.add_argument('--features', help='Comma-separated, complete Cargo feature set')
    for name in ('verify', 'overlay-wheel'):
        command = commands.add_parser(name)
        command.add_argument('--manifest', type=Path, required=True)
        command.add_argument('--profile', choices=('debug', 'ci', 'release'), required=True)
        if name == 'verify':
            command.add_argument('--kind', choices=('wheel', 'binary', 'sdist'))
            command.add_argument('--target')
    args = parser.parse_args()
    try:
        if args.command == 'create':
            create_manifest(args.artifact_dir, args.output, kind=args.kind, profile=args.profile,
                            target=args.target, toolchain_file=args.toolchain_file,
                            features=args.features.split(',') if args.features is not None else None)
        elif args.command == 'overlay-wheel':
            overlay_wheel(args.manifest, profile=args.profile)
        else:
            verify_manifest(args.manifest, profile=args.profile, kind=args.kind, target=args.target)
    except (ArtifactError, subprocess.CalledProcessError) as exc:
        parser.exit(1, f'artifacts: {exc}\n')
    print(f'Artifact {args.command} verified for the actual checkout.')
    return 0


if __name__ == '__main__':
    sys.exit(main())
