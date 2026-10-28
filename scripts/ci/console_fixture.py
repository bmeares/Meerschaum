#!/usr/bin/env python3
"""Build and reuse the small console plugin against the exact CLI producer's SDK."""

from __future__ import annotations

import argparse
import json
import os
from pathlib import Path
import shutil
import subprocess
import sys
import tomllib

REPO = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(REPO))
from scripts.ci import artifacts

PLUGIN = 'w7_native_fig'
TARGET = 'x86_64-unknown-linux-gnu'
FILES = {'build-info.json', f'{PLUGIN}/plugin.toml', f'{PLUGIN}/lib{PLUGIN}.so'}


def select_sdk(messages: Path, binary: Path, profile: str, repo: Path = REPO) -> Path:
    """Accept only SDK artifacts emitted by the successful build which produced this CLI."""
    events = [json.loads(line) for line in messages.read_text().splitlines() if line.strip()]
    if not any(event.get('reason') == 'build-finished' and event.get('success') is True
               for event in events):
        raise artifacts.ArtifactError('The CLI Cargo build did not finish successfully.')
    sdk_source = (repo / 'crates/meerschaum-plugin/src/lib.rs').resolve()
    libraries = set()
    cli_seen = False
    for event in events:
        if event.get('reason') != 'compiler-artifact':
            continue
        target = event.get('target', {})
        if target.get('name') == 'mrsm' and 'bin' in target.get('kind', []):
            cli_seen |= Path(event.get('executable') or '').resolve() == binary.resolve()
        if (target.get('name') == 'meerschaum_plugin'
                and Path(target.get('src_path', '')).resolve() == sdk_source):
            libraries.update(Path(name).resolve() for name in event.get('filenames', [])
                             if name.endswith('.rlib'))
    if not cli_seen or len(libraries) != 1:
        raise artifacts.ArtifactError('Expected the matching CLI and one workspace SDK artifact.')
    sdk = libraries.pop()
    if (sdk.parent != (repo / 'target' / profile / 'deps').resolve()
            or not sdk.is_file() or sdk.is_symlink()
            or not binary.is_file() or binary.is_symlink()):
        raise artifacts.ArtifactError('Missing or incorrect-profile CLI/SDK artifact.')
    return sdk


def build(messages: Path, binary: Path, output: Path, profile: str, repo: Path = REPO) -> None:
    sdk = select_sdk(messages, binary, profile, repo)
    source = repo / 'tests/plugins' / PLUGIN
    destination = output / PLUGIN
    destination.mkdir(parents=True, exist_ok=False)
    shutil.copyfile(source / 'plugin.toml', destination / 'plugin.toml')
    metadata = tomllib.loads((source / 'plugin.toml').read_text())
    env = {key: value for key, value in os.environ.items() if key not in ('TMUX', 'TMUX_PANE')}
    env.update(CARGO_PKG_NAME=PLUGIN, CARGO_PKG_VERSION=metadata['version'])
    command = ['rustc', '--edition', '2024', '--crate-type', 'cdylib', '--crate-name', PLUGIN,
               '-L', f'dependency={sdk.parent}', '--extern', f'meerschaum_plugin={sdk}',
               '-C', 'debuginfo=0', '-C', 'strip=symbols',
               '-C', f'opt-level={3 if profile == "release" else 1}',
               '-o', str(destination / f'lib{PLUGIN}.so'), str(source / 'plugin.rs')]
    if profile == 'release':
        command += ['-C', 'lto=thin']
    subprocess.run(command, cwd=repo, env=env, check=True)
    (output / 'build-info.json').write_text(json.dumps({
        'plugin': PLUGIN, 'profile': profile, 'cli_sha256': artifacts.sha256(binary),
        'sdk_sha256': artifacts.sha256(sdk),
    }, indent=2) + '\n')


def record(output: Path, profile: str, toolchain: Path | None = None,
           repo: Path = REPO) -> None:
    artifacts.create_manifest(output, output / 'provenance.json', kind='binary',
                              profile=profile, target=TARGET, toolchain_file=toolchain,
                              features=['console-native-fixture'], repo=repo)


def verify(manifest: Path, binary_manifest: Path, profile: str, repo: Path = REPO) -> Path:
    fixture = artifacts.verify_manifest(manifest, kind='binary', profile=profile,
                                         target=TARGET, repo=repo)
    cli = artifacts.verify_manifest(binary_manifest, kind='binary', profile=profile,
                                     target=TARGET, repo=repo)
    if (set(fixture['files']) != FILES
            or fixture['build']['features'] != ['console-native-fixture']):
        raise artifacts.ArtifactError('Expected the complete console-native fixture payload.')
    programs = [digest for name, digest in cli['files'].items()
                if Path(name).name.startswith('mrsm') and not name.endswith('.sha256')]
    info = json.loads((manifest.parent / 'build-info.json').read_text())
    if (len(programs) != 1 or info.get('plugin') != PLUGIN or info.get('profile') != profile
            or info.get('cli_sha256') != programs[0]['sha256']
            or fixture['build']['rustc'] != cli['build']['rustc']):
        raise artifacts.ArtifactError('The console fixture was not built with this CLI.')
    return manifest.parent / PLUGIN


def install(manifest: Path, binary_manifest: Path, profile: str, destination: Path,
            repo: Path = REPO) -> None:
    source = verify(manifest, binary_manifest, profile, repo)
    target = destination / PLUGIN
    if target.is_symlink():
        raise artifacts.ArtifactError('Refusing a symlink fixture destination.')
    target.mkdir(parents=True, exist_ok=True)
    for name in ('plugin.toml', f'lib{PLUGIN}.so'):
        if (target / name).is_symlink():
            raise artifacts.ArtifactError('Refusing a symlink fixture file.')
        shutil.copyfile(source / name, target / name)


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    commands = parser.add_subparsers(dest='command', required=True)
    producer = commands.add_parser('build')
    producer.add_argument('--messages', type=Path, required=True)
    producer.add_argument('--binary', type=Path, required=True)
    producer.add_argument('--output', type=Path, required=True)
    recorder = commands.add_parser('record')
    recorder.add_argument('--output', type=Path, required=True)
    recorder.add_argument('--toolchain-file', type=Path)
    for name in ('verify', 'install'):
        consumer = commands.add_parser(name)
        consumer.add_argument('--manifest', type=Path, required=True)
        consumer.add_argument('--binary-manifest', type=Path, required=True)
        if name == 'install':
            consumer.add_argument('--destination', type=Path, required=True)
    for command in (producer, recorder, *[commands.choices[name] for name in ('verify', 'install')]):
        command.add_argument('--profile', choices=('debug', 'release'), required=True)
    args = parser.parse_args()
    try:
        if args.command == 'build':
            build(args.messages, args.binary, args.output, args.profile)
        elif args.command == 'record':
            record(args.output, args.profile, args.toolchain_file)
        elif args.command == 'verify':
            verify(args.manifest, args.binary_manifest, args.profile)
        else:
            install(args.manifest, args.binary_manifest, args.profile, args.destination)
    except (artifacts.ArtifactError, OSError, ValueError, subprocess.CalledProcessError) as exc:
        parser.exit(1, f'console fixture: {exc}\n')


if __name__ == '__main__':
    main()
