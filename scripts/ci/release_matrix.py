#!/usr/bin/env python3
"""Release artifact matrices: Linux x86_64 prereleases, all supported platforms for finals.

Uses only the standard library; neither Meerschaum nor its extension must be installed.
The source distribution and crates.io packages remain platform independent.
"""

import argparse
import json
from pathlib import Path
import re
import tomllib


FULL_MATRICES = {
    'wheels': [
        {'os': 'ubuntu-latest', 'manylinux': '2_28', 'target': 'x86_64-unknown-linux-gnu'},
        {'os': 'ubuntu-24.04-arm', 'manylinux': '2_28', 'target': 'aarch64-unknown-linux-gnu'},
        *[{
            'os': 'ubuntu-latest',
            'manylinux': 'musllinux_1_2',
            'target': f'{arch}-unknown-linux-musl',
            'musl': True,
            'wheel_arch': arch,
            'artifact': f'wheels-musllinux-{arch}',
            'smoke_os': 'ubuntu-latest' if arch == 'x86_64' else 'ubuntu-24.04-arm',
        } for arch in ('x86_64', 'aarch64')],
        {'os': 'macos-latest', 'manylinux': 'off', 'target': 'aarch64-apple-darwin'},
        {'os': 'macos-15-intel', 'manylinux': 'off', 'target': 'x86_64-apple-darwin'},
        {'os': 'windows-latest', 'manylinux': 'off', 'target': 'x86_64-pc-windows-msvc'},
    ],
    'experimental_wheels': [
        {'os': 'ubuntu-latest', 'manylinux': '2_28', 'target': 'x86_64-unknown-linux-gnu',
         'freethreaded': True, 'artifact': 'wheels-ubuntu-latest-cp314t'},
    ],
    'binaries': [
        {'os': 'ubuntu-latest', 'target': 'x86_64-unknown-linux-gnu', 'container_arch': 'x86_64'},
        {'os': 'ubuntu-24.04-arm', 'target': 'aarch64-unknown-linux-gnu', 'container_arch': 'aarch64'},
        {'os': 'macos-latest', 'target': 'aarch64-apple-darwin'},
        {'os': 'macos-15-intel', 'target': 'x86_64-apple-darwin'},
        {'os': 'windows-latest', 'target': 'x86_64-pc-windows-msvc', 'ext': '.exe'},
    ],
    'smoke': [
        {'os': os_name, 'python': python}
        for os_name in (
            'ubuntu-latest', 'ubuntu-24.04-arm', 'macos-latest', 'macos-15-intel', 'windows-latest',
        )
        for python in ('3.11', '3.14')
    ],
    'smoke_musl': [
        {'os': 'ubuntu-latest', 'arch': 'x86_64'},
        {'os': 'ubuntu-24.04-arm', 'arch': 'aarch64'},
    ],
    'docker': [
        {'arch': arch, 'platform': f'linux/{arch}'} for arch in ('amd64', 'arm64')
    ],
}


def _is_linux_x86_64_wheel(entry):
    return (
        entry.get('os') == 'ubuntu-latest'
        and entry.get('target') == 'x86_64-unknown-linux-gnu'
        and not entry.get('musl')
        and not entry.get('freethreaded')
    )


def _is_linux_x86_64_binary(entry):
    return (
        entry.get('os') == 'ubuntu-latest'
        and entry.get('target') == 'x86_64-unknown-linux-gnu'
    )


def release_matrices(version, *, scope='all'):
    """Return GitHub Actions outputs for a Cargo (SemVer) version.

    ``scope`` splits the build matrix when callers want Linux x86_64 release artifacts
    (which unblock ``pytest.yaml``) to run independently from the remaining platforms:
    ``'all'`` (default), ``'linux_x86_64'``, or ``'rest'``.
    """
    if not re.fullmatch(r'\d+\.\d+\.\d+(?:-[0-9A-Za-z.-]+)?(?:\+[0-9A-Za-z.-]+)?', version):
        raise ValueError(f'Invalid Cargo version: {version!r}')
    if scope not in ('all', 'linux_x86_64', 'rest'):
        raise ValueError(f'Invalid release matrix scope: {scope!r}')
    prerelease = '-' in version.split('+', 1)[0]
    selected = {}
    for name, entries in FULL_MATRICES.items():
        if prerelease:
            entries = [entry for entry in entries if (
                entry.get('os', 'ubuntu-latest') == 'ubuntu-latest'
                and entry.get('wheel_arch', 'x86_64') == 'x86_64'
                and entry.get('arch', 'amd64') in ('amd64', 'x86_64')
                and entry.get('target', '').split('-')[0] in ('', 'x86_64')
            )]
        if scope != 'all':
            want_linux = scope == 'linux_x86_64'
            if name == 'wheels':
                entries = [entry for entry in entries if _is_linux_x86_64_wheel(entry) is want_linux]
            elif name == 'binaries':
                entries = [entry for entry in entries if _is_linux_x86_64_binary(entry) is want_linux]
            elif name == 'experimental_wheels':
                entries = [] if want_linux else entries
            elif name == 'smoke':
                entries = [entry for entry in entries if (entry.get('os') == 'ubuntu-latest') is want_linux]
            elif name == 'smoke_musl':
                entries = [] if want_linux else entries
        selected[name] = {'include': [dict(entry) for entry in entries]}
    selected['prerelease'] = prerelease
    selected['has_wheels'] = bool(selected['wheels']['include'])
    selected['has_binaries'] = bool(selected['binaries']['include'])
    selected['has_experimental_wheels'] = bool(selected['experimental_wheels']['include'])
    selected['docker_platforms'] = ','.join(
        entry['platform'] for entry in selected['docker']['include']
    )
    return selected


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--version', help='Cargo version (defaults to Cargo.toml)')
    parser.add_argument(
        '--scope',
        choices=('all', 'linux_x86_64', 'rest'),
        default='all',
        help='Subset of build platforms to emit (default: all)',
    )
    parser.add_argument('--github-output', type=Path, help='Append outputs to this Actions file')
    args = parser.parse_args()
    version = args.version
    if version is None:
        manifest = Path(__file__).resolve().parents[2] / 'Cargo.toml'
        with manifest.open('rb') as stream:
            version = tomllib.load(stream)['workspace']['package']['version']
    outputs = release_matrices(version, scope=args.scope)
    if args.github_output:
        with args.github_output.open('a') as stream:
            for key, value in outputs.items():
                # GitHub Actions rejects an empty `strategy.matrix` even when the job is skipped
                # via `if:`; keep a placeholder entry while `has_<key>=false` skips the job.
                if (
                    key in ('wheels', 'binaries', 'experimental_wheels')
                    and isinstance(value, dict)
                    and not value.get('include')
                ):
                    value = {'include': [dict(FULL_MATRICES[key][0])]}
                encoded = value if isinstance(value, str) else json.dumps(value, separators=(',', ':'))
                stream.write(f'{key}={encoded}\n')
    print(json.dumps(outputs, indent=2))


if __name__ == '__main__':
    main()
