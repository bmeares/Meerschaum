"""
`scripts/docker/buildx.sh` plans: the commands it would run for a local build, for a
per-architecture push from a self-hosted runner, and for the merge into the multi-architecture tags.

`BUILDX_DRY_RUN=1` makes the script print its docker commands instead of running them (no Docker,
no network, nothing pulled or pushed); `PYTHON_BIN` points at a stub that prints the version, so a
final release (which moves `latest`, `full`, `slim`, `api`) is tested as well as a prerelease.
"""

import importlib.util
import io
import os
import re
import stat
import subprocess
import tarfile
from pathlib import Path, PurePosixPath

import pytest

REPO = Path(__file__).resolve().parents[1]
SCRIPT = REPO / 'scripts' / 'docker' / 'buildx.sh'
IMAGE = 'bmeares/meerschaum'


def _buildx(tmp_path, version, *args, env=None):
    python = tmp_path / 'python'
    python.write_text(f'#!/bin/sh\necho {version}\n')
    python.chmod(python.stat().st_mode | stat.S_IEXEC)
    extra = env or {}
    env = {k: v for k, v in os.environ.items() if not k.startswith(('MRSM_', 'GITHUB_'))}
    env.update({'BUILDX_DRY_RUN': '1', 'PYTHON_BIN': str(python), **extra})
    result = subprocess.run(
        ['bash', str(SCRIPT), *args], env=env, capture_output=True, text=True, timeout=60,
    )
    commands = [
        line[2:] for line in result.stdout.splitlines() if line.startswith('+ docker ')
    ]
    return result, commands


def _tags(command):
    return re.findall(r'(?:^| )-t (\S+)', command)


def _platform(command):
    match = re.search(r'--platform (\S+)', command)
    return match.group(1) if match else None


def test_a_local_build_loads_for_the_builders_platform(tmp_path):
    result, commands = _buildx(tmp_path, '5.0.0rc2')
    assert result.returncode == 0, result.stderr
    assert len(commands) == 4
    for command in commands:
        assert ' --load ' in command and '--push' not in command
        assert _platform(command) is None, 'a loaded image cannot be a manifest list'
    assert [_tags(c) for c in commands] == [
        [f'{IMAGE}:5.0.0rc2-minimal', f'{IMAGE}:5.0.0rc2-slim'],
        [f'{IMAGE}:5.0.0rc2-full', f'{IMAGE}:5.0.0rc2-api'],
        [f'{IMAGE}:5.0.0rc2-rust'],
        [f'{IMAGE}:5.0.0rc2-rust-full'],
    ]


@pytest.mark.parametrize('version,platforms', [
    ('5.0.0rc4', 'linux/amd64'),
    ('5.0.0a1', 'linux/amd64'),
    ('5.0.0b1', 'linux/amd64'),
    ('5.0.0.dev1', 'linux/amd64'),
    ('5.0.0', 'linux/amd64,linux/arm64'),
])
def test_a_push_selects_the_release_platforms(tmp_path, version, platforms):
    result, commands = _buildx(tmp_path, version, '--push')
    assert result.returncode == 0, result.stderr
    assert {_platform(c) for c in commands} == {platforms}


@pytest.mark.parametrize('platform,arch', [('linux/arm64', 'arm64'), ('linux/amd64', 'amd64')])
def test_a_per_architecture_push_pushes_only_the_architecture_tags(tmp_path, platform, arch):
    for version in ('5.0.0rc2', '5.0.0'):
        result, commands = _buildx(
            tmp_path, version, '--push', '--platform', platform, '--arch-tags',
        )
        assert result.returncode == 0, result.stderr
        assert [_tags(c) for c in commands] == [
            [f'{IMAGE}:{version}-minimal-{arch}', f'{IMAGE}:{version}-slim-{arch}'],
            [f'{IMAGE}:{version}-full-{arch}'],
            [f'{IMAGE}:{version}-rust-{arch}'],
            [f'{IMAGE}:{version}-rust-full-{arch}'],
        ], 'a release must not move latest/full/slim/api from one architecture'
        assert {_platform(c) for c in commands} == {platform}
        assert all(' --push ' in c for c in commands)


def test_the_merge_of_a_prerelease_makes_only_version_tags(tmp_path):
    result, commands = _buildx(tmp_path, '5.0.0rc2', '--merge')
    assert result.returncode == 0, result.stderr
    assert commands == [
        f'docker buildx imagetools create -t {IMAGE}:5.0.0rc2-{name} '
        + (f'-t {IMAGE}:5.0.0rc2-api ' if name == 'full' else '')
        + f'{IMAGE}:5.0.0rc2-{name}-amd64'
        for name in ('minimal', 'full', 'slim', 'rust', 'rust-full')
    ]


def test_the_merge_of_a_release_moves_the_floating_tags(tmp_path):
    result, commands = _buildx(tmp_path, '5.0.0', '--merge')
    assert result.returncode == 0, result.stderr
    by_name = {c.split('-t ')[1].split(' ')[0].split(':5.0.0-', 1)[1]: c for c in commands}
    assert set(by_name) == {'minimal', 'full', 'slim', 'rust', 'rust-full'}
    assert _tags(by_name['full']) == [
        f'{IMAGE}:5.0.0-full', f'{IMAGE}:5.0.0-api', f'{IMAGE}:full', f'{IMAGE}:latest',
        f'{IMAGE}:5.0.0', f'{IMAGE}:api',
    ]
    assert _tags(by_name['minimal']) == [f'{IMAGE}:5.0.0-minimal', f'{IMAGE}:minimal']
    assert _tags(by_name['slim']) == [f'{IMAGE}:5.0.0-slim', f'{IMAGE}:slim']
    for variant in ('rust', 'rust-full'):
        assert _tags(by_name[variant]) == [f'{IMAGE}:5.0.0-{variant}', f'{IMAGE}:{variant}']
    for command in commands:
        assert command.split(' ')[-2].endswith('-amd64') and command.split(' ')[-1].endswith('-arm64')


@pytest.mark.parametrize('args', [
    ('--arch-tags',),
    ('--arch-tags', '--push'),
    ('--arch-tags', '--push', '--platform', 'linux/amd64,linux/arm64'),
    ('--arch-tags', '--platform', 'linux/arm64'),
    ('--merge', '--push'),
    ('--merge', '--platform', 'linux/arm64'),
    ('--bogus',),
])
def test_inconsistent_options_are_refused_before_any_command(tmp_path, args):
    result, commands = _buildx(tmp_path, '5.0.0rc2', *args)
    assert result.returncode == 2, (result.stdout, result.stderr)
    assert commands == []
    assert 'buildx.sh:' in result.stderr


def test_a_v4_version_is_never_pushed(tmp_path):
    for args in (('--push',), ('--merge',)):
        result, commands = _buildx(tmp_path, '4.1.7', *args)
        assert result.returncode == 1 and commands == []
        assert 'refusing to push' in result.stderr


def test_the_final_release_platforms_are_amd64_and_arm64():
    text = (REPO / 'scripts' / 'config.sh').read_text()
    assert 'export platforms="linux/amd64,linux/arm64"' in text


def test_a_sealed_sdk_context_replaces_source_compilation(tmp_path):
    sdk = tmp_path / 'sdk'
    (sdk / 'wheels').mkdir(parents=True)
    (sdk / 'mrsm').write_bytes(b'verified-program')
    result, commands = _buildx(tmp_path, '5.0.0rc25', '--sdk-artifacts', str(sdk))
    assert result.returncode == 0, result.stderr
    assert len(commands) == 4
    assert all(f'--build-context sdk={sdk}' in c and '--build-arg sdk_source=artifacts' in c for c in commands)
    assert all('--label org.opencontainers.image.revision=' in c for c in commands)


def _dockerfile_stages() -> dict[str, str]:
    text = (REPO / 'Dockerfile').read_text(encoding='utf-8')
    stages: dict[str, list[str]] = {}
    current = None
    for line in text.splitlines():
        match = re.match(r'^FROM\s+(?:--platform=\S+\s+)?\S+\s+AS\s+(\S+)\s*$', line)
        if match:
            current = match.group(1)
            stages[current] = [line]
        elif current is not None:
            stages[current].append(line)
    return {name: '\n'.join(lines) for name, lines in stages.items()}


def test_dockerfile_separates_stable_and_volatile_layers_for_caching():
    stages = _dockerfile_stages()

    # 1. Confined compiler snapshot must not depend on root Cargo.toml version bumps.
    assert 'COPY Cargo.toml' not in stages['confined-compiler']
    assert '--manifest ' not in stages['confined-compiler']
    assert '--snapshot-only' in stages['confined-compiler']
    assert '--policy-only' in stages['compiler-policy']

    # 2. Third-party Python extras must not depend on the volatile Meerschaum wheel.
    assert 'wheel-selected' not in stages['py-full-deps']
    assert 'requirements/full.txt' in stages['py-full-deps']
    assert '--no-deps' in stages['py-slim']

    # 3. Rust OS rootfs stages must not copy `mrsm` before installing build tools.
    assert 'native-selected' not in stages['rust-os-rootfs']
    assert 'native-selected' not in stages['rust-full-os-rootfs']

    # 4. Final `FROM scratch` stages layer stable assets before volatile release assets
    #    so Docker Hub deduplicates unchanged layers across releases and variants.
    def copy_sources(stage: str) -> list[str]:
        return re.findall(r'^COPY\b[^\n]*--from=(\S+)', stages[stage], flags=re.M)

    full_sources = copy_sources('full')
    rust_full_sources = copy_sources('rust-full')
    rust_sources = copy_sources('rust')
    slim_sources = copy_sources('slim')

    assert full_sources.index('py-full-deps') < full_sources.index('py-slim')
    assert full_sources.index('confined-compiler') < full_sources.index('native-selected')
    assert full_sources.index('native-selected') < full_sources.index('compiler-policy')
    assert rust_full_sources.index('confined-compiler') < rust_full_sources.index('native-selected')
    assert rust_sources.index('rust-os-rootfs') < rust_sources.index('native-selected')
    assert slim_sources.index('slim-os-rootfs') < slim_sources.index('py-slim')

    # Shared layers between variants must use identical linked COPY lines: with `--link` an identical
    # copy is one layer, independent of the layers below it, so it has one digest in every image.
    for shared_copy in (
        'COPY --link --from=confined-compiler /confined-compiler/rootfs/usr/local/lib/meerschaum/compiler/ /usr/local/lib/meerschaum/compiler/',
        'COPY --link --from=confined-compiler /confined-compiler/compat/ /',
        'COPY --link --from=rust-toolchain /usr/local/cargo/bin/ /usr/local/cargo/bin/',
        'COPY --link --from=native-selected /out/mrsm /usr/local/bin/mrsm',
        'COPY --link --from=compiler-policy /compiler-policy/etc/meerschaum/ /etc/meerschaum/',
    ):
        assert shared_copy in stages['full'].splitlines()
        assert shared_copy in stages['rust-full'].splitlines()
    assert 'COPY --link --from=native-selected /out/mrsm /usr/local/bin/mrsm' in stages['rust'].splitlines()
    assert 'COPY --link --from=py-slim /install/ /usr/local/' in stages['slim'].splitlines()
    assert 'COPY --link --from=py-slim /install/ /usr/local/' in stages['full'].splitlines()


def test_final_images_link_every_copy_except_into_the_users_home():
    """A linked layer brings its own root-owned parent directories, which must not replace the
    user-owned home directories; everywhere else `--link` decouples a layer from those below it."""
    stages = _dockerfile_stages()
    for stage in ('slim', 'full', 'rust', 'rust-full'):
        copies = [line for line in stages[stage].splitlines() if line.startswith('COPY ')]
        assert copies, stage
        for line in copies:
            into_home = ' /home/$mrsm_user/' in line
            assert ('--link' in line) is not into_home, line
            assert ('--chown=$mrsm_uid:$mrsm_gid' in line) is into_home, line


def test_the_base_image_and_toolchain_are_pinned():
    text = (REPO / 'Dockerfile').read_text(encoding='utf-8')
    assert re.search(r'^ARG fedora_image=fedora:44@sha256:[0-9a-f]{64}$', text, re.M)
    assert re.search(r'^ARG rust_toolchain=\d+\.\d+\.\d+$', text, re.M)
    assert re.search(r'^ARG rustup_version=\d+\.\d+\.\d+$', text, re.M)
    for arch in ('amd64', 'arm64'):
        assert re.search(rf'^ARG rustup_sha256_{arch}=[0-9a-f]{{64}}$', text, re.M)
    froms = re.findall(r'^FROM\s+(?:--platform=\S+\s+)?(\S+)\s+AS\s+\S+', text, re.M)
    assert not [image for image in froms if image.startswith('fedora')], 'every Fedora stage uses ${fedora_image}'
    assert froms.count('${fedora_image}') >= 8
    toolchain = _dockerfile_stages()['rust-toolchain']
    assert 'sh.rustup.rs' not in toolchain and 'sha256sum -c' in toolchain
    assert '--default-toolchain "$rust_toolchain"' in toolchain
    assert 'rust-toolchain.toml' not in toolchain.replace('rust-toolchain.toml\'s', ''), (
        'the image must not follow the repository\'s moving `stable` channel')
    assert '--component rustfmt --component clippy' in toolchain


def test_only_the_small_runtimes_drop_the_ca_sources_and_fips_provider():
    """`update-ca-trust` and FIPS hosts work in the images users extend (full, rust-full)."""
    stages = _dockerfile_stages()

    def drops(stage):
        return {item for item in ('ca-trust-source', 'ossl-modules/fips.so') if item in stages[stage]}

    assert drops('slim-os-rootfs') == {'ca-trust-source', 'ossl-modules/fips.so'}
    assert drops('rust-os-rootfs') == {'ca-trust-source', 'ossl-modules/fips.so'}
    for stage in ('full-os-rootfs', 'rust-base-rootfs', 'rust-full-os-rootfs'):
        assert drops(stage) == set(), stage
    assert 'FROM ${fedora_image} AS rust-base-rootfs' in stages['rust-base-rootfs']
    assert 'FROM rust-base-rootfs AS rust-full-os-rootfs' in stages['rust-full-os-rootfs']
    assert 'COPY --link --from=rust-full-os-rootfs /rootfs/ /' in stages['rust-full']
    assert 'COPY --link --from=rust-os-rootfs /rootfs/ /' in stages['rust']


def test_the_smoke_test_checks_each_attested_image_against_its_manifest():
    script = (REPO / 'scripts' / 'docker' / 'smoke_image.sh').read_text(encoding='utf-8')
    assert '--verify-export -' in script and 'docker export' in script
    loops = script[script.index('for image in "${rust_images[@]}" "${rust_full_images[@]}"; do'):]
    assert loops.index('factory_manifest "$image"') < loops.index('check "native SQL')
    full = loops[loops.index('for image in "${full_images[@]}"; do'):]
    assert 'factory_manifest "$image"' in full[:200]
    subprocess.run(['bash', '-n', str(REPO / 'scripts' / 'docker' / 'smoke_image.sh')], check=True)


# ---------------------------------------------------------------------------------------------
# Candidates pushed by digest to the staging repository
# ---------------------------------------------------------------------------------------------

def test_a_candidate_is_pushed_without_attestations_and_tested_by_digest(tmp_path):
    sdk = tmp_path / 'sdk'
    (sdk / 'wheels').mkdir(parents=True)
    (sdk / 'mrsm').write_bytes(b'verified-program')
    os.environ.pop('GITHUB_RUN_ID', None)
    result, commands = _buildx(
        tmp_path, '5.1.2', '--platform', 'linux/arm64', '--sdk-artifacts', str(sdk),
        '--candidate', 'ghcr.io/owner/repo/candidate', '--metadata-dir', str(tmp_path / 'meta'),
        '--cache-ref', 'ghcr.io/owner/repo/buildcache',
    )
    assert result.returncode == 0, result.stderr
    builds = [c for c in commands if c.startswith('docker buildx build')]
    pulls = [c for c in commands if c.startswith('docker pull')]
    assert len(builds) == 4 and len(pulls) == 4 and len(commands) == 8
    repository = 'ghcr.io/owner/repo/candidate'
    assert [_tags(c) for c in builds] == [
        [f'{repository}:5.1.2-minimal-arm64-local-1', f'{repository}:5.1.2-slim-arm64-local-1'],
        [f'{repository}:5.1.2-full-arm64-local-1'],
        [f'{repository}:5.1.2-rust-arm64-local-1'],
        [f'{repository}:5.1.2-rust-full-arm64-local-1'],
    ]
    for command, target in zip(builds, ('slim', 'full', 'rust', 'rust-full')):
        assert ' --push --provenance=false --sbom=false ' in command and '--load' not in command
        assert f'--metadata-file {tmp_path / "meta"}/{target}.json' in command
        assert _platform(command) == 'linux/arm64'
        for cached in ('slim', 'full', 'rust', 'rust-full'):
            assert f'--cache-from type=registry,ref=ghcr.io/owner/repo/buildcache:{cached}-arm64' in command
        assert (f'--cache-to type=registry,ref=ghcr.io/owner/repo/buildcache:{target}-arm64,mode=max,'
                'image-manifest=true,oci-mediatypes=true,ignore-error=true') in command
        assert '--build-arg sdk_source=artifacts' in command
    assert all(c.startswith(f'docker pull --quiet --platform linux/arm64 {repository}@sha256:') for c in pulls)
    assert not any('bmeares/meerschaum' in c for c in commands), 'a candidate never touches Docker Hub'


def test_the_image_user_is_pinned_by_the_release_environment(tmp_path):
    result, commands = _buildx(tmp_path, '5.1.2', '--platform', 'linux/amd64', '--push', '--arch-tags',
                               env={'MRSM_IMAGE_UID': '1000', 'MRSM_IMAGE_GID': '1000'})
    assert result.returncode == 0, result.stderr
    assert all('--build-arg mrsm_uid=1000 ' in c and '--build-arg mrsm_gid=1000 ' in c for c in commands)


@pytest.mark.parametrize('args', [
    ('--candidate', 'ghcr.io/o/r/candidate', '--metadata-dir', 'm'),
    ('--candidate', 'ghcr.io/o/r/candidate', '--platform', 'linux/amd64'),
    ('--candidate', 'ghcr.io/o/r/candidate', '--metadata-dir', 'm', '--platform', 'linux/amd64,linux/arm64'),
    ('--candidate', 'ghcr.io/o/r/candidate', '--metadata-dir', 'm', '--platform', 'linux/amd64', '--push'),
    ('--candidate', 'ghcr.io/o/r/candidate:tag', '--metadata-dir', 'm', '--platform', 'linux/amd64'),
    ('--candidate', 'ghcr.io/o/r/candidate', '--metadata-dir', 'm', '--platform', 'linux/amd64', '--v4-only'),
    ('--metadata-dir', 'm', '--platform', 'linux/amd64'),
    ('--cache-ref', 'ghcr.io/o/r/buildcache'),
    ('--merge', '--cache-ref', 'ghcr.io/o/r/buildcache'),
    ('--merge', '--candidate', 'ghcr.io/o/r/candidate'),
])
def test_inconsistent_candidate_options_are_refused_before_any_command(tmp_path, args):
    result, commands = _buildx(tmp_path, '5.1.2', *args)
    assert result.returncode == 2, (result.stdout, result.stderr)
    assert commands == [] and 'buildx.sh:' in result.stderr


# ---------------------------------------------------------------------------------------------
# The shipped native Factory manifest, checked against the final image's own files
# ---------------------------------------------------------------------------------------------

_MANIFEST_SOURCE = REPO / 'scripts' / 'docker' / 'write_native_factory_manifest.py'
_manifest_spec = importlib.util.spec_from_file_location('factory_manifest_export_check', _MANIFEST_SOURCE)
factory = importlib.util.module_from_spec(_manifest_spec)
_manifest_spec.loader.exec_module(factory)
_SDK = '/usr/local/lib/python3.14/site-packages'


def _elf():
    header = bytearray(20)
    header[:6] = b'\x7fELF\x02\x01'
    header[18:20] = (62).to_bytes(2, 'little')
    return bytes(header)


def _image_files():
    return {
        factory.CLI: _elf(),
        _SDK + '/meerschaum/__init__.py': b'',
        _SDK + '/meerschaum/_core/_native.abi3.so': _elf(),
        _SDK + '/_meerschaum_mrsm.py': b'# shim\n',
        '/usr/bin/bash': b'not attested',
    }


def _manifest_for(files, tmp_path):
    root = tmp_path / 'attest-root'
    for name, payload in files.items():
        path = root / name.lstrip('/')
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_bytes(payload)
        path.chmod(0o444)
    return factory.build(root, '5.1.2', 'x86_64', _SDK, runtime_uid=-1)


def _export(files, manifest, *, modes=None, owners=None, links=()):
    """A `docker export`-like tar: root-owned directories and files, with optional damage."""
    modes, owners = modes or {}, owners or {}
    stream = io.BytesIO()
    entries = {**files, factory.MANIFEST: manifest}
    with tarfile.open(fileobj=stream, mode='w') as archive:
        directories = sorted({str(PurePosixPath(name).parents[i])
                              for name in entries for i in range(len(PurePosixPath(name).parts) - 2)})
        for directory in directories:
            info = tarfile.TarInfo(directory.lstrip('/'))
            info.type, info.mode = tarfile.DIRTYPE, modes.get(directory, 0o755)
            info.uid = owners.get(directory, 0)
            archive.addfile(info)
        for name, payload in entries.items():
            info = tarfile.TarInfo(name.lstrip('/'))
            info.size, info.mode, info.uid = len(payload), modes.get(name, 0o644), owners.get(name, 0)
            archive.addfile(info, io.BytesIO(payload))
        for name, target in links:
            info = tarfile.TarInfo(name.lstrip('/'))
            info.type, info.linkname = tarfile.LNKTYPE, target.lstrip('/')
            archive.addfile(info)
    stream.seek(0)
    return stream


def test_an_image_matching_its_manifest_passes(tmp_path):
    files = _image_files()
    manifest = _manifest_for(files, tmp_path)
    checked = factory.verify_export(_export(files, manifest), 1000, scratch=tmp_path)
    assert checked['cli']['path'] == factory.CLI and checked['sdk']['root'] == _SDK


@pytest.mark.parametrize('damage', [
    'changed_cli', 'extra_sdk_file', 'missing_sdk_file', 'group_writable', 'runtime_owned_writable',
    'runtime_owned_directory', 'hard_link', 'symlink_in_sdk', 'no_manifest',
])
def test_an_image_that_differs_from_its_manifest_fails(tmp_path, damage):
    files = _image_files()
    manifest = _manifest_for(files, tmp_path)
    kwargs = {}
    if damage == 'changed_cli':
        files[factory.CLI] = _elf() + b'patched'
    elif damage == 'extra_sdk_file':
        files[_SDK + '/meerschaum/extra.py'] = b'not attested'
    elif damage == 'missing_sdk_file':
        del files[_SDK + '/_meerschaum_mrsm.py']
    elif damage == 'group_writable':
        kwargs['modes'] = {factory.CLI: 0o664}
    elif damage == 'runtime_owned_writable':
        kwargs['owners'], kwargs['modes'] = {factory.CLI: 1000}, {factory.CLI: 0o644}
    elif damage == 'runtime_owned_directory':
        kwargs['owners'] = {'/usr/local/bin': 1000}
    elif damage == 'hard_link':
        kwargs['links'] = [('/usr/bin/mrsm-copy', factory.CLI)]
    elif damage == 'symlink_in_sdk':
        stream = _export(files, manifest)
        archive_bytes = stream.getvalue()
        extra = io.BytesIO()
        with tarfile.open(fileobj=io.BytesIO(archive_bytes)) as source, tarfile.open(fileobj=extra, mode='w') as out:
            for member in source.getmembers():
                out.addfile(member, source.extractfile(member) if member.isreg() else None)
            link = tarfile.TarInfo((_SDK + '/meerschaum/link.py').lstrip('/'))
            link.type, link.linkname = tarfile.SYMTYPE, '/etc/passwd'
            out.addfile(link)
        extra.seek(0)
        with pytest.raises(ValueError, match='link or special file'):
            factory.verify_export(extra, 1000, scratch=tmp_path)
        return
    if damage == 'no_manifest':
        stream = io.BytesIO()
        with tarfile.open(fileobj=stream, mode='w') as archive:
            info = tarfile.TarInfo('usr/bin/bash')
            info.size = 1
            archive.addfile(info, io.BytesIO(b'x'))
        stream.seek(0)
        with pytest.raises(ValueError, match='no native Factory manifest'):
            factory.verify_export(stream, 1000, scratch=tmp_path)
        return
    with pytest.raises(ValueError):
        factory.verify_export(_export(files, manifest, **kwargs), 1000, scratch=tmp_path)
