#!/usr/bin/env python3
"""Seal image-build compiler material and retain the ordinary Rustup entry points.

Build-stage tool only: inputs come from the reviewed source lock and the installed
builder toolchain. This does not invoke a compiler, install anything on the host,
or provide a runtime policy selector. The installed policy stays at its fixed path.
"""
from __future__ import annotations

import argparse
import os
from pathlib import Path
import shutil
import tomllib

import native_compiler_snapshot as snapshot


def _read_workspace_release(manifest: Path) -> str:
    with snapshot.regular(manifest) as stream:
        workspace_bytes = stream.read(1024 * 1024 + 1)
    if len(workspace_bytes) > 1024 * 1024:
        raise ValueError('Workspace manifest exceeds its build input bound.')
    workspace = tomllib.loads(workspace_bytes.decode())
    return workspace['workspace']['package']['version']


def _validate_output(output: Path) -> None:
    if (not output.is_absolute() or output.exists() or output.is_symlink()
            or output.parent.resolve() != output.parent):
        raise ValueError('Output must be a new absolute bundle directory.')


def _read_rustup_settings(settings: Path, toolchain: Path) -> tuple[bytes, str]:
    with snapshot.regular(settings) as stream:
        rustup_bytes = stream.read(16 * 1024 + 1)
    if len(rustup_bytes) > 16 * 1024:
        raise ValueError('Rustup settings exceed their build input bound.')
    rustup = tomllib.loads(rustup_bytes.decode())
    name = toolchain.name
    if (not name or name in {'.', '..'} or '/' in name or '\\' in name
            or rustup.get('default_toolchain') != name):
        raise ValueError('The builder default must name the exact installed toolchain.')
    return rustup_bytes, name


def _build_bundle(
    toolchain: Path,
    vendor: Path,
    lock: Path,
    manifest: Path | None,
    settings: Path,
    output: Path,
) -> str:
    _validate_output(output)
    release = _read_workspace_release(manifest) if manifest is not None else None
    rustup_bytes, name = _read_rustup_settings(settings, toolchain)
    pins = {
        'schema': 1,
        'architecture': snapshot.architecture(toolchain / 'bin/rustc'),
        'toolchain_sha256': snapshot.tree_digest(snapshot.files(toolchain)),
        'vendor_sha256': snapshot.tree_digest(snapshot.files(vendor)),
        'lock_sha256': snapshot.digest(snapshot.read_lock(lock)),
    }
    if release is not None:
        pins['release'] = release
    output.mkdir(mode=0o700)
    try:
        pinfile = output / 'input-pins.json'
        pinfile.write_bytes(snapshot.encoded(pins))
        if release is not None:
            identity = snapshot.assemble(toolchain, vendor, lock, pinfile, output / 'rootfs')
        else:
            identity = snapshot.assemble_material(toolchain, vendor, lock, pinfile, output / 'rootfs')
        # Keep the regular full-image Rustup shims and settings. Its one toolchain
        # resolves to the same frozen material as confined compilation, avoiding a
        # second toolchain copy and preserving ordinary Cargo/rustfmt/clippy commands.
        compat = output / 'compat/usr/local/rustup'
        (compat / 'toolchains').mkdir(parents=True)
        # The installed Rustup shims check this directory even for `cargo --version`.
        (compat / 'update-hashes').mkdir(mode=0o555)
        (compat / 'settings.toml').write_bytes(rustup_bytes)
        (compat / 'settings.toml').chmod(0o444)
        (compat / 'toolchains' / name).symlink_to(
            snapshot.PREFIX / identity / 'toolchain', target_is_directory=True)
        return identity
    except BaseException:
        # Only this invocation's never-published new output directory is removable.
        for directory, _, _ in os.walk(output):
            Path(directory).chmod(0o700)
        shutil.rmtree(output)
        raise


def build(toolchain: Path, vendor: Path, lock: Path, manifest: Path,
          settings: Path, output: Path) -> str:
    return _build_bundle(toolchain, vendor, lock, manifest, settings, output)


def build_snapshot(toolchain: Path, vendor: Path, lock: Path,
                   settings: Path, output: Path) -> str:
    """Seal release-independent compiler material and Rustup compat links (no policy file)."""
    return _build_bundle(toolchain, vendor, lock, None, settings, output)


def build_policy(snapshot_rootfs: Path, manifest: Path, output: Path) -> str:
    """Write /etc/meerschaum/native-runtime.json binding a sealed snapshot to the workspace release."""
    _validate_output(output)
    release = _read_workspace_release(manifest)
    _, snapshot_dir = snapshot.locate_snapshot(snapshot_rootfs)
    arch = snapshot.architecture(snapshot_dir / 'toolchain/bin/rustc')
    return snapshot.write_policy(snapshot_rootfs, release, arch, output)


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    mode = parser.add_mutually_exclusive_group()
    mode.add_argument('--snapshot-only', action='store_true')
    mode.add_argument('--policy-only', action='store_true')
    for name in ('toolchain', 'vendor', 'lock', 'manifest', 'settings', 'snapshot-rootfs'):
        parser.add_argument('--' + name, type=Path)
    parser.add_argument('--output', type=Path, required=True)
    args = parser.parse_args()
    if args.policy_only:
        if args.snapshot_rootfs is None or args.manifest is None:
            parser.error('--policy-only requires --snapshot-rootfs and --manifest')
        print(build_policy(args.snapshot_rootfs, args.manifest, args.output))
    elif args.snapshot_only:
        if any(getattr(args, field) is None for field in ('toolchain', 'vendor', 'lock', 'settings')):
            parser.error('--snapshot-only requires --toolchain, --vendor, --lock, and --settings')
        print(build_snapshot(args.toolchain, args.vendor, args.lock, args.settings, args.output))
    else:
        if any(getattr(args, field) is None for field in ('toolchain', 'vendor', 'lock', 'manifest', 'settings')):
            parser.error('requires --toolchain, --vendor, --lock, --manifest, and --settings')
        print(build(args.toolchain, args.vendor, args.lock, args.manifest, args.settings, args.output))


if __name__ == '__main__':
    main()
