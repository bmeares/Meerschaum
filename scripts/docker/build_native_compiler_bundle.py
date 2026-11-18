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


def build(toolchain: Path, vendor: Path, lock: Path, manifest: Path,
          settings: Path, output: Path) -> str:
    if (not output.is_absolute() or output.exists() or output.is_symlink()
            or output.parent.resolve() != output.parent):
        raise ValueError('Output must be a new absolute bundle directory.')
    with snapshot.regular(manifest) as stream:
        workspace_bytes = stream.read(1024 * 1024 + 1)
    if len(workspace_bytes) > 1024 * 1024:
        raise ValueError('Workspace manifest exceeds its build input bound.')
    workspace = tomllib.loads(workspace_bytes.decode())
    release = workspace['workspace']['package']['version']
    with snapshot.regular(settings) as stream:
        rustup_bytes = stream.read(16 * 1024 + 1)
    if len(rustup_bytes) > 16 * 1024:
        raise ValueError('Rustup settings exceed their build input bound.')
    rustup = tomllib.loads(rustup_bytes.decode())
    name = toolchain.name
    if (not name or name in {'.', '..'} or '/' in name or '\\' in name
            or rustup.get('default_toolchain') != name):
        raise ValueError('The builder default must name the exact installed toolchain.')
    pins = {
        'schema': 1, 'release': release,
        'architecture': snapshot.architecture(toolchain / 'bin/rustc'),
        'toolchain_sha256': snapshot.tree_digest(snapshot.files(toolchain)),
        'vendor_sha256': snapshot.tree_digest(snapshot.files(vendor)),
        'lock_sha256': snapshot.digest(snapshot.read_lock(lock)),
    }
    output.mkdir(mode=0o700)
    try:
        pinfile = output / 'input-pins.json'
        pinfile.write_bytes(snapshot.encoded(pins))
        identity = snapshot.assemble(toolchain, vendor, lock, pinfile, output / 'rootfs')
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


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    for name in ('toolchain', 'vendor', 'lock', 'manifest', 'settings', 'output'):
        parser.add_argument('--' + name, type=Path, required=True)
    args = parser.parse_args()
    print(build(args.toolchain, args.vendor, args.lock, args.manifest, args.settings, args.output))


if __name__ == '__main__':
    main()
