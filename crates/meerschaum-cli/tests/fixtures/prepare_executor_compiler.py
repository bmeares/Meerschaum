"""Copy explicitly supplied compiler/vendor bytes into a fixture-owned read-only snapshot.

This helper neither invokes Cargo/rustc nor reads ambient home/toolchain/cache settings.
The root test coordinator supplies an audited offline vendor tree and exact generated
single-file Cargo.lock (root package mrsm_native_source, version 0.0.0) before invoking it.
Prints only the manifest SHA256; the native host constructor pins that identity.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import os
from pathlib import Path
import re
import stat
import tomllib

MAX_FILES = 16_384
MAX_BYTES = 1024 * 1024 * 1024


def prepare(toolchain, vendor, lock, host, output):
    toolchain, vendor, lock = (Path(path).resolve(strict=True) for path in (toolchain, vendor, lock))
    output = Path(output).absolute()
    if output.exists() or not re.fullmatch(r'[A-Za-z0-9_-]+', host):
        raise ValueError('Supply a new fixture directory and an explicit host triple.')
    if not lock.is_file():
        raise ValueError('The exact compiler lock must be a regular file.')
    with lock.open('rb') as incoming:
        lock_bytes = incoming.read(1024 * 1024 + 1)
    if len(lock_bytes) > 1024 * 1024:
        raise ValueError('The exact compiler lock exceeds its limit.')
    parsed = tomllib.loads(lock_bytes.decode('utf-8'))
    roots = [entry for entry in parsed.get('package', ()) if 'source' not in entry]
    if len(roots) != 1 or roots[0].get('name') != 'mrsm_native_source' or roots[0].get('version') != '0.0.0':
        raise ValueError('The lock must bind the exact generated single-file package template.')
    output.mkdir(mode=0o700)
    files, total = {}, 0

    def copy(source, relative, boundary, executable=False):
        nonlocal total
        # Internal toolchain symlinks are copied by content into regular owned files.
        # A link escaping either explicitly supplied input tree is refused.
        source = source.resolve(strict=True)
        if not source.is_relative_to(boundary) or not source.is_file():
            raise ValueError('An input file escaped its selected snapshot tree.')
        size = source.stat().st_size
        if len(files) >= MAX_FILES - 1 or total + size + len(lock_bytes) > MAX_BYTES:
            raise ValueError('The compiler snapshot exceeds its file or byte quota.')
        target = output / relative
        target.parent.mkdir(mode=0o700, parents=True, exist_ok=True)
        hasher, copied = hashlib.sha256(), 0
        descriptor = os.open(source, os.O_RDONLY | os.O_NOFOLLOW | os.O_NONBLOCK)
        with os.fdopen(descriptor, 'rb') as incoming, target.open('xb') as outgoing:
            if not stat.S_ISREG(os.fstat(incoming.fileno()).st_mode):
                raise ValueError('Compiler input is not a regular file.')
            while chunk := incoming.read(1024 * 1024):
                copied += len(chunk)
                if total + copied + len(lock_bytes) > MAX_BYTES:
                    raise ValueError('The compiler input changed beyond its byte quota.')
                outgoing.write(chunk)
                hasher.update(chunk)
        if copied != size:
            raise ValueError('The compiler input changed while it was copied.')
        target.chmod(0o555 if executable else 0o444)
        files[relative.as_posix()] = hasher.hexdigest()
        total += copied

    def tree(source, relative, boundary):
        for directory, directories, names in os.walk(source, followlinks=False):
            directories.sort()
            names.sort()
            for name in directories:
                if (Path(directory) / name).is_symlink():
                    raise ValueError('Directory symlinks need an explicit owned snapshot.')
            for name in names:
                path = Path(directory) / name
                copy(path, relative / path.relative_to(source), boundary)

    if (toolchain / 'bin/cargo').resolve(strict=True) == (toolchain / 'bin/rustc').resolve(strict=True):
        raise ValueError('Cargo and rustc must be real distinct executables, not multicall shims.')
    for name in ('cargo', 'rustc'):
        source = toolchain / 'bin' / name
        with source.open('rb') as executable:
            if executable.read(4) != b'\x7fELF':
                raise ValueError('Supply actual compiler executables, not rustup or shell wrappers.')
        copy(source, Path('toolchain/bin') / name, toolchain, executable=True)
    lib = toolchain / 'lib'
    for path in sorted(lib.iterdir()):
        if path.is_file() and (path.name.startswith('libLLVM') or path.name.startswith('librustc_driver')):
            copy(path, Path('toolchain/lib') / path.name, toolchain)
    host_lib = lib / 'rustlib' / host
    tree(host_lib / 'lib', Path('toolchain/lib/rustlib') / host / 'lib', toolchain)
    if (host_lib / 'bin').is_dir():
        # Host linkers are executables; the rest of the sysroot remains data-only.
        for path in sorted((host_lib / 'bin').rglob('*')):
            if path.is_file():
                copy(path, Path('toolchain/lib/rustlib') / host / 'bin' / path.relative_to(host_lib / 'bin'), toolchain, executable=True)
    tree(vendor, Path('vendor'), vendor)
    target_lock = output / 'Cargo.lock'
    target_lock.write_bytes(lock_bytes)
    target_lock.chmod(0o444)
    files['Cargo.lock'] = hashlib.sha256(lock_bytes).hexdigest()
    encoded = json.dumps({'schema': 1, 'files': dict(sorted(files.items()))}, separators=(',', ':')).encode('utf-8')
    if len(encoded) > 2 * 1024 * 1024:
        raise ValueError('The compiler manifest exceeds its byte quota.')
    manifest = output / 'compiler.json'
    manifest.write_bytes(encoded)
    manifest.chmod(0o444)
    for directory, directories, _ in os.walk(output, topdown=False):
        for name in directories:
            (Path(directory) / name).chmod(0o555)
    output.chmod(0o555)
    return hashlib.sha256(encoded).hexdigest()


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description=__doc__)
    for name in ('toolchain', 'vendor', 'lock', 'host', 'output'):
        parser.add_argument('--' + name, required=True)
    arguments = parser.parse_args()
    print(prepare(arguments.toolchain, arguments.vendor, arguments.lock, arguments.host, arguments.output))
