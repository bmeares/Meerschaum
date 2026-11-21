#!/usr/bin/env python3
"""Describe installed native Factory bytes without importing or executing them.

Run only in the administrator's image build, after installing the CLI, optional SDK,
and optional confined compiler.  The manifest is public evidence, not a trust root:
`mrsm stack up` must independently verify these bytes in its selected immutable image.
"""
from __future__ import annotations

import argparse
import hashlib
import json
import os
from pathlib import Path, PurePosixPath
import shutil
import stat
import sys
import tarfile
import tempfile


MANIFEST = '/usr/local/share/meerschaum/native-factory.json'
CLI = '/usr/local/bin/mrsm'
POLICY = '/etc/meerschaum/native-runtime.json'
COMPILERS = '/usr/local/lib/meerschaum/compiler/'
MAX_MANIFEST = 8 * 1024 * 1024
MAX_FILES = 32768
MAX_FILE = 1024 * 1024 * 1024
MAX_TOTAL = 4 * 1024 * 1024 * 1024


def canonical(path: str, absolute: bool) -> str:
    if (not isinstance(path, str) or not path or len(path.encode()) > 1024
            or '\\' in path or any(ord(c) < 32 or ord(c) == 127 for c in path)
            or path.startswith('/') != absolute
            or any(part in ('', '.', '..') for part in path.lstrip('/').split('/'))):
        raise ValueError('A manifest path is not canonical.')
    return path


def strict_json(raw: bytes):
    def pairs(items):
        value = {}
        for key, item in items:
            if key in value:
                raise ValueError('Duplicate manifest or policy field.')
            value[key] = item
        return value
    return json.loads(raw, object_pairs_hook=pairs)


class Reader:
    def __init__(self, root: Path, runtime_uid: int):
        self.root = root
        self.runtime_uid = runtime_uid
        self.count = 0
        self.total = 0

    def _check(self, metadata, directory=False):
        if (not (stat.S_ISDIR(metadata.st_mode) if directory else stat.S_ISREG(metadata.st_mode))
                or not directory and metadata.st_nlink != 1
                or metadata.st_mode & 0o022
                or metadata.st_uid == self.runtime_uid and metadata.st_mode & 0o200):
            raise ValueError('Installed Factory authority material is writable or nonregular.')

    def open(self, path: str):
        canonical(path, True)
        directory = os.open(self.root, os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW | os.O_CLOEXEC)
        try:
            parts = path[1:].split('/')
            for component in parts[:-1]:
                child = os.open(component, os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW | os.O_CLOEXEC,
                                dir_fd=directory)
                os.close(directory)
                directory = child
                self._check(os.fstat(directory), True)
            descriptor = os.open(parts[-1], os.O_RDONLY | os.O_NOFOLLOW | os.O_NONBLOCK | os.O_CLOEXEC,
                                 dir_fd=directory)
            try:
                self._check(os.fstat(descriptor))
            except BaseException:
                os.close(descriptor)
                raise
            return os.fdopen(descriptor, 'rb')
        finally:
            os.close(directory)

    def file(self, path: str, capture=False, machine=None):
        with self.open(path) as stream:
            before = os.fstat(stream.fileno())
            if before.st_size > MAX_FILE:
                raise ValueError('An installed Factory file exceeds its bound.')
            self.count += 1
            self.total += before.st_size
            if self.count > MAX_FILES or self.total > MAX_TOTAL:
                raise ValueError('Installed Factory material exceeds its aggregate bound.')
            digest = hashlib.sha256()
            payload = bytearray() if capture else None
            header = bytearray()
            size = 0
            while chunk := stream.read(128 * 1024):
                size += len(chunk)
                if size > before.st_size or capture and size > MAX_MANIFEST:
                    raise ValueError('An installed Factory file changed or exceeds its bound.')
                digest.update(chunk)
                if len(header) < 20:
                    header.extend(chunk[:20-len(header)])
                if payload is not None:
                    payload.extend(chunk)
            after = os.fstat(stream.fileno())
            if (size != before.st_size or (before.st_dev, before.st_ino, before.st_size,
                                          before.st_mtime_ns, before.st_ctime_ns)
                    != (after.st_dev, after.st_ino, after.st_size, after.st_mtime_ns, after.st_ctime_ns)):
                raise ValueError('Installed Factory material changed while being read.')
            if machine is not None and (len(header) != 20 or header[:6] != b'\x7fELF\x02\x01'
                                        or int.from_bytes(header[18:20], 'little') != machine):
                raise ValueError('Installed native Factory material has another format or architecture.')
            entry = {'bytes': size, 'sha256': digest.hexdigest()}
            return (entry, bytes(payload)) if capture else entry

    def tree(self, root: str, skip_pycache=False, machine=None):
        canonical(root, True)
        result = {}
        start = self.root / root[1:]
        nodes = 0
        # open() below rechecks every ancestor through nofollow descriptors. Walking
        # never follows links, and every encountered nonregular entry is a refusal.
        for parent, directories, files in os.walk(start, followlinks=False):
            if skip_pycache:
                directories[:] = [name for name in directories if name != '__pycache__']
            nodes += len(directories) + len(files)
            if nodes > MAX_FILES * 2 or len(Path(parent).relative_to(start).parts) > 64:
                raise ValueError('Installed Factory tree exceeds its node/depth bound.')
            directories.sort()
            files.sort()
            for name in directories:
                item = Path(parent) / name
                self._check(item.lstat(), True)
            for name in files:
                item = Path(parent) / name
                relative = canonical(item.relative_to(start).as_posix(), False)
                native = (relative.startswith('_core/_native') and relative.endswith('.so')
                          or relative in ('toolchain/bin/cargo', 'toolchain/bin/rustc'))
                result[relative] = self.file(root + '/' + relative, machine=machine if native else None)
        return result


def build(root: Path, release: str, architecture: str, sdk_root: str | None,
          runtime_uid=1000):
    if (not release or len(release) > 64 or any(ord(c) <= 32 for c in release)
            or architecture not in ('x86_64', 'aarch64')):
        raise ValueError('Invalid native Factory release or architecture.')
    reader = Reader(root, runtime_uid)
    machine = {'x86_64': 62, 'aarch64': 183}[architecture]
    cli = {'path': CLI, **reader.file(CLI, machine=machine)}
    sdk = None
    if sdk_root is not None:
        canonical(sdk_root, True)
        parts = PurePosixPath(sdk_root).parts
        if (len(parts) != 6 or parts[:3] != ('/', 'usr', 'local')
                or parts[3] not in ('lib', 'lib64') or not parts[4].startswith('python')
                or parts[5] != 'site-packages'):
            raise ValueError('The SDK must be installed in a native system purelib.')
        files = {'meerschaum/' + key: value
                 for key, value in reader.tree(sdk_root + '/meerschaum', skip_pycache=True,
                                              machine=machine).items()}
        files['_meerschaum_mrsm.py'] = reader.file(sdk_root + '/_meerschaum_mrsm.py')
        if ('meerschaum/__init__.py' not in files
                or not any(key.startswith('meerschaum/_core/_native')
                           and key.endswith('.so') for key in files)):
            raise ValueError('The installed SDK is missing its package or native extension.')
        sdk = {'root': sdk_root, 'files': dict(sorted(files.items()))}
    compiler = None
    policy_path = root / POLICY[1:]
    if os.path.lexists(policy_path):
        entry, raw = reader.file(POLICY, capture=True)
        policy = strict_json(raw)
        if (set(policy) != {'schema', 'release', 'architecture', 'compiler'}
                or policy['schema'] != 1 or policy['release'] != release
                or policy['architecture'] != architecture
                or not isinstance(policy['compiler'], dict)
                or set(policy['compiler']) != {'snapshot', 'manifest_sha256', 'anchor'}):
            raise ValueError('The installed compiler policy does not match the Factory.')
        selection = policy['compiler']
        canonical(selection['anchor'], True)
        identity = selection['manifest_sha256']
        if (not isinstance(identity, str) or len(identity) != 64
                or any(c not in '0123456789abcdef' for c in identity)
                or selection['snapshot'] != COMPILERS + identity):
            raise ValueError('The compiler snapshot has no exact canonical identity.')
        files = reader.tree(selection['snapshot'], machine=machine)
        if 'compiler.json' not in files or files['compiler.json']['sha256'] != identity:
            raise ValueError('The installed compiler manifest identity does not match.')
        if not {'Cargo.lock', 'toolchain/bin/cargo', 'toolchain/bin/rustc'}.issubset(files):
            raise ValueError('The installed compiler lacks its exact native tools or lock.')
        with reader.open(selection['snapshot'] + '/compiler.json') as stream:
            manifest = strict_json(stream.read(MAX_MANIFEST + 1))
        if (set(manifest) != {'schema', 'files'} or manifest['schema'] != 1
                or set(manifest['files']) != set(files) - {'compiler.json'}
                or any(files[name]['sha256'] != digest for name, digest in manifest['files'].items())):
            raise ValueError('The installed compiler has missing, extra or changed material.')
        compiler = {'policy': {'path': POLICY, **entry}, 'snapshot': selection['snapshot'],
                    'manifest_sha256': identity, 'files': files}
    value = {'schema': 1, 'release': release, 'architecture': architecture,
             'cli': cli, 'sdk': sdk, 'compiler': compiler}
    encoded = (json.dumps(value, sort_keys=True, separators=(',', ':'), ensure_ascii=True) + '\n').encode()
    if len(encoded) > MAX_MANIFEST:
        raise ValueError('The native Factory manifest exceeds its bound.')
    return encoded


def write(root: Path, payload: bytes):
    destination = root / MANIFEST[1:]
    directory = os.open(root, os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW | os.O_CLOEXEC)
    try:
        for part in MANIFEST[1:].split('/')[:-1]:
            try:
                os.mkdir(part, 0o755, dir_fd=directory)
            except FileExistsError:
                pass
            child = os.open(part, os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW | os.O_CLOEXEC,
                            dir_fd=directory)
            os.close(directory)
            directory = child
        descriptor = os.open(MANIFEST.rsplit('/', 1)[1],
                             os.O_WRONLY | os.O_CREAT | os.O_EXCL | os.O_NOFOLLOW | os.O_CLOEXEC,
                             0o444, dir_fd=directory)
    finally:
        os.close(directory)
    # No pre-existing file, link or complete output is replaced. Failed attempts
    # leave no partial manifest which a later build could mistake for completed.
    try:
        with os.fdopen(descriptor, 'wb') as stream:
            stream.write(payload)
            stream.flush()
            os.fsync(stream.fileno())
    except BaseException:
        destination.unlink(missing_ok=True)
        raise


# Where the material a manifest can describe lives in an image (paths without the leading `/`).
_ATTESTED_PREFIXES = (CLI[1:], POLICY[1:], COMPILERS[1:], MANIFEST[1:])


def _attested_candidate(name: str) -> bool:
    """Whether an image path may be part of a manifest: extract it for the comparison."""
    if name.startswith(_ATTESTED_PREFIXES):
        return True
    parts = name.split('/')
    return (len(parts) >= 6 and parts[:2] == ['usr', 'local'] and parts[2] in ('lib', 'lib64')
            and parts[3].startswith('python') and parts[4] == 'site-packages'
            and (parts[5] == '_meerschaum_mrsm.py' and len(parts) == 6
                 or parts[5] == 'meerschaum' and '__pycache__' not in parts))


def _attested_paths(manifest: dict) -> set[str]:
    paths = {manifest['cli']['path']}
    if manifest.get('sdk'):
        paths |= {manifest['sdk']['root'] + '/' + name for name in manifest['sdk']['files']}
    if manifest.get('compiler'):
        paths.add(manifest['compiler']['policy']['path'])
        paths |= {manifest['compiler']['snapshot'] + '/' + name for name in manifest['compiler']['files']}
    return paths


def verify_export(stream, runtime_uid: int, scratch: Path | None = None) -> dict:
    """Check an image's exported filesystem (`docker export`) against its own Factory manifest.

    The build attests a reconstruction of the final image, so this reads the final image itself:
    the manifest it ships must be exactly what `build` writes for the files it contains (no missing,
    extra or changed SDK or compiler file, the same CLI and policy), and every attested file and
    its ancestor directories keep the owners and modes the tar records: regular, single-link, not
    group- or world-writable, and not writable by the runtime user.
    """
    headers: dict[str, tarfile.TarInfo] = {}
    linked: set[str] = set()
    irregular: set[str] = set()

    def canonical_member(raw: str) -> str:
        name = raw
        while name.startswith(('./', '/')):
            name = name[2:] if name.startswith('./') else name[1:]
        name = name.rstrip('/')
        if name and name != '.' and any(part in ('', '.', '..') for part in name.split('/')):
            raise ValueError(f'The image export has a noncanonical path: {raw!r}')
        return '' if name == '.' else name

    with tempfile.TemporaryDirectory(prefix='mrsm-factory-export-', dir=scratch) as directory:
        root = Path(directory)
        with tarfile.open(fileobj=stream, mode='r|') as archive:
            for member in archive:
                name = canonical_member(member.name)
                if not name:
                    continue
                if member.isdir() or _attested_candidate(name):
                    headers[name] = member
                if member.islnk():
                    linked.update({name, canonical_member(member.linkname)})
                if not _attested_candidate(name):
                    continue
                if not (member.isdir() or member.isreg()):
                    irregular.add('/' + name)
                destination = root / name
                if member.isdir():
                    destination.mkdir(mode=0o755, parents=True, exist_ok=True)
                elif member.isreg():
                    destination.parent.mkdir(mode=0o755, parents=True, exist_ok=True)
                    source = archive.extractfile(member)
                    with open(destination, 'wb') as target:
                        shutil.copyfileobj(source, target, 1024 * 1024)
                    destination.chmod(0o444)
        shipped_path = root / MANIFEST[1:]
        if not shipped_path.is_file():
            raise ValueError('The image has no native Factory manifest.')
        shipped = shipped_path.read_bytes()
        manifest = strict_json(shipped)
        sdk = manifest.get('sdk')
        # Ownership is checked from the tar headers below; the extracted copies belong to this user.
        try:
            expected = build(root, manifest.get('release'), manifest.get('architecture'),
                             sdk['root'] if sdk else None, runtime_uid=-1)
        except FileNotFoundError as error:
            raise ValueError(f'Factory material is missing from the image: {error.filename}') from error
        if expected != shipped:
            actual = strict_json(expected)
            differing = sorted(key for key in set(manifest) | set(actual) if manifest.get(key) != actual.get(key))
            raise ValueError(f'The image manifest does not describe the image files: {differing} differ.')
    scopes = [manifest['cli']['path']]
    if sdk:
        scopes += [sdk['root'] + '/meerschaum/', sdk['root'] + '/_meerschaum_mrsm.py']
    if manifest.get('compiler'):
        scopes += [manifest['compiler']['policy']['path'], COMPILERS]
    for path in sorted(irregular):
        if any(path == scope or scope.endswith('/') and path.startswith(scope) for scope in scopes):
            raise ValueError(f'Attested Factory material is a link or special file in the image: {path}')
    checked = set()
    for path in sorted(_attested_paths(manifest)):
        name = path[1:]
        if name in linked:
            raise ValueError(f'Attested Factory material is hard-linked: {path}')
        parts = name.split('/')
        for depth in range(1, len(parts) + 1):
            item = '/'.join(parts[:depth])
            if item in checked:
                continue
            checked.add(item)
            header = headers.get(item)
            directory = depth < len(parts)
            if (header is None or not (header.isdir() if directory else header.isreg())
                    or header.mode & 0o022
                    or header.uid == runtime_uid and header.mode & 0o200):
                raise ValueError(f'Attested Factory material is writable or nonregular in the image: /{item}')
    return manifest


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--rootfs', type=Path)
    parser.add_argument('--release')
    parser.add_argument('--architecture', choices=('x86_64', 'aarch64'))
    parser.add_argument('--sdk-root')
    parser.add_argument('--runtime-uid', type=int, default=1000)
    parser.add_argument('--verify-export', metavar='TAR',
                        help='check an image export (`docker export`; `-` for stdin) against its manifest')
    args = parser.parse_args()
    if args.verify_export:
        try:
            if args.verify_export == '-':
                manifest = verify_export(sys.stdin.buffer, args.runtime_uid)
            else:
                with open(args.verify_export, 'rb') as stream:
                    manifest = verify_export(stream, args.runtime_uid)
        except (ValueError, OSError, tarfile.TarError) as error:
            parser.exit(1, f'native Factory manifest: {error}\n')
        parts = [name for name in ('cli', 'sdk', 'compiler') if manifest.get(name)]
        print(f"The image's native Factory manifest matches its files ({', '.join(parts)}).")
        return
    if args.rootfs is None or args.release is None or args.architecture is None:
        parser.error('--rootfs, --release and --architecture are required')
    write(args.rootfs, build(args.rootfs, args.release, args.architecture,
                            args.sdk_root, args.runtime_uid))


if __name__ == '__main__':
    main()
