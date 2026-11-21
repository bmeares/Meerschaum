#! /usr/bin/env python3
# -*- coding: utf-8 -*-
# vim:fenc=utf-8

"""
Dash-free plugin filesystem primitives: safe plugin names, bounded relative paths,
symlink-resistant `dir_fd` / `O_NOFOLLOW` reads and atomic CAS replacements, and
installed plugin discovery.
"""

from __future__ import annotations

import hashlib
import os
import pathlib
import stat
import threading
import uuid

from meerschaum.utils.typing import Any, Dict, Iterable, List, Optional, Tuple

#: Source preview bounds.
MAX_FILE_BYTES: int = 256 * 1024
MAX_ARCHIVE_BYTES: int = 16 * 1024 * 1024
MAX_SCAN_BYTES: int = 32 * 1024 * 1024
MAX_FILES: int = 100
MAX_ENTRIES: int = 1000
MAX_PATH_BYTES: int = 1024
MAX_PATH_PARTS: int = 32
TEXT_SUFFIXES = frozenset({'.py', '.rs', '.toml', '.md', '.txt', '.json', '.yaml', '.yml', '.sql'})
SKIP_PARTS = frozenset({'__pycache__', 'target', 'node_modules', '.git', '.venv', 'venv'})

#: Upload bounds & filtering.
MAX_UPLOAD_BYTES: int = 128 * 1024 * 1024
MAX_UPLOAD_FILES: int = 2000
MAX_ARCHIVE_ENTRIES: int = 20000
UPLOAD_SKIP_PARTS = frozenset({'__pycache__', '.git', '__MACOSX'})
SKIP_NAMES = frozenset({'.DS_Store'})
SKIP_SUFFIXES = ('.pyc', '.pyo')
SINGLE_SUFFIXES = ('.py', '.rs')
ARCHIVE_SUFFIXES = ('.tar.gz', '.tgz', '.zip')
TEMPORARY_SUFFIX: str = '.mrsm-upload'
DEFAULT_PROXY_BYTES: int = 16 * 1024 * 1024

#: One save at a time per process (the digest check and the rename are not atomic together).
_save_lock = threading.Lock()


class SourceChanged(ValueError):
    """The file on disk is not the one the editor loaded."""


class UploadError(ValueError):
    """Why an upload is refused (the message is shown to the uploader)."""


########################################################################################
# Plugin names & relative paths.
########################################################################################

def is_safe_plugin_name(name: Any) -> bool:
    """A plugin name is one path component of ASCII letters, digits, `_` and `-`."""
    name = str(name or '')
    return (
        bool(name)
        and name not in ('.', '..')
        and all(c.isalnum() or c in '_-' for c in name)
        and name.isascii()
    )


def valid_plugin_name(name: Any) -> bool:
    """
    A name the plugins directories list: `is_safe_plugin_name`, not hidden (`_` first),
    and not the reserved `mrsm`.
    """
    name = str(name or '')
    return is_safe_plugin_name(name) and not name.startswith('_') and name != 'mrsm'


def relative_source_path(
    value: Any,
    *,
    max_path_bytes: int = MAX_PATH_BYTES,
    max_path_parts: int = MAX_PATH_PARTS,
) -> pathlib.PurePosixPath:
    """Validate a relative POSIX path for source preview/editing."""
    text = str(value or '')
    if len(text.encode('utf-8')) > max_path_bytes:
        raise ValueError('This source path is too long to preview.')
    path = pathlib.PurePosixPath(text)
    if (
        not text
        or '\\' in text
        or '\0' in text
        or path.is_absolute()
        or len(path.parts) > max_path_parts
        or any(part in ('', '.', '..') for part in text.split('/'))
        or any(part.startswith('.') or part in SKIP_PARTS for part in path.parts)
    ):
        raise ValueError('Invalid source file.')
    return path


def checked_upload_path(
    value: Any,
    *,
    max_path_bytes: int = MAX_PATH_BYTES,
    max_path_parts: int = MAX_PATH_PARTS,
) -> Optional[pathlib.PurePosixPath]:
    """
    The relative path of an uploaded or archived entry, `None` when it is one never written
    (`__pycache__`, `.git`, `*.pyc`). An absolute path, a `..` or `.` component, a backslash, a
    drive or a NUL is refused: nothing may land outside the plugin.
    """
    text = str(value or '')
    if not text or '\0' in text or '\\' in text:
        raise UploadError(f"Invalid path in the upload: {text!r}.")
    if len(text.encode('utf-8', 'surrogateescape')) > max_path_bytes:
        raise UploadError('A path in the upload is too long.')
    if text.startswith('/') or (len(text) > 1 and text[1] == ':'):
        raise UploadError(f"Absolute paths are not allowed in an upload: {text!r}.")
    parts = text.rstrip('/').split('/')
    if any(part in ('', '.', '..') for part in parts):
        raise UploadError(f"Invalid path in the upload: {text!r}.")
    if len(parts) > max_path_parts:
        raise UploadError('A path in the upload is nested too deeply.')
    if any(part in UPLOAD_SKIP_PARTS for part in parts):
        return None
    if parts[-1] in SKIP_NAMES or parts[-1].lower().endswith(SKIP_SUFFIXES):
        return None
    return pathlib.PurePosixPath(*parts)


def is_text_path(path: pathlib.PurePath) -> bool:
    return path.suffix.lower() in TEXT_SUFFIXES or path.name in ('LICENSE', 'COPYING')


def source_order(path: str):
    entrypoint = (
        path == '__init__.py'
        or path.endswith('/__init__.py')
        or path == 'src/lib.rs'
        or path.endswith('/src/lib.rs')
        or ('/' not in path and pathlib.PurePosixPath(path).suffix in ('.py', '.rs'))
    )
    return (not entrypoint, path.count('/'), path)


def source_digest(data: bytes) -> str:
    """The digest a file is loaded and saved against (SHA-256 of its bytes)."""
    return hashlib.sha256(data).hexdigest()


def decode_source_bytes(data: bytes) -> str:
    if b'\0' in data:
        raise ValueError('This file is binary and cannot be previewed.')
    try:
        return data.decode('utf-8')
    except UnicodeDecodeError:
        raise ValueError('This file is not UTF-8 text.') from None


########################################################################################
# Symlink-resistant dir_fd / O_NOFOLLOW reads and atomic CAS writes.
########################################################################################

def read_checked_path(root: pathlib.Path, parts, limit: int) -> bytes:
    """Portable fallback where Python cannot open relative directory descriptors (Windows)."""
    root = root.resolve(strict=True)
    candidate = root
    for part in parts:
        candidate = candidate / part
        info = candidate.lstat()
        if (
            stat.S_ISLNK(info.st_mode)
            or getattr(info, 'st_file_attributes', 0)
            & getattr(stat, 'FILE_ATTRIBUTE_REPARSE_POINT', 0)
        ):
            raise ValueError('Linked source paths cannot be previewed.')
    resolved = candidate.resolve(strict=True)
    resolved.relative_to(root)
    if not stat.S_ISREG(info.st_mode) or info.st_size > limit:
        raise ValueError('This source file cannot be previewed.')
    with candidate.open('rb') as stream:
        opened = os.fstat(stream.fileno())
        if not stat.S_ISREG(opened.st_mode) or opened.st_size > limit:
            raise ValueError('This source file cannot be previewed.')
        data = stream.read(limit + 1)
    after = candidate.lstat()
    if (
        len(data) > limit
        or candidate.resolve(strict=True) != resolved
        or (opened.st_dev, opened.st_ino) != (after.st_dev, after.st_ino)
    ):
        raise ValueError('The source file changed while it was read.')
    return data


def read_file_no_follow(root: pathlib.Path, relative: str, limit: int = MAX_FILE_BYTES) -> bytes:
    """Open each component beneath a trusted root without following symlinks."""
    parts = relative_source_path(relative).parts
    if os.open not in os.supports_dir_fd or not hasattr(os, 'O_NOFOLLOW'):
        return read_checked_path(root, parts, limit)
    directory = os.open(str(root), os.O_RDONLY | os.O_DIRECTORY)
    try:
        for part in parts[:-1]:
            child = os.open(part, os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW, dir_fd=directory)
            os.close(directory)
            directory = child
        fd = os.open(parts[-1], os.O_RDONLY | os.O_NOFOLLOW | os.O_NONBLOCK, dir_fd=directory)
        try:
            info = os.fstat(fd)
            if not stat.S_ISREG(info.st_mode):
                raise ValueError('Only regular source files can be viewed.')
            if info.st_size > limit:
                raise ValueError(f'This file exceeds the {limit // 1024} KiB preview limit.')
            with os.fdopen(fd, 'rb', closefd=False) as stream:
                data = stream.read(limit + 1)
            if len(data) > limit:
                raise ValueError('This source file is too large to preview.')
            return data
        finally:
            os.close(fd)
    finally:
        os.close(directory)


def open_parent_no_follow(root: pathlib.Path, parts) -> int:
    """A descriptor of the file's directory, every component opened without following links."""
    directory = os.open(str(root), os.O_RDONLY | os.O_DIRECTORY)
    try:
        for part in parts[:-1]:
            child = os.open(part, os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW, dir_fd=directory)
            os.close(directory)
            directory = child
    except BaseException:
        os.close(directory)
        raise
    return directory


def read_at_no_follow(directory: int, name: str, limit: int = MAX_FILE_BYTES) -> Tuple[bytes, int]:
    """The bytes and permission bits of the regular file `name` in `directory` (no links)."""
    fd = os.open(name, os.O_RDONLY | os.O_NOFOLLOW | os.O_NONBLOCK, dir_fd=directory)
    try:
        info = os.fstat(fd)
        if not stat.S_ISREG(info.st_mode):
            raise ValueError('Only regular source files can be edited.')
        if info.st_size > limit:
            raise ValueError(f'This file exceeds the {limit // 1024} KiB limit.')
        with os.fdopen(fd, 'rb', closefd=False) as stream:
            data = stream.read(limit + 1)
        if len(data) > limit:
            raise ValueError(f'This file exceeds the {limit // 1024} KiB limit.')
        return data, stat.S_IMODE(info.st_mode)
    finally:
        os.close(fd)


def replace_checked_path(
    root: pathlib.Path,
    parts,
    data: bytes,
    expected: str,
    limit: int = MAX_FILE_BYTES,
) -> None:
    """Portable fallback where Python cannot open relative directory descriptors (Windows)."""
    root = root.resolve(strict=True)
    candidate = root
    for part in parts:
        candidate = candidate / part
        info = candidate.lstat()
        if (
            stat.S_ISLNK(info.st_mode)
            or getattr(info, 'st_file_attributes', 0)
            & getattr(stat, 'FILE_ATTRIBUTE_REPARSE_POINT', 0)
        ):
            raise ValueError('Linked source paths cannot be edited.')
    candidate.resolve(strict=True).relative_to(root)
    if not stat.S_ISREG(info.st_mode) or info.st_size > limit:
        raise ValueError('Only regular source files can be edited.')
    if source_digest(candidate.read_bytes()) != expected:
        raise SourceChanged(candidate.name)
    temporary = candidate.with_name(f'.{candidate.name}.{uuid.uuid4().hex}.mrsm-edit')
    try:
        with open(temporary, 'xb') as stream:
            stream.write(data)
            stream.flush()
            os.fsync(stream.fileno())
        if source_digest(candidate.read_bytes()) != expected:
            raise SourceChanged(candidate.name)
        os.replace(temporary, candidate)
    except BaseException:
        try:
            temporary.unlink()
        except OSError:
            pass
        raise


def replace_file_cas(
    root: pathlib.Path,
    relative: str,
    data: bytes,
    expected: str,
    limit: int = MAX_FILE_BYTES,
) -> None:
    """
    Replace `root/relative` with `data` if its bytes still hash to `expected`: a temporary file
    beside it (hidden, created exclusively, never through a link) is written, flushed and
    renamed over it, and the directory is flushed. Every directory is opened without following
    links, so nothing outside `root` can be reached.
    """
    parts = relative_source_path(relative).parts
    if os.open not in os.supports_dir_fd or not hasattr(os, 'O_NOFOLLOW'):
        return replace_checked_path(root, parts, data, expected, limit=limit)
    directory = open_parent_no_follow(root, parts)
    name = parts[-1]
    temporary = f'.{name}.{uuid.uuid4().hex}.mrsm-edit'
    try:
        current, mode = read_at_no_follow(directory, name, limit=limit)
        if source_digest(current) != expected:
            raise SourceChanged(name)
        fd = os.open(
            temporary,
            os.O_WRONLY | os.O_CREAT | os.O_EXCL | os.O_NOFOLLOW,
            mode or 0o644,
            dir_fd=directory,
        )
        try:
            with os.fdopen(fd, 'wb', closefd=False) as stream:
                stream.write(data)
                stream.flush()
                os.fsync(fd)
        finally:
            os.close(fd)
        try:
            ### Checked again just before the rename: a change while writing is refused too.
            if source_digest(read_at_no_follow(directory, name, limit=limit)[0]) != expected:
                raise SourceChanged(name)
            os.replace(temporary, name, src_dir_fd=directory, dst_dir_fd=directory)
        except BaseException:
            try:
                os.unlink(temporary, dir_fd=directory)
            except OSError:
                pass
            raise
        try:
            os.fsync(directory)
        except OSError:
            pass
    finally:
        os.close(directory)


########################################################################################
# Plugin filesystem discovery.
########################################################################################

def _plugin_directories(plugins_dirs: Optional[Iterable[Any]] = None) -> List[pathlib.Path]:
    if plugins_dirs is not None:
        return [pathlib.Path(d) for d in plugins_dirs]
    import meerschaum.config.paths as paths
    return [pathlib.Path(d) for d in paths.PLUGINS_DIR_PATHS]


def plugin_paths(directory: pathlib.Path, name: str) -> List[pathlib.Path]:
    """Where a plugin `name` may live in a plugins directory (package, `.py`, `.rs`)."""
    return [directory / name, directory / (name + '.py'), directory / (name + '.rs')]


def existing_plugin_paths(directory: pathlib.Path, name: str) -> List[pathlib.Path]:
    """The entries of `plugin_paths()` that exist (links included)."""
    return [path for path in plugin_paths(directory, name) if os.path.lexists(path)]


def discover_plugin_files(plugins_dirs: Optional[Iterable[Any]] = None) -> Dict[str, Dict[str, Any]]:
    """
    The plugins in the plugins directories, first directory first: `{name: {kind, layout, path}}`
    (`kind` is `Python` or `Rust`; `layout` is `file`, `package` or `directory`). A Rust plugin
    is a `<name>.rs` file or a directory with a `plugin.toml`, and replaces a Python plugin of
    the same name.
    """
    found: Dict[str, Dict[str, Any]] = {}
    for directory in _plugin_directories(plugins_dirs):
        try:
            entries = sorted(directory.iterdir(), key=lambda p: p.name)
        except OSError:
            continue
        for entry in entries:
            name = entry.name
            if name.startswith(('.', '_')):
                continue
            if entry.is_dir():
                if (entry / 'plugin.toml').exists():
                    info = {'kind': 'Rust', 'layout': 'directory', 'path': str(entry)}
                elif (entry / '__init__.py').exists():
                    info = {'kind': 'Python', 'layout': 'package', 'path': str(entry)}
                else:
                    continue
            elif entry.suffix == '.py':
                name = entry.stem
                info = {'kind': 'Python', 'layout': 'file', 'path': str(entry)}
            elif entry.suffix == '.rs':
                name = entry.stem
                info = {'kind': 'Rust', 'layout': 'file', 'path': str(entry)}
            else:
                continue
            existing = found.get(name)
            if existing is None or (existing['kind'] == 'Python' and info['kind'] == 'Rust'):
                found[name] = info
    return found


def installed_plugin_location(
    name: str,
    plugins_dirs: Optional[Iterable[Any]] = None,
) -> Tuple[pathlib.Path, pathlib.Path, Dict[str, Any]]:
    info = discover_plugin_files(plugins_dirs).get(name)
    if not info:
        raise ValueError('This plugin is not installed.')
    candidate = pathlib.Path(info['path'])
    roots = [root.resolve() for root in _plugin_directories(plugins_dirs)]
    root = next((root for root in roots if candidate.parent.resolve() == root), None)
    if root is None or candidate.is_symlink() or candidate.resolve().parent != root:
        raise ValueError('Plugin source is outside the installed plugin directories.')
    return root, candidate, info


def installed_source_files(
    name: str,
    plugins_dirs: Optional[Iterable[Any]] = None,
    *,
    max_entries: int = MAX_ENTRIES,
    max_files: int = MAX_FILES,
) -> List[str]:
    root, candidate, info = installed_plugin_location(name, plugins_dirs)
    if info['layout'] == 'file':
        return [candidate.name]
    found, scanned = [], 0
    for directory, dirs, files in os.walk(candidate, followlinks=False):
        scanned += len(dirs)
        if scanned > max_entries:
            raise ValueError('This plugin contains too many entries to preview.')
        dirs[:] = sorted(
            part for part in dirs
            if not part.startswith('.')
            and part not in SKIP_PARTS
            and not (pathlib.Path(directory) / part).is_symlink()
        )
        for file in sorted(files):
            scanned += 1
            if scanned > max_entries:
                raise ValueError('This plugin contains too many files to preview.')
            path = pathlib.Path(directory) / file
            relative = path.relative_to(candidate)
            if path.is_symlink() or not is_text_path(relative):
                continue
            try:
                relative_source_path(relative.as_posix())
            except ValueError:
                continue
            found.append(relative.as_posix())
            if len(found) > max_files:
                raise ValueError('This plugin contains too many source files to preview.')
    return sorted(found, key=source_order)


def installed_source_target(
    name: str,
    selected: str,
    plugins_dirs: Optional[Iterable[Any]] = None,
    *,
    max_entries: int = MAX_ENTRIES,
    max_files: int = MAX_FILES,
) -> Tuple[pathlib.Path, str]:
    """`(root, path of the file relative to root)` of a listed file of an installed plugin."""
    relative_source_path(selected)
    if selected not in installed_source_files(
        name, plugins_dirs, max_entries=max_entries, max_files=max_files,
    ):
        raise ValueError('This source file is not available.')
    root, candidate, info = installed_plugin_location(name, plugins_dirs)
    relative = candidate.name if info['layout'] == 'file' else candidate.name + '/' + selected
    return root, relative
