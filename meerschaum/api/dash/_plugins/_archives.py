#! /usr/bin/env python3
# -*- coding: utf-8 -*-
# vim:fenc=utf-8

"""
Dash-free bounded archive reading (for repository source preview) and upload
decoding, validation, planning, and atomic directory/file installation.
"""

from __future__ import annotations

import base64
import binascii
import gzip
import io
import os
import pathlib
import shutil
import stat
import tarfile
import uuid
import zipfile
import zlib

from meerschaum.utils.typing import Any, Dict, List, Optional, Tuple
from meerschaum.api.dash._plugins._fs import (
    ARCHIVE_SUFFIXES,
    DEFAULT_PROXY_BYTES,
    MAX_ARCHIVE_BYTES,
    MAX_ARCHIVE_ENTRIES,
    MAX_ENTRIES,
    MAX_FILE_BYTES,
    MAX_FILES,
    MAX_SCAN_BYTES,
    MAX_UPLOAD_BYTES,
    MAX_UPLOAD_FILES,
    SINGLE_SUFFIXES,
    TEMPORARY_SUFFIX,
    UploadError,
    checked_upload_path,
    existing_plugin_paths,
    is_text_path,
    read_file_no_follow,
    relative_source_path,
    source_order,
    valid_plugin_name,
)


########################################################################################
# Shared bounded gzip expansion + repository archive inspection.
########################################################################################

def _decompress_gzip_bounded(
    data: bytes,
    max_bytes: int,
    *,
    error_cls: type[Exception],
    too_large_message: str,
    corrupt_message: Optional[str] = None,
) -> bytes:
    """Decompress gzip bytes with a hard ceiling before tarfile reads any headers."""
    try:
        with gzip.GzipFile(fileobj=io.BytesIO(data)) as compressed:
            expanded = compressed.read(max_bytes + 1)
    except (OSError, EOFError, zlib.error):
        if corrupt_message is not None:
            raise error_cls(corrupt_message) from None
        raise
    if len(expanded) > max_bytes:
        raise error_cls(too_large_message)
    return expanded


def open_repository_archive(
    name: str,
    *,
    archives_root: Optional[pathlib.Path] = None,
    max_archive_bytes: int = MAX_ARCHIVE_BYTES,
    max_scan_bytes: int = MAX_SCAN_BYTES,
) -> tarfile.TarFile:
    """Open only the named repository archive beneath the configured archive root."""
    if archives_root is None:
        from meerschaum.config import paths
        root = pathlib.Path(paths.PLUGINS_ARCHIVES_RESOURCES_PATH).resolve()
    else:
        root = pathlib.Path(archives_root).resolve()
    data = read_file_no_follow(root, name + '.tar.gz', max_archive_bytes)
    expanded = _decompress_gzip_bounded(
        data,
        max_scan_bytes,
        error_cls=ValueError,
        too_large_message='This archive is too large to preview safely.',
    )
    return tarfile.open(fileobj=io.BytesIO(expanded), mode='r:')


def repository_source_files(
    name: str,
    selected: str = '',
    *,
    archives_root: Optional[pathlib.Path] = None,
    max_archive_bytes: int = MAX_ARCHIVE_BYTES,
    max_scan_bytes: int = MAX_SCAN_BYTES,
    max_entries: int = MAX_ENTRIES,
    max_files: int = MAX_FILES,
    max_file_bytes: int = MAX_FILE_BYTES,
) -> Tuple[List[str], Optional[bytes]]:
    files: List[str] = []
    result: Optional[bytes] = None
    scanned_bytes = 0
    with open_repository_archive(
        name,
        archives_root=archives_root,
        max_archive_bytes=max_archive_bytes,
        max_scan_bytes=max_scan_bytes,
    ) as archive:
        for count, member in enumerate(archive):
            if count >= max_entries:
                raise ValueError('This archive contains too many entries to preview.')
            scanned_bytes += max(0, member.size) + 512
            if scanned_bytes > max_scan_bytes:
                raise ValueError('This archive is too large to preview safely.')
            try:
                relative = relative_source_path(member.name)
            except ValueError:
                continue
            if not member.isfile() or not is_text_path(relative):
                continue
            if member.size > max_file_bytes:
                continue
            files.append(relative.as_posix())
            if len(files) > max_files:
                raise ValueError('This archive contains too many source files to preview.')
            if member.name == selected:
                stream = archive.extractfile(member)
                result = stream.read(max_file_bytes + 1) if stream else None
    if len(files) != len(set(files)):
        raise ValueError('This archive contains ambiguous duplicate source files.')
    return sorted(files, key=source_order), result


########################################################################################
# Upload decoding, archive unpacking, planning, and atomic installation.
########################################################################################

def upload_limit_bytes() -> int:
    """
    The largest upload the browser is offered: `MAX_UPLOAD_BYTES`, or less when the server's
    limit for one request to the console (`api:limits:proxy_bytes`, which carries the files as
    Base64) is smaller.
    """
    try:
        from meerschaum.config import get_config
        limits = get_config('api', 'limits', warn=False)
    except Exception:
        limits = None
    ### An explicit `null` is no limit; a missing key is the server's default.
    proxy = (
        limits.get('proxy_bytes', DEFAULT_PROXY_BYTES)
        if isinstance(limits, dict)
        else DEFAULT_PROXY_BYTES
    )
    if proxy is None:
        return MAX_UPLOAD_BYTES
    try:
        proxy = int(proxy)
    except (TypeError, ValueError):
        proxy = DEFAULT_PROXY_BYTES
    return max(64 * 1024, min(MAX_UPLOAD_BYTES, ((proxy - 64 * 1024) * 3) // 4))


def size_label(size: int) -> str:
    """`11 MiB`, `512 KiB`."""
    if size >= 1024 * 1024:
        return f"{size // (1024 * 1024)} MiB"
    return f"{max(1, size // 1024)} KiB"


def decode_contents(contents: Any, limit: int = MAX_UPLOAD_BYTES) -> bytes:
    """
    The bytes of one `dcc.Upload` file (`data:<type>;base64,<data>`), refused before decoding
    when they would exceed `limit`.
    """
    if not isinstance(contents, str) or ',' not in contents:
        raise UploadError('The upload could not be read.')
    header, encoded = contents.split(',', 1)
    if not header.startswith('data:') or not header.endswith(';base64'):
        raise UploadError('The upload could not be read.')
    if (len(encoded) * 3) // 4 > limit + 2:
        raise UploadError(f"Uploads are limited to {size_label(limit)}.")
    try:
        return base64.b64decode(encoded, validate=True)
    except (binascii.Error, ValueError):
        raise UploadError('The upload could not be read.') from None


def decode_uploads(
    contents: Any,
    filenames: Any,
    limit: int = MAX_UPLOAD_BYTES,
    *,
    max_files: Optional[int] = None,
) -> List[Tuple[str, bytes]]:
    """`[(filename, bytes)]` of a `dcc.Upload`'s `contents` and `filename` (one file or several)."""
    file_limit = MAX_UPLOAD_FILES if max_files is None else max_files
    if isinstance(contents, str):
        contents, filenames = [contents], [filenames]
    if not isinstance(contents, (list, tuple)) or not contents:
        raise UploadError('Choose the files to upload.')
    if not isinstance(filenames, (list, tuple)) or len(filenames) != len(contents):
        raise UploadError('The upload could not be read.')
    if len(contents) > file_limit:
        raise UploadError(f"Uploads are limited to {file_limit:,} files.")
    ### Refused before anything is decoded.
    encoded = sum(len(content) for content in contents if isinstance(content, str))
    if (encoded * 3) // 4 > limit + 2 * len(contents) + 64 * len(contents):
        raise UploadError(f"Uploads are limited to {size_label(limit)}.")
    files = [
        (str(filename or ''), decode_contents(content, limit))
        for filename, content in zip(filenames, contents)
    ]
    if sum(len(data) for _, data in files) > limit:
        raise UploadError(f"Uploads are limited to {size_label(limit)}.")
    return files


def archive_suffix(filename: str) -> Optional[str]:
    """`.zip`, `.tar.gz` or `.tgz` when `filename` names an archive."""
    lowered = str(filename or '').lower()
    return next((suffix for suffix in ARCHIVE_SUFFIXES if lowered.endswith(suffix)), None)


def name_from_filename(filename: str) -> str:
    """The plugin name a file suggests: its name without `.py`, `.rs` or the archive suffix."""
    base = str(filename or '').replace('\\', '/').rsplit('/', 1)[-1]
    suffix = archive_suffix(base) or next(
        (suffix for suffix in SINGLE_SUFFIXES if base.lower().endswith(suffix)), None,
    )
    return base[:-len(suffix)] if suffix else base


def suggested_name(filenames: Any) -> Optional[str]:
    """The name one uploaded file suggests (`None` for several files, or an unsafe name)."""
    if isinstance(filenames, str):
        filenames = [filenames]
    if not isinstance(filenames, (list, tuple)) or len(filenames) != 1:
        return None
    name = name_from_filename(filenames[0])
    return name if valid_plugin_name(name) else None


class _Budget:
    """The bytes and files an upload may still add."""

    def __init__(self, limit: int = MAX_UPLOAD_BYTES, files: Optional[int] = None):
        self.limit = limit
        self.remaining = limit
        self.max_files = MAX_UPLOAD_FILES if files is None else files
        self.files = self.max_files

    def add(self, size: int) -> None:
        self.remaining -= size
        self.files -= 1
        if self.remaining < 0:
            raise UploadError(f"Uploads are limited to {size_label(self.limit)} once unpacked.")
        if self.files < 0:
            raise UploadError(f"Uploads are limited to {self.max_files:,} files.")


def _add(files: Dict[str, bytes], path: pathlib.PurePosixPath, data: bytes, budget: _Budget) -> None:
    key = path.as_posix()
    if key in files:
        raise UploadError(f"The upload contains '{key}' more than once.")
    budget.add(len(data))
    files[key] = data


def _zip_files(
    data: bytes,
    budget: _Budget,
    *,
    max_entries: int = MAX_ARCHIVE_ENTRIES,
) -> Dict[str, bytes]:
    files: Dict[str, bytes] = {}
    try:
        archive = zipfile.ZipFile(io.BytesIO(data))
    except (zipfile.BadZipFile, ValueError, OSError):
        raise UploadError('This zip archive could not be read.') from None
    with archive:
        entries = archive.infolist()
        if len(entries) > max_entries:
            raise UploadError('This archive lists too many entries.')
        for info in entries:
            path = checked_upload_path(info.filename)
            mode = info.external_attr >> 16
            kind = stat.S_IFMT(mode)
            if info.is_dir() or kind == stat.S_IFDIR:
                continue
            if path is None:
                continue
            if kind and kind != stat.S_IFREG:
                raise UploadError(
                    f"'{info.filename}' is a link or a special file: archives may only hold "
                    "regular files."
                )
            if info.flag_bits & 0x1:
                raise UploadError('Encrypted archives cannot be uploaded.')
            if info.file_size > budget.remaining:
                budget.add(info.file_size)
            try:
                with archive.open(info) as stream:
                    content = stream.read(budget.remaining + 1)
            except UploadError:
                raise
            except Exception:
                raise UploadError(f"'{info.filename}' could not be read from the archive.") from None
            _add(files, path, content, budget)
    return files


def _tar_files(
    data: bytes,
    budget: _Budget,
    *,
    max_entries: int = MAX_ARCHIVE_ENTRIES,
) -> Dict[str, bytes]:
    expanded = _decompress_gzip_bounded(
        data,
        2 * budget.limit,
        error_cls=UploadError,
        too_large_message=f"Uploads are limited to {size_label(budget.limit)} once unpacked.",
        corrupt_message='This archive could not be read.',
    )
    files: Dict[str, bytes] = {}
    try:
        archive = tarfile.open(fileobj=io.BytesIO(expanded), mode='r:')
    except (tarfile.TarError, OSError, EOFError):
        raise UploadError('This archive could not be read.') from None
    with archive:
        try:
            for count, member in enumerate(archive):
                if count >= max_entries:
                    raise UploadError('This archive lists too many entries.')
                name = member.name
                ### `./name` is how `tar -C dir .` spells its members.
                while name.startswith('./'):
                    name = name[2:]
                if name in ('', '.'):
                    continue
                path = checked_upload_path(name)
                if member.isdir():
                    continue
                if path is None:
                    continue
                if not member.isreg():
                    raise UploadError(
                        f"'{member.name}' is a link or a special file: archives may only hold "
                        "regular files."
                    )
                if member.size > budget.remaining:
                    budget.add(member.size)
                stream = archive.extractfile(member)
                content = stream.read(budget.remaining + 1) if stream is not None else b''
                _add(files, path, content, budget)
        except tarfile.TarError:
            raise UploadError('This archive could not be read.') from None
    return files


def archive_files(
    filename: str,
    data: bytes,
    budget: Optional[_Budget] = None,
    *,
    max_files: Optional[int] = None,
    max_entries: int = MAX_ARCHIVE_ENTRIES,
) -> Dict[str, bytes]:
    """
    `{relative path: bytes}` of the regular files of a `.zip`, `.tar.gz` or `.tgz` archive, read
    in memory: unsafe paths, links, devices, encrypted entries and anything past the size and
    count limits are refused; caches and `.git` are left out.
    """
    budget = budget or _Budget(files=max_files)
    suffix = archive_suffix(filename)
    if suffix == '.zip':
        return _zip_files(data, budget, max_entries=max_entries)
    if suffix in ('.tar.gz', '.tgz'):
        return _tar_files(data, budget, max_entries=max_entries)
    raise UploadError('Upload a .zip, .tar.gz or .tgz archive.')


def _check_tree(files: Dict[str, bytes]) -> None:
    """No path is both a file and a directory of another file."""
    for key in files:
        parts = key.split('/')
        for depth in range(1, len(parts)):
            if '/'.join(parts[:depth]) in files:
                raise UploadError(
                    f"'{'/'.join(parts[:depth])}' is both a file and a directory in the upload."
                )


def _checked_name(name: Any, what: str = 'plugin name') -> str:
    name = str(name or '').strip()
    if not valid_plugin_name(name):
        raise UploadError(f"Invalid {what}: '{name}' (use letters, digits, '_' and '-').")
    return name


def _plan_single(name: Any, filename: str, data: bytes) -> Dict[str, Any]:
    suffix = pathlib.PurePosixPath(filename).suffix.lower()
    name = _checked_name(name or filename[:-len(suffix)])
    if suffix == '.rs' and not name[0].isalpha():
        raise UploadError(f"Rust plugin names must start with a letter (got '{name}').")
    return {'name': name, 'layout': 'file', 'suffix': suffix, 'files': {name + suffix: data}}


def _plan_directory(name: Any, files: Dict[str, bytes]) -> Dict[str, Any]:
    name = _checked_name(name)
    _check_tree(files)
    if 'plugin.toml' not in files and '__init__.py' not in files:
        raise UploadError(
            "A plugin package needs an __init__.py (Python) or a plugin.toml (Rust) at its top level."
        )
    return {'name': name, 'layout': 'directory', 'suffix': '', 'files': dict(files)}


def plan_upload(
    uploads: List[Tuple[str, bytes]],
    name: Optional[str] = None,
    limit: int = MAX_UPLOAD_BYTES,
    *,
    max_files: Optional[int] = None,
) -> Dict[str, Any]:
    """
    What an upload installs: `{name, layout ('file' | 'directory'), suffix, files: {path: bytes}}`
    (paths relative to the plugins directory for a single file, to the plugin's directory
    otherwise). Raises `UploadError` with the reason when it is refused.
    """
    file_limit = MAX_UPLOAD_FILES if max_files is None else max_files
    name = str(name or '').strip() or None
    if not uploads:
        raise UploadError('Choose the files to upload.')
    if len(uploads) > file_limit:
        raise UploadError(f"Uploads are limited to {file_limit:,} files.")
    if sum(len(data) for _, data in uploads) > limit:
        raise UploadError(f"Uploads are limited to {size_label(limit)}.")
    budget = _Budget(limit, files=file_limit)
    archives = [filename for filename, _ in uploads if archive_suffix(filename)]
    if archives and len(uploads) > 1:
        raise UploadError('Upload one archive on its own, or loose files without an archive.')

    if archives:
        filename, data = uploads[0]
        files = archive_files(filename, data, budget)
        if not files:
            raise UploadError('This archive holds no files to install.')
        tops = {key.split('/', 1)[0] for key in files}
        nested = all('/' in key for key in files)
        if nested and len(tops) == 1:
            top = next(iter(tops))
            prefix = top + '/'
            return _plan_directory(
                name or top,
                {key[len(prefix):]: data for key, data in files.items()},
            )
        if len(files) == 1:
            only = next(iter(files))
            if only.lower().endswith(SINGLE_SUFFIXES):
                return _plan_single(name, only, files[only])
        return _plan_directory(name or name_from_filename(filename), files)

    files: Dict[str, bytes] = {}
    for filename, data in uploads:
        ### A browser sends bare file names; anything else is refused like an archive's paths.
        path = checked_upload_path(filename)
        if path is None:
            continue
        if len(path.parts) != 1:
            raise UploadError(f"Invalid file name in the upload: {filename!r}.")
        _add(files, path, data, budget)
    if not files:
        raise UploadError('Choose the files to upload.')
    if len(files) == 1:
        only = next(iter(files))
        if only.lower().endswith(SINGLE_SUFFIXES):
            return _plan_single(name, only, files[only])
        raise UploadError(
            'Upload a .py or .rs file, a .zip or .tar.gz archive, or the files of a package.'
        )
    if not name:
        raise UploadError('Enter the name of the plugin these files make.')
    return _plan_directory(name, files)


def _remove(path: pathlib.Path) -> None:
    try:
        if path.is_dir() and not path.is_symlink():
            shutil.rmtree(path, ignore_errors=True)
        else:
            path.unlink()
    except OSError:
        pass


def _write_tree(root: pathlib.Path, files: Dict[str, bytes]) -> None:
    """Write `files` beneath the new directory `root` (every path checked again, no overwrite)."""
    resolved_root = root.resolve(strict=True)
    for key, data in sorted(files.items()):
        relative = checked_upload_path(key)
        if relative is None:
            continue
        target = root.joinpath(*relative.parts)
        target.parent.mkdir(parents=True, exist_ok=True)
        try:
            target.parent.resolve(strict=True).relative_to(resolved_root)
        except ValueError:
            raise UploadError(f"'{key}' would be written outside the plugin.") from None
        with open(target, 'xb') as stream:
            stream.write(data)


def write_plugin(plan: Dict[str, Any], directory: Any, replace: bool = False) -> pathlib.Path:
    """
    Install a `plan_upload()` plan into the plugins directory `directory`: written to a hidden
    temporary entry there, then renamed into place. A plugin of that name (a package, a `.py` or
    a `.rs`) is refused unless `replace`; then the old one is moved aside, the new one renamed
    in, and the old one removed (and put back if the rename fails). Returns the plugin's path.
    """
    name = plan['name']
    if not valid_plugin_name(name):
        raise UploadError(f"Invalid plugin name: '{name}' (use letters, digits, '_' and '-').")
    directory = pathlib.Path(directory)
    if not directory.is_dir():
        raise UploadError('The plugins directory does not exist.')
    existing = existing_plugin_paths(directory, name)
    if existing and not replace:
        raise UploadError(f"Plugin '{name}' already exists.")
    if any(path.is_symlink() for path in existing):
        raise UploadError(f"Plugin '{name}' is a link and cannot be replaced by an upload.")

    token = uuid.uuid4().hex
    final = directory / (name + plan.get('suffix', '') if plan['layout'] == 'file' else name)
    temporary = directory / f'.{name}.{token}{TEMPORARY_SUFFIX}'
    moved: List[Tuple[pathlib.Path, pathlib.Path]] = []
    try:
        if plan['layout'] == 'file':
            (data,) = plan['files'].values()
            with open(temporary, 'xb') as stream:
                stream.write(data)
                stream.flush()
                os.fsync(stream.fileno())
        else:
            temporary.mkdir()
            _write_tree(temporary, plan['files'])
        for index, path in enumerate(existing):
            backup = directory / f'.{name}.{token}.{index}.old{TEMPORARY_SUFFIX}'
            os.rename(path, backup)
            moved.append((path, backup))
        if os.path.lexists(final):
            raise UploadError(f"Plugin '{name}' already exists.")
        os.rename(temporary, final)
    except BaseException as e:
        for path, backup in reversed(moved):
            try:
                if not os.path.lexists(path):
                    os.rename(backup, path)
            except OSError:
                pass
        _remove(temporary)
        if isinstance(e, UploadError):
            raise
        if isinstance(e, OSError):
            raise UploadError(f"Failed to write plugin '{name}': {e.strerror or e}") from None
        raise
    for _, backup in moved:
        _remove(backup)
    return final
