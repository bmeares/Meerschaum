#! /usr/bin/env python3
# -*- coding: utf-8 -*-
# vim:fenc=utf-8

"""
Uploading a plugin's files from the console (New plugin > Upload files): one `.py` or `.rs`
file, a `.zip` / `.tar.gz` / `.tgz` archive, or several loose files that become a package.

The checks are pure functions over bytes (`plan_upload`, `write_plugin`): nothing is imported or
executed, an archive is never extracted by a library call, and every path is validated before a
byte is written. A plugin is written to a hidden temporary entry inside its plugins directory and
renamed into place. `upload_plugin` is the console's entry: it applies the authoring policy
(`plugin_sources.authoring_refusal`, read fresh) and records the plugin for the uploader's team.
"""

from __future__ import annotations

import base64
import binascii
import io
import os
import pathlib
import shutil
import stat
import uuid

from meerschaum.utils.typing import Any, Dict, List, Optional, Tuple

#: The decoded size an upload may have in all (the server's plugin archive limit), and what it
#: may expand to.
MAX_UPLOAD_BYTES: int = 128 * 1024 * 1024

#: The files an upload may hold (and the entries an archive may list).
MAX_UPLOAD_FILES: int = 2000
MAX_ARCHIVE_ENTRIES: int = 20000

MAX_PATH_BYTES: int = 1024
MAX_PATH_PARTS: int = 32

#: Never written: caches, version control and what archive tools add.
SKIP_PARTS = frozenset({'__pycache__', '.git', '__MACOSX'})
SKIP_NAMES = frozenset({'.DS_Store'})
SKIP_SUFFIXES = ('.pyc', '.pyo')

SINGLE_SUFFIXES = ('.py', '.rs')
ARCHIVE_SUFFIXES = ('.tar.gz', '.tgz', '.zip')

#: The suffix of the hidden entries an upload is written to (and an old plugin is moved to)
#: before the rename; the plugins directories never list hidden entries.
TEMPORARY_SUFFIX: str = '.mrsm-upload'

#: The proxy's default limit for one request to the console (`api:limits:proxy_bytes`).
DEFAULT_PROXY_BYTES: int = 16 * 1024 * 1024


class UploadError(ValueError):
    """Why an upload is refused (the message is shown to the uploader)."""


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
    proxy = limits.get('proxy_bytes', DEFAULT_PROXY_BYTES) if isinstance(limits, dict) else DEFAULT_PROXY_BYTES
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


########################################################################################
# Reading what was uploaded.
########################################################################################

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


def decode_uploads(contents: Any, filenames: Any, limit: int = MAX_UPLOAD_BYTES) -> List[Tuple[str, bytes]]:
    """`[(filename, bytes)]` of a `dcc.Upload`'s `contents` and `filename` (one file or several)."""
    if isinstance(contents, str):
        contents, filenames = [contents], [filenames]
    if not isinstance(contents, (list, tuple)) or not contents:
        raise UploadError('Choose the files to upload.')
    if not isinstance(filenames, (list, tuple)) or len(filenames) != len(contents):
        raise UploadError('The upload could not be read.')
    if len(contents) > MAX_UPLOAD_FILES:
        raise UploadError(f"Uploads are limited to {MAX_UPLOAD_FILES:,} files.")
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


def valid_plugin_name(name: Any) -> bool:
    """
    A name the plugins directories list: `plugins.is_safe_plugin_name` (one path component of
    ASCII letters, digits, `_` and `-`), not hidden (`_` first) and not the reserved `mrsm`.
    """
    from meerschaum.api.dash.plugins import is_safe_plugin_name
    name = str(name or '')
    return is_safe_plugin_name(name) and not name.startswith('_') and name != 'mrsm'


def checked_path(value: Any) -> Optional[pathlib.PurePosixPath]:
    """
    The relative path of an uploaded or archived entry, `None` when it is one never written
    (`__pycache__`, `.git`, `*.pyc`). An absolute path, a `..` or `.` component, a backslash, a
    drive or a NUL is refused: nothing may land outside the plugin.
    """
    text = str(value or '')
    if not text or '\0' in text or '\\' in text:
        raise UploadError(f"Invalid path in the upload: {text!r}.")
    if len(text.encode('utf-8', 'surrogateescape')) > MAX_PATH_BYTES:
        raise UploadError('A path in the upload is too long.')
    if text.startswith('/') or (len(text) > 1 and text[1] == ':'):
        raise UploadError(f"Absolute paths are not allowed in an upload: {text!r}.")
    parts = text.rstrip('/').split('/')
    if any(part in ('', '.', '..') for part in parts):
        raise UploadError(f"Invalid path in the upload: {text!r}.")
    if len(parts) > MAX_PATH_PARTS:
        raise UploadError('A path in the upload is nested too deeply.')
    if any(part in SKIP_PARTS for part in parts):
        return None
    if parts[-1] in SKIP_NAMES or parts[-1].lower().endswith(SKIP_SUFFIXES):
        return None
    return pathlib.PurePosixPath(*parts)


class _Budget:
    """The bytes and files an upload may still add."""

    def __init__(self, limit: int = MAX_UPLOAD_BYTES, files: Optional[int] = None):
        self.limit, self.remaining = limit, limit
        self.files = MAX_UPLOAD_FILES if files is None else files

    def add(self, size: int) -> None:
        self.remaining -= size
        self.files -= 1
        if self.remaining < 0:
            raise UploadError(f"Uploads are limited to {size_label(self.limit)} once unpacked.")
        if self.files < 0:
            raise UploadError(f"Uploads are limited to {MAX_UPLOAD_FILES:,} files.")


def _add(files: Dict[str, bytes], path: pathlib.PurePosixPath, data: bytes, budget: _Budget) -> None:
    key = path.as_posix()
    if key in files:
        raise UploadError(f"The upload contains '{key}' more than once.")
    budget.add(len(data))
    files[key] = data


def _zip_files(data: bytes, budget: _Budget) -> Dict[str, bytes]:
    import zipfile
    files: Dict[str, bytes] = {}
    try:
        archive = zipfile.ZipFile(io.BytesIO(data))
    except (zipfile.BadZipFile, ValueError, OSError):
        raise UploadError('This zip archive could not be read.') from None
    with archive:
        entries = archive.infolist()
        if len(entries) > MAX_ARCHIVE_ENTRIES:
            raise UploadError('This archive lists too many entries.')
        for info in entries:
            path = checked_path(info.filename)
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


def _tar_files(data: bytes, budget: _Budget) -> Dict[str, bytes]:
    import gzip
    import tarfile
    import zlib
    ### Expansion is bounded before `tarfile` reads a header (long names and PAX records too).
    try:
        with gzip.GzipFile(fileobj=io.BytesIO(data)) as compressed:
            expanded = compressed.read(2 * budget.limit + 1)
    except (OSError, EOFError, zlib.error):
        raise UploadError('This archive could not be read.') from None
    if len(expanded) > 2 * budget.limit:
        raise UploadError(f"Uploads are limited to {size_label(budget.limit)} once unpacked.")
    files: Dict[str, bytes] = {}
    try:
        archive = tarfile.open(fileobj=io.BytesIO(expanded), mode='r:')
    except (tarfile.TarError, OSError, EOFError):
        raise UploadError('This archive could not be read.') from None
    with archive:
        try:
            for count, member in enumerate(archive):
                if count >= MAX_ARCHIVE_ENTRIES:
                    raise UploadError('This archive lists too many entries.')
                name = member.name
                ### `./name` is how `tar -C dir .` spells its members.
                while name.startswith('./'):
                    name = name[2:]
                if name in ('', '.'):
                    continue
                path = checked_path(name)
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


def archive_files(filename: str, data: bytes, budget: Optional[_Budget] = None) -> Dict[str, bytes]:
    """
    `{relative path: bytes}` of the regular files of a `.zip`, `.tar.gz` or `.tgz` archive, read
    in memory: unsafe paths, links, devices, encrypted entries and anything past the size and
    count limits are refused; caches and `.git` are left out.
    """
    budget = budget or _Budget()
    suffix = archive_suffix(filename)
    if suffix == '.zip':
        return _zip_files(data, budget)
    if suffix in ('.tar.gz', '.tgz'):
        return _tar_files(data, budget)
    raise UploadError('Upload a .zip, .tar.gz or .tgz archive.')


########################################################################################
# What the upload is: one file, or a directory.
########################################################################################

def _check_tree(files: Dict[str, bytes]) -> None:
    """No path is both a file and a directory of another file."""
    for key in files:
        parts = key.split('/')
        for depth in range(1, len(parts)):
            if '/'.join(parts[:depth]) in files:
                raise UploadError(f"'{'/'.join(parts[:depth])}' is both a file and a directory in the upload.")


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
) -> Dict[str, Any]:
    """
    What an upload installs: `{name, layout ('file' | 'directory'), suffix, files: {path: bytes}}`
    (paths relative to the plugins directory for a single file, to the plugin's directory
    otherwise). Raises `UploadError` with the reason when it is refused.

    - One `.py` or `.rs` file: a single-file plugin named `name`, else the file's stem.
    - One `.zip` / `.tar.gz` / `.tgz` archive holding one top-level directory: that package,
      named `name`, else the directory; holding only one `.py` / `.rs` file: a single-file
      plugin; holding files at its root: a package named `name`, else the archive's stem.
    - Several loose files: a package named `name` (required).
    """
    name = str(name or '').strip() or None
    if not uploads:
        raise UploadError('Choose the files to upload.')
    if len(uploads) > MAX_UPLOAD_FILES:
        raise UploadError(f"Uploads are limited to {MAX_UPLOAD_FILES:,} files.")
    if sum(len(data) for _, data in uploads) > limit:
        raise UploadError(f"Uploads are limited to {size_label(limit)}.")
    budget = _Budget(limit)
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
            return _plan_directory(name or top, {key[len(prefix):]: data for key, data in files.items()})
        if len(files) == 1:
            only = next(iter(files))
            if only.lower().endswith(SINGLE_SUFFIXES):
                return _plan_single(name, only, files[only])
        return _plan_directory(name or name_from_filename(filename), files)

    files: Dict[str, bytes] = {}
    for filename, data in uploads:
        ### A browser sends bare file names; anything else is refused like an archive's paths.
        path = checked_path(filename)
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
        raise UploadError('Upload a .py or .rs file, a .zip or .tar.gz archive, or the files of a package.')
    if not name:
        raise UploadError('Enter the name of the plugin these files make.')
    return _plan_directory(name, files)


########################################################################################
# Writing it.
########################################################################################

def plugin_paths(directory: pathlib.Path, name: str) -> List[pathlib.Path]:
    """Where a plugin `name` may live in a plugins directory (package, `.py`, `.rs`)."""
    return [directory / name, directory / (name + '.py'), directory / (name + '.rs')]


def existing_plugin_paths(directory: pathlib.Path, name: str) -> List[pathlib.Path]:
    """The entries of `plugin_paths()` that exist (links included)."""
    return [path for path in plugin_paths(directory, name) if os.path.lexists(path)]


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
        relative = checked_path(key)
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


########################################################################################
# The console's entry.
########################################################################################

def upload_plugin(
    contents: Any,
    filenames: Any,
    name: Any,
    directory: Any,
    replace: Any,
    session: Any,
) -> Tuple[bool, str, Optional[str]]:
    """
    Install the files a `dcc.Upload` holds as a plugin. Returns `(success, message, URL of the
    installed plugin's page)`. Only authors may (`plugin_sources.authoring_refusal`, read fresh:
    never a flag from the browser); an installed plugin is replaced only when `replace` is
    ticked and the uploader may manage it (`plugins.may_manage_plugin`), where it is installed.
    A non-administrator's upload is recorded as their team's plugin (`plugins.claim_plugin`).
    """
    from meerschaum.api.dash.plugin_sources import authoring_refusal, source_viewer
    from meerschaum.api.dash import plugins as pages
    refusal = authoring_refusal(session)
    if refusal:
        return False, refusal, None
    try:
        who = source_viewer(session)
        limit = MAX_UPLOAD_BYTES
        plan = plan_upload(decode_uploads(contents, filenames, limit), name=name, limit=limit)
        name = plan['name']
        directories = [str(path) for path in pages.new_plugin_choices(name).get('directories', [])]
        if not directories:
            raise UploadError('This server has no plugins directory to upload to.')
        directory = str(directory) if directory else directories[0]
        if directory not in directories:
            raise UploadError(f"'{directory}' is not one of the plugins directories.")
        installed = pages._discover_plugin_files().get(name)
        taken = [
            path for candidate in directories
            for path in existing_plugin_paths(pathlib.Path(candidate), name)
        ]
        if installed or taken:
            if not pages.may_manage_plugin(who, name):
                raise UploadError(pages.foreign_plugin_message(name))
            if replace is not True:
                raise UploadError(f"Plugin '{name}' already exists. Tick Replace existing to overwrite it.")
            ### Replaced where it is, so no second copy shadows it from another directory.
            homes = {str(path.parent) for path in taken}
            if len(homes) > 1:
                raise UploadError(f"Plugin '{name}' is in more than one plugins directory.")
            if homes:
                directory = next(iter(homes))
        write_plugin(plan, directory, replace=(replace is True))
    except UploadError as e:
        return False, str(e), None
    except Exception as e:
        return False, f"Failed to upload the plugin: {e}", None
    message = f"Uploaded plugin '{name}'."
    claimed, claim_message = pages.claim_plugin(who, name)
    if not claimed:
        message += ' ' + claim_message
    return True, message, pages.plugin_url(name)
