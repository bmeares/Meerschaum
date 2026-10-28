"""Bounded, read-only plugin source inspection. No plugin is imported or archive extracted."""

from __future__ import annotations

import os
import pathlib
import stat
import tarfile

from meerschaum.utils.typing import Any, List

MAX_FILE_BYTES = 256 * 1024
MAX_ARCHIVE_BYTES = 16 * 1024 * 1024
MAX_SCAN_BYTES = 32 * 1024 * 1024
MAX_FILES = 100
MAX_ENTRIES = 1000
TEXT_SUFFIXES = frozenset({'.py', '.rs', '.toml', '.md', '.txt', '.json', '.yaml', '.yml', '.sql'})
SKIP_PARTS = frozenset({'__pycache__', 'target', 'node_modules', '.git', '.venv', 'venv'})


def source_viewer(session: Any):
    """Refresh the session's identity and rights for each source read; never trust UI flags."""
    from meerschaum.api import no_auth
    from meerschaum.api.dash import access
    from meerschaum.api.dash.sessions import get_username_from_session
    if no_auth:
        return access.viewer_for(None)
    session_id = session.get('session-id') if isinstance(session, dict) else None
    username = get_username_from_session(session_id) if session_id else None
    return access.viewer_for(username) if username else access.ANONYMOUS


def may_read_source(kind: str, session: Any) -> bool:
    who = source_viewer(session)
    if who.no_auth:
        return True
    if not who.signed_in or not who.can('plugins:read'):
        return False
    # Installed code is server-private. A repository archive is already downloadable.
    return kind == 'repository' or (kind == 'installed' and who.is_admin)


def _relative(value: Any) -> pathlib.PurePosixPath:
    text = str(value or '')
    path = pathlib.PurePosixPath(text)
    if (not text or '\\' in text or '\0' in text or path.is_absolute()
        or any(part in ('', '.', '..') for part in text.split('/'))
        or any(part.startswith('.') or part in SKIP_PARTS for part in path.parts)):
        raise ValueError('Invalid source file.')
    return path


def _is_text(path: pathlib.PurePath) -> bool:
    return path.suffix.lower() in TEXT_SUFFIXES or path.name in ('LICENSE', 'COPYING')


def _source_order(path: str):
    entrypoint = (path == '__init__.py' or path.endswith('/__init__.py')
                  or path == 'src/lib.rs' or path.endswith('/src/lib.rs')
                  or ('/' not in path and pathlib.PurePosixPath(path).suffix in ('.py', '.rs')))
    return (not entrypoint, path.count('/'), path)


def _read_checked_path(root: pathlib.Path, parts, limit: int) -> bytes:
    """Portable fallback where Python cannot open relative directory descriptors (Windows)."""
    root = root.resolve(strict=True)
    candidate = root
    for part in parts:
        candidate = candidate / part
        info = candidate.lstat()
        if (stat.S_ISLNK(info.st_mode)
            or getattr(info, 'st_file_attributes', 0) & getattr(stat, 'FILE_ATTRIBUTE_REPARSE_POINT', 0)):
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
    if (len(data) > limit or candidate.resolve(strict=True) != resolved
        or (opened.st_dev, opened.st_ino) != (after.st_dev, after.st_ino)):
        raise ValueError('The source file changed while it was read.')
    return data


def _read_file(root: pathlib.Path, relative: str, limit: int = MAX_FILE_BYTES) -> bytes:
    """Open each component beneath a trusted root without following symlinks."""
    parts = _relative(relative).parts
    if os.open not in os.supports_dir_fd or not hasattr(os, 'O_NOFOLLOW'):
        return _read_checked_path(root, parts, limit)
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


def _installed(name: str):
    from meerschaum.config import paths
    from meerschaum.api.dash.plugins import _discover_plugin_files
    info = _discover_plugin_files().get(name)
    if not info:
        raise ValueError('This plugin is not installed.')
    candidate = pathlib.Path(info['path'])
    roots = [pathlib.Path(root).resolve() for root in paths.PLUGINS_DIR_PATHS]
    root = next((root for root in roots if candidate.parent.resolve() == root), None)
    if root is None or candidate.is_symlink() or candidate.resolve().parent != root:
        raise ValueError('Plugin source is outside the installed plugin directories.')
    return root, candidate, info


def _installed_files(name: str) -> List[str]:
    root, candidate, info = _installed(name)
    if info['layout'] == 'file':
        return [candidate.name]
    found, scanned = [], 0
    for directory, dirs, files in os.walk(candidate, followlinks=False):
        scanned += len(dirs)
        if scanned > MAX_ENTRIES:
            raise ValueError('This plugin contains too many entries to preview.')
        dirs[:] = sorted(part for part in dirs if not part.startswith('.') and part not in SKIP_PARTS
                         and not (pathlib.Path(directory) / part).is_symlink())
        for file in sorted(files):
            scanned += 1
            if scanned > MAX_ENTRIES:
                raise ValueError('This plugin contains too many files to preview.')
            path = pathlib.Path(directory) / file
            relative = path.relative_to(candidate)
            if path.is_symlink() or not _is_text(relative):
                continue
            try:
                _relative(relative.as_posix())
            except ValueError:
                continue
            found.append(relative.as_posix())
            if len(found) > MAX_FILES:
                raise ValueError('This plugin contains too many source files to preview.')
    return sorted(found, key=_source_order)


def _archive(name: str):
    """Open only the named repository archive beneath the configured archive root."""
    import gzip
    import io
    from meerschaum.config import paths
    root = pathlib.Path(paths.PLUGINS_ARCHIVES_RESOURCES_PATH).resolve()
    data = _read_file(root, name + '.tar.gz', MAX_ARCHIVE_BYTES)
    # Bound expansion before tarfile processes long-name / PAX headers as well as file bodies.
    with gzip.GzipFile(fileobj=io.BytesIO(data)) as compressed:
        expanded = compressed.read(MAX_SCAN_BYTES + 1)
    if len(expanded) > MAX_SCAN_BYTES:
        raise ValueError('This archive is too large to preview safely.')
    return tarfile.open(fileobj=io.BytesIO(expanded), mode='r:')


def _repository_files(name: str, selected: str = ''):
    files, result, scanned_bytes = [], None, 0
    with _archive(name) as archive:
        for count, member in enumerate(archive):
            if count >= MAX_ENTRIES:
                raise ValueError('This archive contains too many entries to preview.')
            scanned_bytes += max(0, member.size) + 512
            if scanned_bytes > MAX_SCAN_BYTES:
                raise ValueError('This archive is too large to preview safely.')
            try:
                relative = _relative(member.name)
            except ValueError:
                continue
            if not member.isfile() or not _is_text(relative):
                continue
            if member.size > MAX_FILE_BYTES:
                continue
            files.append(relative.as_posix())
            if len(files) > MAX_FILES:
                raise ValueError('This archive contains too many source files to preview.')
            if member.name == selected:
                stream = archive.extractfile(member)
                result = stream.read(MAX_FILE_BYTES + 1) if stream else None
    if len(files) != len(set(files)):
        raise ValueError('This archive contains ambiguous duplicate source files.')
    return sorted(files, key=_source_order), result


def _validate(kind: str, name: str, session: Any):
    from meerschaum.api.dash.plugins import is_safe_plugin_name
    if kind not in ('installed', 'repository') or not is_safe_plugin_name(name):
        raise ValueError('Invalid plugin source selection.')
    if not may_read_source(kind, session):
        raise PermissionError('Plugin source is not available to your account.')
    if kind == 'repository':
        from meerschaum.api import get_api_connector
        if name not in (get_api_connector().get_plugins(search_term=name) or []):
            raise ValueError('This plugin is not in the repository.')


def source_files(kind: str, name: str, session: Any) -> List[str]:
    _validate(kind, name, session)
    return _installed_files(name) if kind == 'installed' else _repository_files(name)[0]


def read_source(kind: str, name: str, selected: str, session: Any) -> str:
    _validate(kind, name, session)
    _relative(selected)
    if kind == 'installed':
        if selected not in _installed_files(name):
            raise ValueError('This source file is not available.')
        root, candidate, info = _installed(name)
        relative = candidate.name if info['layout'] == 'file' else candidate.name + '/' + selected
        data = _read_file(root, relative)
    else:
        files, data = _repository_files(name, selected)
        if selected not in files or data is None:
            raise ValueError('This source file is not available.')
    if b'\0' in data:
        raise ValueError('This file is binary and cannot be previewed.')
    try:
        return data.decode('utf-8')
    except UnicodeDecodeError:
        raise ValueError('This file is not UTF-8 text.') from None


def build_source_browser(kind: str, name: str, session: Any):
    from dash import html, dcc
    import dash_bootstrap_components as dbc
    if not may_read_source(kind, session):
        return None
    try:
        files = source_files(kind, name, session)
    except Exception:
        return dbc.Alert('Source preview is unavailable for this plugin.', color='secondary')
    if not files:
        return html.P('No source files are available to preview.', className='text-muted')
    return html.Section([
        html.H4('Source'),
        html.Label('File', htmlFor='plugin-source-file'),
        dcc.Dropdown(id='plugin-source-file', value=files[0], clearable=False,
                     options=[{'label': file, 'value': file} for file in files]),
        dcc.Store(id='plugin-source-context', data={'kind': kind, 'name': name}),
        dcc.Loading(html.Pre(id='plugin-source-content', className='plugin-source-content',
                             tabIndex=0, **{'aria-label': 'Plugin source'}), type='dot'),
    ], className='plugin-source-browser')
