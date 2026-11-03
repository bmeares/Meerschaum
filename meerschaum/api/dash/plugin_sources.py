"""
Bounded plugin source inspection (no plugin is imported or archive extracted) and, for the
administrators who may use the web terminal, editing an installed plugin's files in place.
"""

from __future__ import annotations

import hashlib
import os
import pathlib
import stat
import tarfile
import threading
import uuid

from meerschaum.utils.typing import Any, List, Optional, Tuple

MAX_FILE_BYTES = 256 * 1024
MAX_ARCHIVE_BYTES = 16 * 1024 * 1024
MAX_SCAN_BYTES = 32 * 1024 * 1024
MAX_FILES = 100
MAX_ENTRIES = 1000
MAX_PATH_BYTES = 1024
MAX_PATH_PARTS = 32
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


#: Why creating and editing plugins is unavailable to a viewer.
AUTHORING_OFF = 'Creating and editing plugins is turned off on this server.'
AUTHORING_ADMINS = 'Only administrators can create and edit plugins.'


def authoring_refusal(session: Any) -> Optional[str]:
    """
    Why this session may not create or edit plugins (`None` when it may). Writing a plugin is
    running code on the server, so it is gated exactly as the web terminal is: the server serves
    the terminal (not `--no-webterm`, `--no-dash`, `--no-auth` or `--public`), and the session's
    user, read fresh, is an administrator who may use it (`webterm:use`). The server must also
    accept plugins (`api:permissions:registration:plugins`).
    """
    try:
        from meerschaum.api.dash.webterm import webterm_enabled
        if not webterm_enabled():
            return AUTHORING_OFF
        from meerschaum.config import get_config
        if get_config('api', 'permissions', 'registration', 'plugins', warn=False) is False:
            return AUTHORING_OFF
    except Exception:
        return AUTHORING_OFF
    try:
        who = source_viewer(session)
    except Exception:
        return AUTHORING_ADMINS
    if who.no_auth or not who.signed_in or not who.is_admin or not who.can('webterm:use'):
        return AUTHORING_ADMINS
    return None


def may_author(session: Any) -> bool:
    """Whether this session may create and edit plugins (`authoring_refusal`)."""
    return authoring_refusal(session) is None


def _relative(value: Any) -> pathlib.PurePosixPath:
    text = str(value or '')
    if len(text.encode('utf-8')) > MAX_PATH_BYTES:
        raise ValueError('This source path is too long to preview.')
    path = pathlib.PurePosixPath(text)
    if (not text or '\\' in text or '\0' in text or path.is_absolute()
        or len(path.parts) > MAX_PATH_PARTS
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


def _installed_target(name: str, selected: str):
    """`(root, path of the file relative to root)` of a listed file of an installed plugin."""
    _relative(selected)
    if selected not in _installed_files(name):
        raise ValueError('This source file is not available.')
    root, candidate, info = _installed(name)
    relative = candidate.name if info['layout'] == 'file' else candidate.name + '/' + selected
    return root, relative


def _decode(data: bytes) -> str:
    if b'\0' in data:
        raise ValueError('This file is binary and cannot be previewed.')
    try:
        return data.decode('utf-8')
    except UnicodeDecodeError:
        raise ValueError('This file is not UTF-8 text.') from None


def source_digest(data: bytes) -> str:
    """The digest a file is loaded and saved against (SHA-256 of its bytes)."""
    return hashlib.sha256(data).hexdigest()


def read_source_with_digest(kind: str, name: str, selected: str, session: Any) -> Tuple[str, str]:
    """A source file's text and the digest of its bytes (what a save is checked against)."""
    _validate(kind, name, session)
    _relative(selected)
    if kind == 'installed':
        data = _read_file(*_installed_target(name, selected))
    else:
        files, data = _repository_files(name, selected)
        if selected not in files or data is None:
            raise ValueError('This source file is not available.')
    return _decode(data), source_digest(data)


def read_source(kind: str, name: str, selected: str, session: Any) -> str:
    return read_source_with_digest(kind, name, selected, session)[0]


########################################################################################
# Editing an installed plugin's files.
########################################################################################

class SourceChanged(ValueError):
    """The file on disk is not the one the editor loaded."""


#: One save at a time per process (the digest check and the rename are not atomic together).
_save_lock = threading.Lock()


def _open_parent(root: pathlib.Path, parts) -> int:
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


def _read_at(directory: int, name: str) -> Tuple[bytes, int]:
    """The bytes and permission bits of the regular file `name` in `directory` (no links)."""
    fd = os.open(name, os.O_RDONLY | os.O_NOFOLLOW | os.O_NONBLOCK, dir_fd=directory)
    try:
        info = os.fstat(fd)
        if not stat.S_ISREG(info.st_mode):
            raise ValueError('Only regular source files can be edited.')
        if info.st_size > MAX_FILE_BYTES:
            raise ValueError(f'This file exceeds the {MAX_FILE_BYTES // 1024} KiB limit.')
        with os.fdopen(fd, 'rb', closefd=False) as stream:
            data = stream.read(MAX_FILE_BYTES + 1)
        if len(data) > MAX_FILE_BYTES:
            raise ValueError(f'This file exceeds the {MAX_FILE_BYTES // 1024} KiB limit.')
        return data, stat.S_IMODE(info.st_mode)
    finally:
        os.close(fd)


def _replace_at(root: pathlib.Path, relative: str, data: bytes, expected: str) -> None:
    """
    Replace `root/relative` with `data` if its bytes still hash to `expected`: a temporary file
    beside it (hidden, created exclusively, never through a link) is written, flushed and
    renamed over it, and the directory is flushed. Every directory is opened without following
    links, so nothing outside `root` can be reached.
    """
    parts = _relative(relative).parts
    if os.open not in os.supports_dir_fd or not hasattr(os, 'O_NOFOLLOW'):
        return _replace_checked_path(root, parts, data, expected)
    directory = _open_parent(root, parts)
    name = parts[-1]
    temporary = f'.{name}.{uuid.uuid4().hex}.mrsm-edit'
    try:
        current, mode = _read_at(directory, name)
        if source_digest(current) != expected:
            raise SourceChanged(name)
        fd = os.open(
            temporary, os.O_WRONLY | os.O_CREAT | os.O_EXCL | os.O_NOFOLLOW, mode or 0o644,
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
            if source_digest(_read_at(directory, name)[0]) != expected:
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


def _replace_checked_path(root: pathlib.Path, parts, data: bytes, expected: str) -> None:
    """Portable fallback where Python cannot open relative directory descriptors (Windows)."""
    root = root.resolve(strict=True)
    candidate = root
    for part in parts:
        candidate = candidate / part
        info = candidate.lstat()
        if (stat.S_ISLNK(info.st_mode)
            or getattr(info, 'st_file_attributes', 0) & getattr(stat, 'FILE_ATTRIBUTE_REPARSE_POINT', 0)):
            raise ValueError('Linked source paths cannot be edited.')
    candidate.resolve(strict=True).relative_to(root)
    if not stat.S_ISREG(info.st_mode) or info.st_size > MAX_FILE_BYTES:
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


def save_source(name: str, selected: str, text: Any, expected: Any, session: Any) -> Tuple[bool, str, Optional[str]]:
    """
    Save `text` as an installed plugin's listed file `selected`, if it is unchanged on disk since
    it was loaded (its digest is `expected`). Returns `(success, message, new digest)`. Only a
    file the source view lists, inside that plugin's own directory, can be written
    (`authoring_refusal` is checked again here, with a fresh read of the user).
    """
    refusal = authoring_refusal(session)
    if refusal:
        return False, refusal, None
    try:
        _validate('installed', name, session)
    except PermissionError:
        return False, AUTHORING_ADMINS, None
    except ValueError as e:
        return False, str(e), None
    try:
        if not isinstance(text, str):
            raise ValueError('Nothing to save.')
        if not isinstance(expected, str) or len(expected) != 64:
            raise ValueError('Open the file for editing again before saving.')
        if '\0' in text:
            raise ValueError('Source files cannot contain NUL characters.')
        try:
            data = text.encode('utf-8')
        except UnicodeEncodeError:
            raise ValueError('The text is not valid UTF-8.') from None
        if len(data) > MAX_FILE_BYTES:
            raise ValueError(f'Source files are limited to {MAX_FILE_BYTES // 1024} KiB.')
        with _save_lock:
            root, relative = _installed_target(name, selected)
            _replace_at(root, relative, data, expected)
    except SourceChanged:
        return False, (
            f"{selected} changed on disk after it was opened, so it was not saved. "
            "Cancel to load the current file."
        ), None
    except (OSError, ValueError) as e:
        message = str(e) if isinstance(e, ValueError) else (e.strerror or str(e))
        return False, f"Failed to save {selected}: {message}", None
    return True, f"Saved {selected}.", source_digest(data)


#: The source viewer's language per suffix (Pygments lexer names).
SOURCE_MODES = {
    '.py': 'python', '.rs': 'rust', '.toml': 'toml', '.json': 'json', '.yaml': 'yaml',
    '.yml': 'yaml', '.sql': 'sql', '.md': 'markdown', '.txt': 'text',
}

#: Token classes (`source-token-<name>`), most specific first.
TOKEN_STYLES = (
    ('Comment', 'comment'), ('Keyword', 'keyword'), ('Name.Function', 'function'),
    ('Name.Class', 'class'), ('Name.Builtin', 'builtin'), ('Name.Decorator', 'decorator'),
    ('Literal.String', 'string'), ('Literal.Number', 'number'), ('Operator', 'operator'),
)

#: At most this many highlighted runs per file (a 256 KiB file of real code has far fewer); a
#: hostile, highly fragmented file is highlighted up to here and plain after.
MAX_TOKEN_RUNS = 60000

#: The Font Awesome brand logo of each plugin language.
LANGUAGE_LOGOS = {'Python': 'fa-brands fa-python', 'Rust': 'fa-brands fa-rust'}

#: Icons of a file in the tree, by suffix: `(language, logo)`; other files show their suffix.
LANGUAGE_ICONS = {
    '.py': ('Python', LANGUAGE_LOGOS['Python']),
    '.rs': ('Rust', LANGUAGE_LOGOS['Rust']),
}


def language_logo(language: str, class_name: str = ''):
    """
    A language's logo (Font Awesome) with its name as the accessible text: `role="img"`,
    `aria-label` and `title` on the logo, and the name as visually hidden text.
    """
    from dash import html
    language = str(language or '')
    logo = LANGUAGE_LOGOS.get(language)
    if not logo:
        return html.Span(language, className=('plugin-language ' + class_name).strip())
    return html.Span(
        [
            html.I(className=logo, **{'aria-hidden': 'true'}),
            html.Span(language, className='visually-hidden'),
        ],
        className=(f'plugin-language plugin-language-{language.lower()} ' + class_name).strip(),
        title=language,
        role='img',
        **{'aria-label': language},
    )


def source_mode(path: str) -> str:
    """The language a file is highlighted as."""
    return SOURCE_MODES.get(pathlib.PurePosixPath(path).suffix.lower(), 'text')


def build_source_browser(
    kind: str,
    name: str,
    session: Any,
    selected: Optional[str] = None,
    edit: bool = False,
    notice: Any = None,
):
    """
    A plugin's source: the file tree and the chosen file (`selected`, else the entry point).
    Administrators who may author plugins can edit an installed plugin's files (`edit` opens
    `selected` for editing, as New plugin does); `notice` is shown above the files.
    """
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
    first = selected if selected in files else files[0]
    editing = (
        {'path': first, 'nonce': uuid.uuid4().hex}
        if edit and kind == 'installed' and may_author(session) else None
    )
    return html.Section([
        html.Div(html.H4('Source'), className='plugin-source-header'),
        html.Div(notice, id='plugin-source-alert', className='plugin-source-alert'),
        dcc.Input(id='plugin-source-file',type='hidden',value=first),
        ### `edit` is `{path, nonce}` while a file is edited, else `None` (one writer:
        ### `callbacks/plugins.py::plugin_source_action`).
        dcc.Store(id='plugin-source-context', data={'kind': kind, 'name': name, 'edit': editing}),
        html.Div([
            source_tree(files,first),
            dcc.Loading(html.Div(id='plugin-source-content',className='plugin-source-content',
                                **{'aria-label':'Plugin source'}),type='dot'),
        ],className='plugin-source-workspace'),
    ], className='plugin-source-browser')


def _file_icon(name: str):
    from dash import html
    suffix = pathlib.PurePosixPath(name).suffix.lower()
    if suffix in LANGUAGE_ICONS:
        language, _ = LANGUAGE_ICONS[suffix]
        return html.Span(language_logo(language), className='plugin-source-file-kind')
    return html.Span(suffix.lstrip('.').upper() or 'FILE', className='plugin-source-file-kind')


def source_tree(files, selected):
    from dash import html
    from meerschaum.api.dash.components import build_icon
    tree = {'folders':{},'files':[]}
    for relative in files:
        node = tree
        parts = pathlib.PurePosixPath(relative).parts
        for part in parts[:-1]:
            node = node['folders'].setdefault(part,{'folders':{},'files':[]})
        node['files'].append((parts[-1],relative))
    def branch(node, prefix=''):
        items = []
        for name,child in sorted(node['folders'].items()):
            path = prefix+name+'/'
            ### The folder icon opens with its `details` (`console_pages.css`).
            items.append(html.Li(html.Details([
                html.Summary([
                    build_icon('folder', 'plugin-source-folder-closed'),
                    build_icon('folder-open', 'plugin-source-folder-open'),
                    html.Span(name, className='plugin-source-folder-name'),
                ], className='plugin-source-folder'),
                branch(child,path),
            ],open=selected.startswith(path))))
        for name,path in sorted(node['files']):
            items.append(html.Li(html.Button([
                _file_icon(name),
                html.Span(name,className='plugin-source-file-name'),
            ],id={'type':'plugin-source-select','path':path},n_clicks=0,type='button',
                className='plugin-source-file'+(' active' if path==selected else ''),
                title=path,**{'aria-label':path})))
        return html.Ul(items,className='plugin-source-tree-list')
    return html.Nav(branch(tree),className='plugin-source-tree',**{'aria-label':'Plugin files'})


def _utf16_length(value: str) -> int:
    """A string's length in the browser (UTF-16 code units), which the runs count in."""
    return len(value.encode('utf-16-le')) // 2


def source_tokens(path: str, source: str) -> dict:
    """
    The highlighted source as data: the text, and runs of `[length, style]` (UTF-16 lengths;
    `style` indexes `styles`, `-1` is plain) that `assets/plugin_source.js` renders as escaped
    text with line numbers. Every file is highlighted in full, up to `MAX_TOKEN_RUNS` runs.
    """
    from pygments.lexers import get_lexer_by_name
    from pygments.token import Token, string_to_tokentype
    mode = source_mode(path)
    categories = [(string_to_tokentype(name), index) for index, (name, _) in enumerate(TOKEN_STYLES)]
    runs: List[List[int]] = []
    plain = 0
    for _, token, value in get_lexer_by_name(mode).get_tokens_unprocessed(source):
        if not value:
            continue
        if len(runs) >= MAX_TOKEN_RUNS:
            plain += _utf16_length(value)
            continue
        style = next((index for category, index in categories if token in category), -1)
        length = _utf16_length(value)
        if runs and runs[-1][1] == style:
            runs[-1][0] += length
        else:
            runs.append([length, style])
    if plain:
        runs.append([plain, -1])
    return {
        'path': path,
        'mode': mode,
        'source': source,
        'styles': [name for _, name in TOKEN_STYLES],
        'runs': runs,
    }


def _source_action_button(action: str, label: str, icon: str, style: str):
    """Edit, Save or Cancel in a file's toolbar (`callbacks/plugins.py::plugin_source_action`)."""
    from dash import html
    return html.Button(
        [html.I(className=icon, **{'aria-hidden': 'true'}), html.Span(label)],
        type='button', n_clicks=0,
        id={'type': 'plugin-source-action', 'action': action},
        className=f'btn btn-sm {style} plugin-source-action plugin-source-{action}',
        title=label,
    )


def source_preview(path, source, digest: Optional[str] = None, editable: bool = False,
                   editing: bool = False):
    """
    One file: its toolbar and its code, which the browser fills from `plugin-source-data`
    (`assets/plugin_source.js`), always as text, never as the plugin's HTML. An `editable` file
    has Edit; while `editing`, the code is an editor with the same look, with Save and Cancel.
    """
    from dash import html, dcc
    from meerschaum.api.dash.components import build_icon
    suffix = pathlib.PurePosixPath(str(path)).suffix.lower()
    logo = language_logo(LANGUAGE_ICONS[suffix][0], 'plugin-source-toolbar-language') if suffix in LANGUAGE_ICONS else None
    lines = len(source.splitlines())
    copy = html.Button(
        [build_icon('copy', 'plugin-source-icon-copy'), build_icon('check', 'plugin-source-icon-copied')],
        type='button', className='btn btn-sm btn-outline-secondary plugin-source-copy',
        title='Copy source', **{'aria-label': 'Copy source', 'data-source-action': 'copy'},
    )
    fullscreen = html.Button(
        [build_icon('fullscreen', 'plugin-source-icon-enter'),
         build_icon('fullscreen-exit', 'plugin-source-icon-exit')],
        type='button', className='btn btn-sm btn-outline-secondary plugin-source-fullscreen-button',
        title='Fullscreen', **{'aria-label': 'Fullscreen', 'aria-pressed': 'false',
                               'data-source-action': 'fullscreen'},
    )
    if editing:
        data = {**source_tokens(path, source),
                'digest': digest, 'editing': True}
        toolbar = [
            logo, html.Code(path),
            _source_action_button('save', 'Save', 'fa-solid fa-floppy-disk', 'btn-primary'),
            _source_action_button('cancel', 'Cancel', 'fa-solid fa-xmark', 'btn-outline-secondary'),
        ]
        ### The gutter's numbers follow the text (`assets/plugin_source.js`).
        code = html.Div([
            html.Pre('\n'.join(str(n) for n in range(1, max(lines, 1) + 1)),
                     className='plugin-source-gutter', **{'aria-hidden': 'true'}),
            html.Label(f'Edit {path}', htmlFor='plugin-source-editor', className='visually-hidden'),
            html.Div([
                html.Pre(html.Code(id='plugin-source-editor-highlight'),
                         className='plugin-source-editor-highlight', **{'aria-hidden': 'true'}),
                dcc.Textarea(
                    id='plugin-source-editor', value=source, spellCheck='false', wrap='off',
                    className='plugin-source-editor',
                ),
            ], className='plugin-source-editor-surface'),
        ], className='plugin-source-code plugin-source-editor-code')
    else:
        data = {**source_tokens(path, source), 'digest': digest, 'editing': False}
        toolbar = [logo, html.Code(path), html.Span(f'{lines:,} lines', className='text-muted')]
        if editable:
            toolbar.append(_source_action_button(
                'edit', 'Edit', 'fa-solid fa-pen-to-square', 'btn-outline-secondary'))
        code = html.Div(html.Pre(html.Code(id='plugin-source-code', className='language-'+data['mode'])),
                        className='plugin-source-code')
    toolbar.extend([copy, fullscreen])
    return html.Div([
        html.Div([item for item in toolbar if item is not None], className='plugin-source-toolbar'),
        code,
        dcc.Store(id='plugin-source-data', data=data),
        dcc.Store(id='plugin-source-edit-request'),
        dcc.Store(id='plugin-source-edit-tokens'),
        html.Div(id='plugin-source-rendered', hidden=True),
        html.Div(id='plugin-source-edit-rendered', hidden=True),
    ],className='plugin-source-preview' + (' plugin-source-preview-editing' if editing else ''))
