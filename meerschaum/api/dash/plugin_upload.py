#! /usr/bin/env python3
# -*- coding: utf-8 -*-
# vim:fenc=utf-8

"""
Uploading a plugin's files from the console (New plugin > Upload files).
Thin acyclic facade over the Dash-free domain core in `meerschaum.api.dash._plugins`.
"""

from __future__ import annotations

from meerschaum.utils.typing import Any, Dict, List, Optional, Tuple
from meerschaum.api.dash._plugins import _archives, _fs, _lifecycle

MAX_UPLOAD_BYTES: int = _fs.MAX_UPLOAD_BYTES
MAX_UPLOAD_FILES: int = _fs.MAX_UPLOAD_FILES
MAX_ARCHIVE_ENTRIES: int = _fs.MAX_ARCHIVE_ENTRIES
MAX_PATH_BYTES: int = _fs.MAX_PATH_BYTES
MAX_PATH_PARTS: int = _fs.MAX_PATH_PARTS

SKIP_PARTS = _fs.UPLOAD_SKIP_PARTS
SKIP_NAMES = _fs.SKIP_NAMES
SKIP_SUFFIXES = _fs.SKIP_SUFFIXES
SINGLE_SUFFIXES = _fs.SINGLE_SUFFIXES
ARCHIVE_SUFFIXES = _fs.ARCHIVE_SUFFIXES
TEMPORARY_SUFFIX: str = _fs.TEMPORARY_SUFFIX
DEFAULT_PROXY_BYTES: int = _fs.DEFAULT_PROXY_BYTES

UploadError = _fs.UploadError
_Budget = _archives._Budget

upload_limit_bytes = _archives.upload_limit_bytes
size_label = _archives.size_label
decode_contents = _archives.decode_contents
archive_suffix = _archives.archive_suffix
name_from_filename = _archives.name_from_filename
suggested_name = _archives.suggested_name
valid_plugin_name = _fs.valid_plugin_name
checked_path = _fs.checked_upload_path
plugin_paths = _fs.plugin_paths
existing_plugin_paths = _fs.existing_plugin_paths
write_plugin = _archives.write_plugin


def decode_uploads(
    contents: Any,
    filenames: Any,
    limit: int = MAX_UPLOAD_BYTES,
) -> List[Tuple[str, bytes]]:
    return _archives.decode_uploads(contents, filenames, limit=limit, max_files=MAX_UPLOAD_FILES)


def archive_files(
    filename: str,
    data: bytes,
    budget: Optional[_Budget] = None,
) -> Dict[str, bytes]:
    return _archives.archive_files(
        filename,
        data,
        budget=budget,
        max_files=MAX_UPLOAD_FILES,
        max_entries=MAX_ARCHIVE_ENTRIES,
    )


def plan_upload(
    uploads: List[Tuple[str, bytes]],
    name: Optional[str] = None,
    limit: int = MAX_UPLOAD_BYTES,
) -> Dict[str, Any]:
    return _archives.plan_upload(uploads, name=name, limit=limit, max_files=MAX_UPLOAD_FILES)


def upload_plugin(
    contents: Any,
    filenames: Any,
    name: Any,
    directory: Any,
    replace: Any,
    session: Any,
) -> Tuple[bool, str, Optional[str]]:
    return _lifecycle.upload_plugin(
        contents,
        filenames,
        name,
        directory,
        replace,
        session,
        limit=MAX_UPLOAD_BYTES,
        max_files=MAX_UPLOAD_FILES,
    )
