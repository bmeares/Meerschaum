#! /usr/bin/env python
# -*- coding: utf-8 -*-
# vim:fenc=utf-8

"""
Meerschaum wrapper around YAML libraries.

When the native extension (`meerschaum._core`) is present, YAML is read and written by the
Rust codec in `meerschaum_core::yaml`: a port of PyYAML's reader, scanner, parser, composer,
constructor, representer, serializer and emitter that builds real Python objects (`datetime.date`,
`datetime.datetime`, `bytes`, big `int`, any hashable dict key). No YAML library is imported or
installed in that case, and `import meerschaum.utils.yaml` requests no package.

PyYAML (or `ruamel.yaml`) is only a fallback for an installation without the extension, which is
not a supported configuration of the v5 wheels; it keeps a bare source checkout usable.
"""

import threading

_lib = None
### The fallback library. Also supports 'ruamel.yaml'.
_import_name = 'yaml'

_yaml = None
_dumper = None
_native = None
_locks = {
    '_lib': threading.Lock(),
    '_yaml': threading.Lock(),
}

### The `dump()` keywords the native codec understands (the others are ignored, like the
### keywords `filter_keywords()` used to strip before PyYAML saw them).
_NATIVE_DUMP_KEYWORDS = frozenset((
    'default_style', 'default_flow_style', 'sort_keys', 'indent', 'width', 'allow_unicode',
    'line_break', 'explicit_start', 'explicit_end', 'encoding', 'Dumper', 'canonical',
    'version', 'tags',
))


def _load_native():
    """
    Return the native YAML module (`meerschaum._core.yaml`), or `None` without the extension.
    """
    try:
        from meerschaum import _core
        extension = _core.load()
    except Exception:
        return None
    if extension is None:
        return None
    return getattr(extension, 'yaml', None)


def _string_presenter(dumper, data: str):
    """
    Format strings with newlines as blocks.
    https://stackoverflow.com/a/33300001/9699829
    """
    tag_str = 'tag:yaml.org,2002:str'
    kw = {}
    if len(data.splitlines()) > 1:
        kw['style'] = '|'
    return dumper.represent_scalar(tag_str, data, **kw)


_native = _load_native()


class yaml:
    """
    Wrapper around the native codec, `PyYAML` and `ruamel.yaml` so that we may switch between
    implementations.
    """
    global _yaml, _lib, _dumper
    if _native is not None:
        _lib = _yaml = _native
        _import_name = 'meerschaum._core.yaml'
    else:
        from meerschaum.utils.packages import attempt_import, _import_module
        with _locks['_lib']:
            try:
                _lib = _import_module(_import_name)
            except (ImportError, ModuleNotFoundError):
                _lib = attempt_import(_import_name, split=False, lazy=False, install=True)
        with _locks['_yaml']:
            _yaml = _lib if _import_name != 'ruamel.yaml' else _lib.YAML()
            if _import_name != 'ruamel.yaml':
                _yaml.add_representer(str, _string_presenter)
                _yaml.representer.SafeRepresenter.add_representer(str, _string_presenter)

    @staticmethod
    def safe_load(*args, **kw):
        """
        Execute `safe_load` for the native codec and `PyYAML`, and `load` for `ruamel.yaml`.
        """
        if _native is not None:
            return _native.safe_load(*args)
        from meerschaum.utils.misc import filter_keywords
        if _import_name == 'ruamel.yaml':
            return _yaml.load(*args, **filter_keywords(_yaml.load, **kw))
        return _yaml.safe_load(*args, **filter_keywords(_yaml.safe_load, **kw))

    @staticmethod
    def load(*args, **kw):
        """
        Execute `yaml.load()`.
        Handles the breaking change at `v6.0` of `PyYAML`
        (added `yaml.Loader` as a positional argument).
        """
        if _native is not None:
            return _native.load(*args, **{k: v for k, v in kw.items() if k == 'Loader'})
        from meerschaum.utils.misc import filter_keywords
        from meerschaum.utils.packages import attempt_import
        packaging_version = attempt_import('packaging.version')
        if (
            _import_name == 'yaml'
            and packaging_version.parse(_yaml.__version__) >= packaging_version.parse('6.0')
            and 'Loader' not in kw
        ):
            kw['Loader'] = _yaml.Loader

        return _yaml.load(*args, **filter_keywords(_yaml.load, **kw))

    @staticmethod
    def dump(data, stream=None, **kw):
        """
        Dump to a stream. If no stream is provided, return a string instead.
        For `ruamel.yaml`, it dumps into a `StringIO` stream and returns `getvalue()`.
        """
        if _native is not None:
            return _native.dump(
                data,
                stream,
                **{k: v for k, v in kw.items() if k in _NATIVE_DUMP_KEYWORDS}
            )
        from meerschaum.utils.misc import filter_keywords
        get_string = False
        if stream is None and _import_name == 'ruamel.yaml':
            stream = _lib.compat.StringIO()
            get_string = True

        if _import_name == 'yaml' and 'Dumper' not in kw:
            kw['Dumper'] = get_dumper_class()

        result = _yaml.dump(data, stream, **filter_keywords(_yaml.dump, **kw))
        if get_string:
            return stream.getvalue()
        return result


def get_dumper_class():
    """
    Return the dumper class to use when writing.
    Only supports `yaml` (PyYAML); the native codec has no dumper class and returns `None`.
    """
    global _dumper
    if _dumper is not None:
        return _dumper

    if _native is not None or _import_name != 'yaml':
        return None

    class CustomDumper(_yaml.Dumper):
        """
        Add an extra line break when writing.
        """
        def write_line_break(self, data=None):
            if len(self.indents) == 1:
                super(CustomDumper, self).write_line_break(data)
            super(CustomDumper, self).write_line_break(data)

    _dumper = CustomDumper
    return _dumper
