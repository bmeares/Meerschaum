#! /usr/bin/env python
# -*- coding: utf-8 -*-
# vim:fenc=utf-8

"""
Meerschaum wrapper around YAML libraries.

This is so switching between PyYAML and ruamel.yaml is smoother.
"""

from meerschaum.utils.misc import filter_keywords
from meerschaum.utils.packages import attempt_import, all_packages, _import_module
from meerschaum.utils.warnings import error
from meerschaum.utils.threading import Lock

_lib = None
### Also supports 'ruamel.yaml'.
_import_name = 'yaml'

_yaml = None
_dumper = None
_locks = {
    '_lib': Lock(),
    '_yaml': Lock(),
}

__pdoc__ = {
    'attempt_import': False,
    'error': False,
    'all_packages': False,
}


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


class yaml:
    """
    Wrapper around `PyYAML` and `ruamel.yaml` so that we may switch between implementations.
    """
    global _yaml, _lib, _dumper
    if _import_name is None:
        error("No YAML library declared.")
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
        Execute `safe_load` for `PyYAML` and `load` for `ruamel.yaml`.
        """
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
    Only supports `yaml`.
    """
    global _dumper
    if _dumper is not None:
        return _dumper

    if _import_name != 'yaml':
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
