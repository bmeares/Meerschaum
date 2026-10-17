#! /usr/bin/env python
# -*- coding: utf-8 -*-
# vim:fenc=utf-8

"""
Meerschaum wrapper around the YAML codec.

YAML is read and written by the Rust codec in `meerschaum_core::yaml` (`meerschaum._core.yaml`):
a port of PyYAML's reader, scanner, parser, composer, constructor, representer, serializer and
emitter that builds real Python objects (`datetime.date`, `datetime.datetime`, `bytes`, big
`int`, any hashable dict key). No YAML package is imported or installed.
"""

from meerschaum import _core

### The native codec module. Importing this module loads the extension.
_native = _core.yaml
_yaml = _native

### The `dump()` keywords the native codec understands (the others are ignored, like the
### keywords `filter_keywords()` used to strip before PyYAML saw them).
_NATIVE_DUMP_KEYWORDS = frozenset((
    'default_style', 'default_flow_style', 'sort_keys', 'indent', 'width', 'allow_unicode',
    'line_break', 'explicit_start', 'explicit_end', 'encoding', 'Dumper', 'canonical',
    'version', 'tags',
))


class yaml:
    """
    Wrapper around the native codec with the interface of `PyYAML`.
    """

    @staticmethod
    def safe_load(*args, **kw):
        """
        Parse a YAML document into Python objects.
        """
        return _native.safe_load(*args)

    @staticmethod
    def load(*args, **kw):
        """
        Parse a YAML document into Python objects (`Loader` is accepted and ignored).
        """
        return _native.load(*args, **{k: v for k, v in kw.items() if k == 'Loader'})

    @staticmethod
    def dump(data, stream=None, **kw):
        """
        Dump to a stream. If no stream is provided, return a string instead.
        """
        return _native.dump(
            data,
            stream,
            **{k: v for k, v in kw.items() if k in _NATIVE_DUMP_KEYWORDS}
        )


def get_dumper_class():
    """
    Return the dumper class to use when writing.
    The native codec has no dumper class and returns `None`.
    """
    return None
