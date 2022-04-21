#! /usr/bin/env python
# -*- coding: utf-8 -*-
# vim:fenc=utf-8

"""
Meerschaum wrapper around YAML libraries.

This is so switching between PyYAML and ruamel.yaml is smoother.
"""
_lib = None
_import_name = None
### The first import that is found will be used.
_yaml_imports = ['yaml', 'ruamel.yaml']
_yaml = None

class yaml:
    global _yaml, _import_name, _lib
    from meerschaum.utils.packages import attempt_import, all_packages, _import_module
    from meerschaum.utils.warnings import error
    for k in all_packages:
        if k in _yaml_imports:
            _import_name = k
            break
    if _import_name is None:
        error(f"No YAML libraries declared in meerschaum.packages.")

    try:
        _lib = _import_module(_import_name)
    except (ImportError, ModuleNotFoundError):
        _lib = attempt_import(_import_name, split=False, lazy=False, install=True)
    if _import_name == 'ruamel.yaml':
        _yaml = _lib.YAML()
    else:
        _yaml = _lib

    def safe_load(*args, **kw):
        from meerschaum.utils.misc import filter_keywords
        if _import_name == 'ruamel.yaml':
            return _yaml.load(*args, **filter_keywords(_yaml.load, **kw))
        return _yaml.safe_load(*args, **filter_keywords(_yaml.safe_load, **kw))

    def load(*args, **kw):
        from meerschaum.utils.misc import filter_keywords
        from meerschaum.utils.packages import attempt_import
        packaging_version = attempt_import('packaging.version')
        _args = list(args)
        if (
            _import_name == 'yaml'
            and packaging_version.parse(_yaml.__version__) >= packaging_version.parse('6.0')
        ):
            _args += [_yaml.Loader]
        return _yaml.load(*_args, **filter_keywords(_yaml.load, **kw))

    def dump(data, stream=None, **kw):
        from meerschaum.utils.misc import filter_keywords
        get_string = False
        if stream is None and _import_name == 'ruamel.yaml':
            stream = _lib.compat.StringIO()
            get_string = True
        result = _yaml.dump(data, stream, **filter_keywords(_yaml.dump, **kw))
        if get_string:
            return stream.getvalue()
        return result

