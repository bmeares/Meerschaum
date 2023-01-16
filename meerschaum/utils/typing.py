from __future__ import annotations
try:
    from typing import (
        Tuple,
        Optional,
        Dict,
        List,
        Mapping,
        Sequence,
        Callable,
        Union,
        Any,
        Iterable,
        Hashable,
    )
except Exception as e:
    import urllib.request, sys, pathlib, os
    old_cwd = os.getcwd()
    cache_dir = pathlib.Path.home() / '.cache'
    if not cache_dir.exists():
        try:
            cache_dir.mkdir(parents=True, exist_ok=True)
        except Exception as _e:
            cache_dir = pathlib.Path.home()

    dest_file = cache_dir / 'typing_hotfix.py'
    os.chdir(cache_dir)

    url = 'https://raw.githubusercontent.com/python/typing_extensions/main/src/typing_extensions.py'
    if not dest_file.exists():
        response = urllib.request.urlopen(url)
        if response.code != 200:
            print(f"Could not download typing. Please install typing via pip or upgrade Python.")
            sys.exit(1)
        with open(dest_file, 'wb') as f:
            f.write(response.fp.read())
    
    import typing_hotfix
    os.chdir(old_cwd)

### Patch Literal for Python 3.7.
try:
    from typing import Literal
except ImportError:
    import typing

    class _LiteralForm(typing._SpecialForm, _root=True):

        def __repr__(self):
            return 'typing_extensions.' + self._name

        def __getitem__(self, parameters):
            return typing._GenericAlias(self, parameters)

    typing.Literal = _LiteralForm(
        'Literal',
       doc = """A type that can be used to indicate to type checkers
       that the corresponding value has a value literally equivalent
       to the provided parameter. For example:

           var: Literal[4] = 4

       The type checker understands that 'var' is literally equal to
       the value 4 and no other value.

       Literal[...] cannot be subclassed. There is no runtime
       checking verifying that the parameter is actually a value
       instead of a type."""
    )

    
SuccessTuple = Tuple[bool, str]
InstanceConnector = Union[
    'meerschaum.connectors.sql.SQLConnector',
    'meerschaum.connectors.api.APIConnector'
]
PipesDict = Dict[
    str, Dict[                           ### connector_keys : metrics
        str, Dict[                       ### metric_key     : locations
            str, 'meerschaum.Pipe'       ### location_key   : Pipe
        ]
    ]
]
WebState = Dict[str, str]
