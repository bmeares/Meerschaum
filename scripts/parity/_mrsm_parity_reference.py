"""Keep every reference subprocess on the released package, even from a source checkout.

Installed only in .venv-ref and loaded by a .pth file. Unlike a parent process's sys.path
edits, this import finder also protects v4's daemons and generator children. Do not import
meerschaum here: generators set their scratch configuration before its first import.
"""

import importlib.abc
import importlib.machinery
import os
import pathlib
import sys

_SITE = pathlib.Path(__file__).resolve().parent
_PACKAGE = _SITE / 'meerschaum'


class _ReferencePackage(importlib.abc.MetaPathFinder):
    def find_spec(self, fullname, path=None, target=None):
        if fullname != 'meerschaum' and not fullname.startswith('meerschaum.'):
            return None
        search = [str(_SITE)] if fullname == 'meerschaum' else path
        spec = importlib.machinery.PathFinder.find_spec(fullname, search, target)
        if spec is None:
            raise ModuleNotFoundError(f'{fullname} is not in the released parity reference.')
        origin = pathlib.Path(spec.origin).resolve() if spec.origin else None
        if origin is None or not origin.is_relative_to(_PACKAGE):
            raise ImportError(f'{fullname} resolved outside the released parity reference: {origin}')
        return spec


sys.meta_path.insert(0, _ReferencePackage())

# The recorded v4 defaults (including worker and connection counts) use a 40-CPU
# reference host. Use the same input when the golden fixtures run on other machines.
def _fixture_cpu_count():
    return 40


os.cpu_count = _fixture_cpu_count
