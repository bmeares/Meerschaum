"""
Installed into the parity reference venv (.venv-ref) by scripts/parity/setup_reference.sh and
imported at interpreter startup through a `.pth` file, so it applies to every reference process,
including servers and daemons that generators spawn.

v4 discovers its actions (and other modules) with `glob.glob`, which returns entries in the
filesystem's directory order: alphabetical for an in-order install on XFS, newest-first on tmpfs,
arbitrary for a parallel installer. That order leaks into recorded fixtures (`GET /actions`, the
shell's completions). Sorting makes the reference deterministic; the committed fixtures were
recorded with alphabetical order.
"""

import glob as _glob

_orig_glob = _glob.glob
_orig_iglob = _glob.iglob


def glob(*args, **kwargs):
    return sorted(_orig_glob(*args, **kwargs))


def iglob(*args, **kwargs):
    return iter(sorted(_orig_iglob(*args, **kwargs)))


_glob.glob = glob
_glob.iglob = iglob
