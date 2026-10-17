#!/usr/bin/env bash
# Usage: scripts/docker/trim_install.sh PREFIX
#
# Shrinks a `pip install --prefix=PREFIX --no-compile` tree for the Docker images (the §12.1 size
# gate: `full` < 900 MB). Run in the builder stage, never in the final image. It removes only
# files that are not needed at run time:
#
#   - `__pycache__` directories and `.pyc` files (the images install with `--no-compile`: the byte
#     code of pandas, pyarrow, plotly, dash, ... would add ~125 MB; Python compiles on first
#     import into $PYTHONPYCACHEPREFIX instead, see the Dockerfile);
#   - the `tests` directories that wheels ship (pandas/tests alone is ~15 MB);
#   - C/C++ headers and Cython sources (`include/` directories, `*.h`, `*.hpp`, `*.pxd`, `*.pxi`,
#     `*.pyx`), which only matter to someone compiling against the package;
#   - debug and symbol sections of every shared library (`strip --strip-unneeded`), which keeps
#     the dynamic symbol table and so cannot change behavior (~110 MB for the `[full]` extras).
#
# It keeps every `*.dist-info` (so `pip list`, `importlib.metadata` and `prune venvs` still see the
# distributions), the `.pyi` stubs, and the `mrsm` binary (already stripped by maturin).
set -euo pipefail

prefix="${1:?usage: trim_install.sh PREFIX}"
before=$(du -sm "$prefix" | cut -f1)

find "$prefix" -type d -name __pycache__ -prune -exec rm -rf {} +
find "$prefix" -type f -name '*.pyc' -delete
find "$prefix" -path '*/site-packages/*' -type d -name tests -prune -exec rm -rf {} +
find "$prefix" -path '*/site-packages/*' -type d -name include -prune -exec rm -rf {} +
find "$prefix" -path '*/site-packages/*' -type f \
  \( -name '*.h' -o -name '*.hpp' -o -name '*.pxd' -o -name '*.pxi' -o -name '*.pyx' \) -delete
find "$prefix" -path '*/site-packages/*' -type f \( -name '*.so' -o -name '*.so.*' \) \
  -exec strip --strip-unneeded {} + 2>/dev/null || true

after=$(du -sm "$prefix" | cut -f1)
echo "trim_install: $prefix ${before} MB -> ${after} MB"
