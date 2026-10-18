#! /bin/bash

DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" >/dev/null 2>&1 && pwd )"
. "$DIR"/config.sh
cd "$PARENT"

[ "$1" == "--push" ] && push="--push"

### Delete leftover files before building.
./scripts/clean.sh

### Build documentation.
./scripts/docs.sh

### Update requirements/<extra>.txt and the extras table compiled into `mrsm` from pyproject.toml.
./scripts/docker/update_requirements.sh

### Check that the Cargo version, the console scripts and the extras agree.
python scripts/check_packaging.py || exit 1

### Build the package for uploading to PyPI (the maturin backend; needs a Rust toolchain): the one
### `meerschaum` wheel, whose `mrsm` console script runs the CLI from the extension (D8, revised).
### Per-platform wheels are built by .github/workflows/wheels.yaml. The standalone binary is
### `cargo install meerschaum`.
python -m build

### The built wheel agrees with Cargo: the METADATA version, no dependencies, the `_version.py` inside
### the wheel, the `mrsm` / `meerschaum` console scripts, and no native binary in it.
python scripts/check_packaging.py --wheels dist || exit 1
