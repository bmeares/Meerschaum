#! /bin/bash

DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" >/dev/null 2>&1 && pwd )"
. "$DIR"/config.sh
cd "$PARENT"

[ "$1" == "--push" ] && push="--push"

### Delete leftover files before building.
./scripts/clean.sh

### Build documentation.
./scripts/docs.sh

### Update the requirements text files.
./scripts/docker/update_requirements.sh

### Check that the Cargo version, the pyproject pins and the extras agree.
python scripts/check_packaging.py || exit 1

### Build the packages for uploading to PyPI (maturin backends; needs a Rust toolchain):
### `meerschaum` (the root pyproject.toml) and `meerschaum-cli` (the `mrsm` binary wheel).
### Per-platform wheels are built by .github/workflows/wheels.yaml.
python -m build
python -m build crates/meerschaum-cli --outdir dist

