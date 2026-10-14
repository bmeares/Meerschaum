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

### Check that the Cargo version, the pyproject pins and the extras agree.
python scripts/check_packaging.py || exit 1

### Build the packages for uploading to PyPI (maturin backends; needs a Rust toolchain):
### `meerschaum` (the root pyproject.toml) and `meerschaum-cli` (the `mrsm` binary wheel).
### Per-platform wheels are built by .github/workflows/wheels.yaml.
python -m build
python -m build crates/meerschaum-cli --outdir dist

### The built wheels agree with Cargo: METADATA versions, the cli pin, the `_version.py` inside the
### wheel and `mrsm --version` of the binary in the meerschaum-cli wheel (runs it in a throwaway root).
python scripts/check_packaging.py --wheels dist --require-cli-run || exit 1

