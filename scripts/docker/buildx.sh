#! /usr/bin/env bash
# Usage: scripts/docker/buildx.sh [--push] [--v4-only]
#
# Builds the Meerschaum images (see the Dockerfile header and docs/design/v5-build.md):
#   - one image per dep group in $tags (scripts/config.sh): `minimal` uses the default (v4) target,
#     `full` uses `--target full` (the v4 full image plus the native `mrsm` binary);
#   - `--target slim`, tagged `$image:<version>-slim` (and `$image:slim` for releases).
# Images are loaded into the local daemon unless --push is given.
#
#   --push      push to the registry instead of loading locally.
#   --v4-only   build exactly what v4 built: every dep group on the default target, no Rust, no slim.
#
# Pre-release versions (dev, rc, aN, bN) only get version-qualified tags, never `latest`/`full`/`slim`.
# Pushing the Rust targets requires a 5.x version, so publishing 4.x from this branch needs --v4-only.
# When $GITHUB_OUTPUT is set, writes `slim_image` and `full_image` (the version-qualified tags).
# Experimental features must be enabled and docker buildx must be installed (see scripts/setup.sh).
set -euo pipefail

DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" >/dev/null 2>&1 && pwd )"
# shellcheck source=scripts/config.sh
. "$DIR"/../config.sh
cd "$PARENT"

push="--load"
v4_only=""
for arg in "$@"; do
  case "$arg" in
    --push)    push="--push" ;;
    --v4-only) v4_only=1 ;;
    *) echo "buildx.sh: unknown argument '$arg'" >&2; exit 2 ;;
  esac
done
PYTHON_BIN="${PYTHON_BIN:-python}"

mrsm_version=$($PYTHON_BIN -m meerschaum -V | sed 's/Meerschaum v//g')
mrsm_uid=$(id -u)
mrsm_gid=$(id -g)

prerelease=""
case "$mrsm_version" in
  *dev*|*rc*|*a[0-9]*|*b[0-9]*) prerelease=1 ;;
esac

if [ -z "$v4_only" ] && [ "$push" = "--push" ] && [ "${mrsm_version%%.*}" -lt 5 ]; then
  echo "buildx.sh: refusing to push the Rust targets for version $mrsm_version; use --v4-only." >&2
  exit 1
fi

./scripts/docker/update_requirements.sh

docker pull "$python_image"

build() {
  ### build <target or ""> <dep_group> <tags...>
  local target="$1" dep_group="$2"; shift 2
  local target_args=() tag_args=() t
  [ -n "$target" ] && target_args=(--target "$target")
  for t in "$@"; do tag_args+=(-t "$t"); done
  echo "Building tags (target ${target:-default}): $*"
  docker buildx build --progress plain "$push" \
    "${target_args[@]}" \
    --build-arg dep_group="$dep_group" \
    --build-arg mrsm_uid="$mrsm_uid" \
    --build-arg mrsm_gid="$mrsm_gid" \
    --platform "$platforms" \
    "${tag_args[@]}" \
    .
}

for tag in "${tags[@]}"; do

  specific_tags=("$image:$mrsm_version-$tag")
  if [ -z "$prerelease" ]; then
    specific_tags+=("$image:$tag")
    if [ "$latest_alias" == "$tag" ]; then
      ### NOTE: the `api` tag is deprecated; treat as `latest`.
      specific_tags+=("$image:latest" "$image:$mrsm_version" "$image:api" "$image:$mrsm_version-api")
    fi
  fi

  target=""
  if [ -z "$v4_only" ] && [ "$tag" == "full" ]; then
    target="full"
  fi
  build "$target" "$tag" "${specific_tags[@]}"

done

if [ -z "$v4_only" ]; then
  slim_tags=("$image:$mrsm_version-slim")
  [ -z "$prerelease" ] && slim_tags+=("$image:slim")
  build "slim" "minimal" "${slim_tags[@]}"
fi

if [ -n "${GITHUB_OUTPUT:-}" ]; then
  echo "slim_image=$image:$mrsm_version-slim" >> "$GITHUB_OUTPUT"
  echo "full_image=$image:$mrsm_version-full" >> "$GITHUB_OUTPUT"
fi
