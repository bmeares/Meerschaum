#! /usr/bin/env bash
# Usage: scripts/docker/buildx.sh [--push] [--v4-only] [--platform P] [--arch-tags] [--merge]
#
# Builds the Meerschaum images (see the Dockerfile header and docs/design/v5-build.md):
#   - one image per dep group in $tags (scripts/config.sh): `minimal` uses the default (v4) target,
#     `full` uses `--target full` (`meerschaum[full]`; its `mrsm` is the console script);
#   - `--target slim`, tagged `$image:<version>-slim` (and `$image:slim` for releases).
# Images are loaded into the local daemon unless --push is given; loaded images are then smoke-tested
# (scripts/docker/smoke_image.sh: curl, git in full, the stack's API healthcheck; SKIP_SMOKE=1 skips it).
#
#   --push        push to the registry instead of loading locally.
#   --v4-only     build every dep group on the default target only (no `slim`).
#   --platform P  build for P only (`linux/amd64`, `linux/arm64`). Default: the builder's own platform
#                 when loading, `$platforms` (scripts/config.sh: amd64 and arm64) when pushing.
#   --arch-tags   push only the architecture-qualified tag `$image:<version>-<tag>-<arch>` of every image
#                 (needs --push and --platform with one platform): what each per-architecture job of
#                 .github/workflows/docker-build.yaml pushes, from a runner of that architecture (the Rust
#                 release build is far too slow under QEMU).
#   --merge       build nothing: join the `-<arch>` tags of every `$platforms` architecture into the
#                 multi-architecture tags (the version-qualified ones, and `latest`, `full`, `slim`,
#                 `minimal`, `api` for a release) with `docker buildx imagetools create`. Run it after
#                 every `--push --arch-tags` job succeeded. The `-<arch>` tags stay in the registry as
#                 the architectures' own images.
#
# Pre-release versions (dev, rc, aN, bN) only get version-qualified tags, never `latest`/`full`/`slim`.
# Pushing requires a 5.x version (the wheels' version, from the Cargo workspace).
# When $GITHUB_OUTPUT is set, writes `slim_image` and `full_image` (the version-qualified tags).
# Experimental features must be enabled and docker buildx must be installed (see scripts/setup.sh).
#
# For tests: BUILDX_DRY_RUN=1 prints every docker command (`+ docker ...`) instead of running it and
# skips the requirements refresh, the `docker pull` and the smoke test (tests/test_docker_buildx.py).
set -euo pipefail

DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" >/dev/null 2>&1 && pwd )"
# shellcheck source=scripts/config.sh
. "$DIR"/../config.sh
cd "$PARENT"

push="--load"
v4_only=""
platform=""
arch_tags=""
merge=""
while [ $# -gt 0 ]; do
  case "$1" in
    --push)      push="--push" ;;
    --v4-only)   v4_only=1 ;;
    --arch-tags) arch_tags=1 ;;
    --merge)     merge=1 ;;
    --platform)
      [ $# -ge 2 ] || { echo "buildx.sh: --platform needs a value" >&2; exit 2; }
      platform="$2"; shift ;;
    --platform=*) platform="${1#--platform=}" ;;
    *) echo "buildx.sh: unknown argument '$1'" >&2; exit 2 ;;
  esac
  shift
done
PYTHON_BIN="${PYTHON_BIN:-python}"
dry_run="${BUILDX_DRY_RUN:-}"

run() {
  ### Run a command, or only print it under BUILDX_DRY_RUN.
  if [ -n "$dry_run" ]; then
    echo "+ $*"
  else
    "$@"
  fi
}

arch_of() {
  ### arch_of linux/arm64 -> arm64 (linux/arm/v7 -> arm-v7)
  local p="${1#linux/}"
  echo "${p//\//-}"
}

if [ -n "$arch_tags" ]; then
  if [ -z "$platform" ] || [[ "$platform" == *,* ]]; then
    echo "buildx.sh: --arch-tags needs --platform with exactly one platform (e.g. linux/arm64)." >&2
    exit 2
  fi
  if [ "$push" != "--push" ]; then
    echo "buildx.sh: --arch-tags is for --push (loaded images keep their plain tags)." >&2
    exit 2
  fi
fi
if [ -n "$merge" ] && { [ -n "$arch_tags" ] || [ -n "$platform" ] || [ "$push" = "--push" ]; }; then
  echo "buildx.sh: --merge takes no --push, --platform or --arch-tags." >&2
  exit 2
fi

### The wheels' version: the Cargo workspace version in PEP 440 form (scripts/check_packaging.py).
mrsm_version=$($PYTHON_BIN scripts/check_packaging.py --version)
mrsm_uid=$(id -u)
mrsm_gid=$(id -g)

prerelease=""
case "$mrsm_version" in
  *dev*|*rc*|*a[0-9]*|*b[0-9]*) prerelease=1 ;;
esac

if { [ "$push" = "--push" ] || [ -n "$merge" ]; } && [ "${mrsm_version%%.*}" -lt 5 ]; then
  echo "buildx.sh: refusing to push version $mrsm_version (the wheels are 5.x)." >&2
  exit 1
fi

### The tags of one image (a dep group, or `slim`), one per line: the version-qualified one first.
final_tags() {
  local tag="$1"
  echo "$image:$mrsm_version-$tag"
  if [ -z "$prerelease" ]; then
    echo "$image:$tag"
    if [ "$tag" == "$latest_alias" ]; then
      ### NOTE: the `api` tag is deprecated; treat as `latest`.
      echo "$image:latest"
      echo "$image:$mrsm_version"
      echo "$image:api"
      echo "$image:$mrsm_version-api"
    fi
  fi
}

### Every image this script publishes: the dep groups, then `slim` (unless --v4-only).
image_groups() {
  local tag
  for tag in "${tags[@]}"; do echo "$tag"; done
  if [ -z "$v4_only" ]; then echo "slim"; fi
}

if [ -n "$merge" ]; then
  ### Join the per-architecture images into the published multi-architecture tags.
  [ -n "$dry_run" ] || command -v docker >/dev/null || { echo "buildx.sh: docker is required." >&2; exit 1; }
  for group in $(image_groups); do
    sources=()
    for p in ${platforms//,/ }; do
      sources+=("$image:$mrsm_version-$group-$(arch_of "$p")")
    done
    tag_args=()
    while IFS= read -r t; do tag_args+=(-t "$t"); done < <(final_tags "$group")
    echo "Merging ${sources[*]} into: ${tag_args[*]}"
    run docker buildx imagetools create "${tag_args[@]}" "${sources[@]}"
  done
  exit 0
fi

if [ -z "$dry_run" ]; then
  ./scripts/docker/update_requirements.sh
  docker pull "$python_image"
fi

### --platform wins; else the builder's own platform when loading (a manifest list cannot be loaded) and
### `$platforms` when pushing.
platform_args=()
if [ -n "$platform" ]; then
  platform_args=(--platform "$platform")
elif [ "$push" = "--push" ]; then
  platform_args=(--platform "$platforms")
fi

build() {
  ### build <target or ""> <dep_group> <tags...>
  local target="$1" dep_group="$2"; shift 2
  local target_args=() tag_args=() t
  [ -n "$target" ] && target_args=(--target "$target")
  for t in "$@"; do tag_args+=(-t "$t"); done
  echo "Building tags (target ${target:-default}): $*"
  run docker buildx build --progress plain "$push" \
    ${target_args[@]+"${target_args[@]}"} \
    --build-arg dep_group="$dep_group" \
    --build-arg mrsm_uid="$mrsm_uid" \
    --build-arg mrsm_gid="$mrsm_gid" \
    ${platform_args[@]+"${platform_args[@]}"} \
    "${tag_args[@]}" \
    .
}

### The tags one image is built under: all of them, or (--arch-tags) only its architecture's own tag,
### from which --merge builds the rest.
tags_for() {
  if [ -n "$arch_tags" ]; then
    echo "$image:$mrsm_version-$1-$(arch_of "$platform")"
  else
    final_tags "$1"
  fi
}

for tag in "${tags[@]}"; do
  target=""
  if [ -z "$v4_only" ] && [ "$tag" == "full" ]; then
    target="full"
  fi
  specific_tags=()
  while IFS= read -r t; do specific_tags+=("$t"); done < <(tags_for "$tag")
  build "$target" "$tag" "${specific_tags[@]}"
done

if [ -z "$v4_only" ]; then
  slim_tags=()
  if [ -n "$arch_tags" ]; then
    while IFS= read -r t; do slim_tags+=("$t"); done < <(tags_for slim)
  else
    slim_tags=("$image:$mrsm_version-slim")
    [ -z "$prerelease" ] && slim_tags+=("$image:slim")
  fi
  build "slim" "minimal" "${slim_tags[@]}"
fi

### Smoke-test the images that were loaded locally (scripts/docker/smoke_image.sh): `curl` in every
### image (the stack's API healthcheck), `git` and `ssh` in full, and the API healthcheck run for
### real inside the full image. A failure fails the build. `SKIP_SMOKE=1` skips it.
if [ "$push" = "--load" ] && [ -z "${SKIP_SMOKE:-}" ] && [ -z "$dry_run" ]; then
  smoke_args=()
  for tag in "${tags[@]}"; do
    if [ "$tag" == "full" ]; then
      smoke_args+=(--full-image "$image:$mrsm_version-$tag")
    else
      smoke_args+=(--image "$image:$mrsm_version-$tag")
    fi
  done
  [ -z "$v4_only" ] && smoke_args+=(--slim-image "$image:$mrsm_version-slim")
  ./scripts/docker/smoke_image.sh "${smoke_args[@]}"
fi

if [ -n "${GITHUB_OUTPUT:-}" ]; then
  echo "slim_image=$image:$mrsm_version-slim" >> "$GITHUB_OUTPUT"
  echo "full_image=$image:$mrsm_version-full" >> "$GITHUB_OUTPUT"
fi
