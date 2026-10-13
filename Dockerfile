### Meerschaum images (docs/design/v5-build.md, docs/design/v5.0.0.md §12.1).
###
### Every image installs the two maturin wheels built from this tree: `meerschaum` (the Python
### package + the `meerschaum._core` extension) and `meerschaum-cli` (the `mrsm` binary).
###
### Targets:
###   (default) `docker build .`           Fedora 44 + `meerschaum[<extras>]`; `--build-arg dep_group=`
###                                        selects the extras: `full` (default: `[full]`), `api`
###                                        (`[api,sql,pandas]`) or `minimal` (the base wheel, no extras).
###   --target full                        the same image (kept so `buildx.sh` can name the target).
###   --target slim                        a Fedora 44 rootfs (dnf --installroot) with python3 and the
###                                        two wheels, no extras. Size gate: < 200 MB.
###   --target rust-builder / py-builder / slim-rootfs   build stages only.
###
### Requires BuildKit (`docker buildx build`). The Cargo workspace is compiled once (rust-builder).

### ---------------------------------------------------------------------------------------------
### rust-builder: `maturin build --release` for the `meerschaum` and `meerschaum-cli` wheels.
### Fedora 44 (not rust:1-slim) so the binary and extension link against the same glibc and Python
### as the Fedora 44 runtimes. rust-toolchain.toml is honored through rustup.
FROM fedora:44 AS rust-builder

ENV RUSTUP_HOME=/usr/local/rustup \
    CARGO_HOME=/usr/local/cargo \
    PATH=/usr/local/cargo/bin:$PATH \
    CARGO_TERM_COLOR=never \
    CARGO_INCREMENTAL=0 \
    CARGO_PROFILE_RELEASE_STRIP=symbols \
    CARGO_PROFILE_RELEASE_LTO=thin \
    CARGO_PROFILE_RELEASE_OPT_LEVEL=s
### The last two are image-only size tuning for the slim size gate (§12.1: `slim` < 200 MB): measured
### 224 MB with the workspace's default release profile, 186 MB with these. The PyPI wheels
### (wheels.yaml) keep the default profile.

RUN dnf install -y --setopt=install_weak_deps=False --nodocs \
      gcc gcc-c++ make pkgconf-pkg-config \
      python3 python3-pip git curl patchelf \
  && dnf clean all \
  && python3 -m pip install --no-cache-dir 'maturin>=1.9,<2.0' \
  && curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs \
     | sh -s -- -y --profile minimal --default-toolchain none --no-modify-path

WORKDIR /src
COPY rust-toolchain.toml ./
RUN rustup toolchain install && rustc --version

COPY Cargo.toml Cargo.lock ./
COPY .cargo ./.cargo
COPY crates ./crates
COPY pyproject.toml README.md LICENSE NOTICE ./
COPY meerschaum ./meerschaum
### `target/` and the registry live in BuildKit cache mounts, so this layer only holds /out/wheels.
### `meerschaum/_core/_core*.so` from a developer's tree is excluded by the pyproject (and by
### .dockerignore), so the wheel always carries the freshly compiled extension.
RUN --mount=type=cache,id=mrsm-v5-cargo-registry,target=/usr/local/cargo/registry \
    --mount=type=cache,id=mrsm-v5-cargo-target,target=/src/target,sharing=locked \
    maturin build --release --locked -o /out/wheels \
  && maturin build --release --locked -m crates/meerschaum-cli/Cargo.toml -o /out/wheels \
  && ls -l /out/wheels


### ---------------------------------------------------------------------------------------------
### py-builder: the two wheels with no extras, installed into /install for `slim`.
FROM fedora:44 AS py-builder

RUN dnf install -y --setopt=install_weak_deps=False --nodocs python3 python3-pip && dnf clean all
RUN --mount=type=bind,from=rust-builder,source=/out/wheels,target=/wheels \
    python3 -m pip install --no-index --find-links /wheels --no-cache-dir --no-compile \
      --prefix=/install meerschaum \
  && ls -l /install/bin


### ---------------------------------------------------------------------------------------------
### runtime: Fedora 44 + `meerschaum[<extras>]`. Also the base of `full` and the default target.
FROM fedora:44 AS runtime

ARG dep_group=full \
    mrsm_user=meerschaum \
    mrsm_root_dir=/meerschaum \
    mrsm_uid=1000 \
    mrsm_gid=1000

ENV MRSM_USER=$mrsm_user \
    MRSM_DEP_GROUP=$dep_group \
    MRSM_UID=$mrsm_uid \
    MRSM_GID=$mrsm_gid \
    MRSM_ROOT_DIR=$mrsm_root_dir \
    MRSM_WORK_DIR=$mrsm_root_dir \
    MRSM_RUNTIME=docker \
    MRSM_HOME=/home/meerschaum \
    MRSM_SRC=/home/meerschaum/src \
    PATH=$PATH:/home/meerschaum/.local/bin

### Layer 1: Install static dependencies.
### Should not rebuild cache unless the base Python image has changed.
COPY scripts/docker/image_setup.sh scripts/drivers.sh scripts/docker/dev.sh scripts/docker/sleep_forever.sh /scripts/
RUN /scripts/image_setup.sh

### From this point on, run as a non-privileged user for security.
USER $MRSM_UID:$MRSM_GID
WORKDIR $MRSM_WORK_DIR

### Layer 2: Install Meerschaum and its extras (`dep_group` -> extras, §12.1) from the wheels.
### The extras' dependencies come from PyPI; the two Meerschaum wheels come from rust-builder.
RUN --mount=type=bind,from=rust-builder,source=/out/wheels,target=/wheels \
    --mount=type=cache,target=/home/meerschaum/.cache/pip,uid=$MRSM_UID,gid=$MRSM_GID \
    case "$MRSM_DEP_GROUP" in \
      minimal) extras="" ;; \
      api)     extras="[api,sql,pandas]" ;; \
      *)       extras="[full]" ;; \
    esac \
  && wheel="$(ls /wheels/meerschaum-[0-9]*.whl)" \
  && python -m pip install --user --find-links /wheels "$wheel$extras"
### NOTE: install the wheel by path. `meerschaum[full]` by name would let pip pick the newest
### *stable* release on PyPI over this pre-release build.

### Start up Meerschaum to bootstrap its environment.
RUN cd $MRSM_WORK_DIR && [ "$MRSM_DEP_GROUP" != "minimal" ] && \
  MRSM_VENVS_DIR=$MRSM_HOME/venvs python -m meerschaum show version && \
  MRSM_VENVS_DIR=$MRSM_HOME/venvs python -m meerschaum upgrade packages _internal -y || \
  MRSM_VENVS_DIR=$MRSM_HOME/venvs python -m meerschaum --version

VOLUME $MRSM_ROOT_DIR
ENTRYPOINT ["python", "-m", "meerschaum"]


### ---------------------------------------------------------------------------------------------
### full: the runtime image under the name `buildx.sh` builds for the `full` tag.
FROM runtime AS full


### ---------------------------------------------------------------------------------------------
### slim-rootfs: a Fedora 44 root filesystem with only python3 and its runtime libraries, installed
### with `dnf --installroot` so the image carries no dnf, rpm tooling, compilers or pip. The rpm
### database is kept so image scanners still see the package list.
FROM fedora:44 AS slim-rootfs

ARG mrsm_user=meerschaum \
    mrsm_root_dir=/meerschaum \
    mrsm_uid=1000 \
    mrsm_gid=1000

RUN dnf install -y --installroot=/rootfs --use-host-config --releasever=44 \
      --setopt=install_weak_deps=False --nodocs \
      python3 bash coreutils-single ca-certificates tzdata \
  && dnf --installroot=/rootfs --use-host-config clean all \
  && rm -rf /rootfs/var/cache/* /rootfs/var/log/* /rootfs/var/lib/dnf /rootfs/usr/share/locale/* \
  && find /rootfs/usr/lib64/python3* -name '*.opt-[12].pyc' -delete \
  && echo "$mrsm_user:x:$mrsm_gid:" >> /rootfs/etc/group \
  && echo "$mrsm_user:x:$mrsm_uid:$mrsm_gid::/home/$mrsm_user:/bin/bash" >> /rootfs/etc/passwd \
  && install -d -o "$mrsm_uid" -g "$mrsm_gid" "/rootfs/home/$mrsm_user" "/rootfs$mrsm_root_dir"
COPY --from=py-builder /install/ /rootfs/usr/local/


### ---------------------------------------------------------------------------------------------
### slim: python3 and the two wheels (no extras) on the slim-rootfs. Size gate: < 200 MB.
FROM scratch AS slim

ARG mrsm_user=meerschaum \
    mrsm_root_dir=/meerschaum \
    mrsm_uid=1000 \
    mrsm_gid=1000

ENV MRSM_USER=$mrsm_user \
    MRSM_DEP_GROUP=slim \
    MRSM_UID=$mrsm_uid \
    MRSM_GID=$mrsm_gid \
    MRSM_ROOT_DIR=$mrsm_root_dir \
    MRSM_WORK_DIR=$mrsm_root_dir \
    MRSM_RUNTIME=docker \
    MRSM_HOME=/home/meerschaum \
    HOME=/home/meerschaum \
    PATH=/usr/local/bin:/usr/bin \
    LANG=C.UTF-8

COPY --from=slim-rootfs /rootfs/ /

USER $MRSM_UID:$MRSM_GID
WORKDIR $MRSM_ROOT_DIR
VOLUME $MRSM_ROOT_DIR
ENTRYPOINT ["mrsm"]


### ---------------------------------------------------------------------------------------------
### The last stage is what `docker build .` produces: the runtime image.
FROM runtime AS default
