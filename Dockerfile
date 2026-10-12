### Meerschaum images (docs/design/v5-build.md).
###
### Targets:
###   (default) `docker build .`           v4 image, unchanged: Fedora 44 + the `dep_group` requirements
###                                        (`--build-arg dep_group=minimal|api|full`, default `full`).
###   --target full                        the default image plus the native `mrsm` binary (and the
###                                        `meerschaum._core` extension once scripts/rust/build_py.sh exists).
###   --target slim                        a Fedora 44 rootfs (dnf --installroot) with python3, the base
###                                        `meerschaum` wheel (no extras) and the native `mrsm` binary.
###                                        Size gate: < 200 MB.
###   --target rust-builder / py-builder / slim-rootfs   build stages only.
###
### The `full` and `slim` targets compile the Cargo workspace; the default target never does, so the
### v4 build keeps working while the crates are scaffolds. Requires BuildKit (`docker buildx build`).

### ---------------------------------------------------------------------------------------------
### rust-builder: `cargo build --release -p meerschaum-cli` (+ the PyO3 `_core` if build_py.sh exists).
### Fedora 44 (not rust:1-slim) so the binary and extension link against the same glibc and Python
### as the Fedora 44 runtimes. rust-toolchain.toml is honored through rustup.
FROM fedora:44 AS rust-builder

ENV RUSTUP_HOME=/usr/local/rustup \
    CARGO_HOME=/usr/local/cargo \
    PATH=/usr/local/cargo/bin:$PATH \
    CARGO_TERM_COLOR=never \
    CARGO_INCREMENTAL=0 \
    CARGO_PROFILE_RELEASE_STRIP=symbols

RUN dnf install -y --setopt=install_weak_deps=False --nodocs \
      gcc gcc-c++ make pkgconf-pkg-config \
      python3 python3-pip git curl \
  && dnf clean all \
  && curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs \
     | sh -s -- -y --profile minimal --default-toolchain none --no-modify-path

WORKDIR /src
COPY rust-toolchain.toml ./
RUN rustup toolchain install && rustc --version

COPY Cargo.toml Cargo.lock ./
COPY .cargo ./.cargo
COPY crates ./crates
### `target/` and the registry live in BuildKit cache mounts, so this layer only holds /out.
RUN --mount=type=cache,id=mrsm-v5-cargo-registry,target=/usr/local/cargo/registry \
    --mount=type=cache,id=mrsm-v5-cargo-target,target=/src/target,sharing=locked \
    cargo build --release --locked -p meerschaum-cli \
  && install -D -m 0755 target/release/mrsm /out/bin/mrsm \
  && mkdir -p /out/core

### Optional: the `meerschaum._core` extension, built by scripts/rust/build_py.sh into meerschaum/_core/.
### Whatever compiled extension it leaves under meerschaum/ is copied to /out/core with its path.
COPY scripts/rust ./scripts/rust
COPY pyproject.toml setup.py setup.cfg README.md LICENSE NOTICE MANIFEST.in ./
COPY meerschaum ./meerschaum
RUN --mount=type=cache,id=mrsm-v5-cargo-registry,target=/usr/local/cargo/registry \
    --mount=type=cache,id=mrsm-v5-cargo-target,target=/src/target,sharing=locked \
    if [ -x scripts/rust/build_py.sh ]; then \
      python3 -m pip install --no-cache-dir 'maturin>=1.9,<2.0' \
      && scripts/rust/build_py.sh \
      && find meerschaum \( -name '_core*.so' -o -name '_core*.pyd' \) -exec cp --parents {} /out/core/ \; ; \
    else \
      echo "scripts/rust/build_py.sh not present: skipping meerschaum._core"; \
    fi \
  && find /out -type f -exec ls -l {} \;


### ---------------------------------------------------------------------------------------------
### py-builder: the base `meerschaum` wheel with no extras, installed into /install for `slim`.
FROM fedora:44 AS py-builder

RUN dnf install -y --setopt=install_weak_deps=False --nodocs python3 python3-pip && dnf clean all
WORKDIR /src
COPY pyproject.toml setup.py setup.cfg README.md LICENSE NOTICE MANIFEST.in ./
COPY meerschaum ./meerschaum
RUN --mount=type=bind,from=rust-builder,source=/out/core,target=/core \
    python3 -m pip wheel --no-deps --no-cache-dir -w /wheels . \
  && python3 -m pip install --no-deps --no-cache-dir --no-compile --prefix=/install /wheels/meerschaum-*.whl \
  && rm -f /install/bin/mrsm \
  && site="$(find /install -type d -path '*/site-packages/meerschaum' | head -n 1)" \
  && test -n "$site" \
  && if [ -d /core/meerschaum ]; then cp -a /core/meerschaum/. "$site"/; fi


### ---------------------------------------------------------------------------------------------
### runtime: the v4 image (unchanged). Also the base of `full` and the default target.
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

### Layer 2: Install Python packages.
### Only rebuilds cache if dependencies have changed.
COPY --chown=$MRSM_USER:$MRSM_USER requirements $MRSM_HOME/requirements
RUN python -m pip install --user --no-cache-dir \
  -r $MRSM_HOME/requirements/$MRSM_DEP_GROUP.txt && \
  rm -rf $MRSM_HOME/requirements

### Layer 3: Install Meerschaum.
### Recache this every build.
COPY --chown=$MRSM_USER:$MRSM_USER pyproject.toml setup.py README.md $MRSM_SRC/
COPY --chown=$MRSM_USER:$MRSM_USER meerschaum $MRSM_SRC/meerschaum
RUN python -m pip install --user --no-cache-dir $MRSM_SRC && rm -rf $MRSM_SRC

### Start up Meerschaum to bootstrap its environment.
RUN cd $MRSM_WORK_DIR && [ "$MRSM_DEP_GROUP" != "minimal" ] && \
  MRSM_VENVS_DIR=$MRSM_HOME/venvs python -m meerschaum show version && \
  MRSM_VENVS_DIR=$MRSM_HOME/venvs python -m meerschaum upgrade packages _internal -y || \
  MRSM_VENVS_DIR=$MRSM_HOME/venvs python -m meerschaum --version

VOLUME $MRSM_ROOT_DIR
ENTRYPOINT ["python", "-m", "meerschaum"]


### ---------------------------------------------------------------------------------------------
### full: the v4 image plus the native binary. `mrsm` on PATH resolves to the binary
### (/usr/local/bin precedes ~/.local/bin); the entrypoint stays `python -m meerschaum`.
FROM runtime AS full

COPY --from=rust-builder /out/bin/mrsm /usr/local/bin/mrsm
RUN --mount=type=bind,from=rust-builder,source=/out/core,target=/tmp/mrsm-core \
    if [ -d /tmp/mrsm-core/meerschaum ]; then \
      site="$(python -c 'import importlib.util as u; print(u.find_spec("meerschaum").submodule_search_locations[0])')" \
      && cp -a /tmp/mrsm-core/meerschaum/. "$site"/; \
    fi


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
COPY --from=rust-builder /out/bin/mrsm /rootfs/usr/local/bin/mrsm


### ---------------------------------------------------------------------------------------------
### slim: python3, the base wheel (no extras) and the native binary on the slim-rootfs. Size gate: < 200 MB.
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
### The last stage is what `docker build .` produces: the unchanged v4 image.
FROM runtime AS default
