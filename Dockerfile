### Meerschaum runtime variants (all use the same Rust engine).
###   rust: native binary, no Python or toolchain.
###   rust-full: native binary + Cargo/rustc and native build tools, no Python.
###   slim: Python base wheel, no extras or toolchain (minimal compatibility variant).
###   full/default: native binary, Python SDK + all extras, Cargo/rustc and build tools.
### BuildKit is required. `api` remains an alias of full; it is not a separate build.

ARG sdk_source=source

FROM fedora:44 AS rust-toolchain

ARG TARGETARCH

ENV RUSTUP_HOME=/usr/local/rustup \
    CARGO_HOME=/usr/local/cargo \
    PATH=/usr/local/cargo/bin:$PATH \
    CARGO_TERM_COLOR=never \
    CARGO_INCREMENTAL=0

RUN --mount=type=cache,id=mrsm-dnf-${TARGETARCH},target=/var/cache/libdnf5,sharing=locked \
    dnf install -y --setopt=install_weak_deps=False --nodocs \
      gcc gcc-c++ make pkgconf-pkg-config \
      git curl binutils \
  && curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs \
     | sh -s -- -y --profile minimal --default-toolchain none --no-modify-path

WORKDIR /src
COPY rust-toolchain.toml ./
RUN rustup toolchain install && rustup default stable && rustc --version

### Seed only the small dependency set needed by single-file Rust plugins. Other crates
### still use the normal Cargo registry. No vendor override or build artifacts are shipped.
FROM rust-toolchain AS plugin-dependencies
RUN mkdir -p /plugin/src \
  && printf '%s\n' '[package]' 'name = "mrsm-plugin-dependencies"' 'version = "0.0.0"' 'edition = "2024"' \
       '[dependencies]' 'serde = { version = "1", features = ["derive"] }' \
       'serde_json = { version = "1", features = ["preserve_order", "arbitrary_precision", "raw_value"] }' \
       > /plugin/Cargo.toml \
  && touch /plugin/src/lib.rs \
  && CARGO_HOME=/plugin-cargo cargo fetch --manifest-path /plugin/Cargo.toml

### Administrator-built immutable compiler material. The generated-source lock is
### reviewed with the release; only this build stage fetches its exact dependencies.
### Runtime compilation uses the fixed protected policy and frozen vendor tree.
### Built without Cargo.toml so the ~300 MB snapshot stays cached across patch releases
### and is shared by `full` and `rust-full`; `compiler-policy` writes only the 200-byte
### release-bound /etc/meerschaum/native-runtime.json.
FROM plugin-dependencies AS confined-compiler
ARG TARGETARCH
RUN --mount=type=cache,id=mrsm-dnf-${TARGETARCH},target=/var/cache/libdnf5,sharing=locked \
    dnf install -y --setopt=install_weak_deps=False --nodocs python3
COPY scripts/docker/native_compiler_snapshot.py scripts/docker/build_native_compiler_bundle.py /compiler-tools/
COPY scripts/docker/compiler_source/ /compiler-source/
RUN CARGO_HOME=/plugin-cargo cargo vendor --locked --versioned-dirs \
      --manifest-path /compiler-source/Cargo.toml /compiler-vendor > /compiler-vendor-config.toml \
  && python3 /compiler-tools/build_native_compiler_bundle.py --snapshot-only \
      --toolchain "$(rustc --print sysroot)" --vendor /compiler-vendor \
      --lock /compiler-source/Cargo.lock \
      --settings /usr/local/rustup/settings.toml --output /confined-compiler

FROM rust-toolchain AS source-builder

COPY Cargo.toml Cargo.lock ./
COPY .cargo ./.cargo
COPY crates ./crates
FROM source-builder AS native-builder
RUN --mount=type=cache,id=mrsm-v5-cargo-registry,target=/usr/local/cargo/registry \
    --mount=type=cache,id=mrsm-v5-cargo-target,target=/src/target,sharing=locked \
    cargo build --release --locked -p meerschaum --bin mrsm \
  && install -D target/release/mrsm /out/mrsm \
  && strip --strip-unneeded /out/mrsm

FROM source-builder AS rust-builder
ARG TARGETARCH
RUN --mount=type=cache,id=mrsm-dnf-${TARGETARCH},target=/var/cache/libdnf5,sharing=locked \
    dnf install -y --setopt=install_weak_deps=False --nodocs python3 python3-pip patchelf \
  && python3 -m pip install --no-cache-dir 'maturin>=1.9,<2.0'
COPY pyproject.toml README.md LICENSE NOTICE _meerschaum_mrsm.py ./
COPY meerschaum ./meerschaum
### `target/` and the registry live in BuildKit cache mounts, so this layer only holds /out/wheels.
### `meerschaum/_core/_native*.so` from a developer's tree is excluded by the pyproject (and by
### .dockerignore), so the wheel always carries the freshly compiled extension.
RUN --mount=type=cache,id=mrsm-v5-cargo-registry,target=/usr/local/cargo/registry \
    --mount=type=cache,id=mrsm-v5-cargo-target,target=/src/target,sharing=locked \
    maturin build --release --locked -o /out/wheels \
  && ls -l /out/wheels


FROM native-builder AS native-source
FROM rust-builder AS wheel-source
FROM scratch AS native-artifacts
COPY --from=sdk /mrsm /out/mrsm
FROM scratch AS wheel-artifacts
COPY --from=sdk /wheels /out/wheels
FROM native-${sdk_source} AS native-selected
FROM wheel-${sdk_source} AS wheel-selected

### Tiny build-platform Python helper shared by pure-Python attestation stages so arm64 builds
### hash natively on $BUILDPLATFORM instead of under QEMU.
FROM --platform=$BUILDPLATFORM fedora:44 AS py-tool
ARG BUILDARCH
RUN --mount=type=cache,id=mrsm-dnf-${BUILDARCH},target=/var/cache/libdnf5,sharing=locked \
    dnf install -y --setopt=install_weak_deps=False --nodocs python3 curl-minimal

### Hash the completed native installation with builder-only Python. The runtime
### receives only the fixed manifest, never this tool or the builder interpreter.
FROM py-tool AS factory-manifest-tools
COPY scripts/docker/write_native_factory_manifest.py \
     scripts/docker/native_compiler_snapshot.py \
     scripts/docker/build_native_compiler_bundle.py /factory-tools/
COPY Cargo.toml /factory-workspace.toml
ARG TARGETARCH
ARG mrsm_uid=1000

### Bind the release-independent compiler snapshot to this workspace release version
### without invalidating or re-copying the snapshot itself.
FROM factory-manifest-tools AS compiler-policy
RUN --mount=type=bind,from=confined-compiler,source=/confined-compiler/rootfs,target=/compiler-snapshot,readonly \
    python3 /factory-tools/build_native_compiler_bundle.py --policy-only \
      --snapshot-rootfs /compiler-snapshot \
      --manifest /factory-workspace.toml \
      --output /compiler-policy

### ---------------------------------------------------------------------------------------------
### py-base: shared builder stage for installing Python wheels/extras into /install.
FROM fedora:44 AS py-base
ARG TARGETARCH

RUN --mount=type=cache,id=mrsm-dnf-${TARGETARCH},target=/var/cache/libdnf5,sharing=locked \
    dnf install -y --setopt=install_weak_deps=False --nodocs python3 python3-pip binutils findutils

### ---------------------------------------------------------------------------------------------
### py-slim: the Meerschaum wheel alone (no extras, --no-deps), installed into /install.
### Shared by both `slim` (`minimal`) and `full` so only this volatile layer changes per release.
FROM py-base AS py-slim

RUN --mount=type=bind,from=wheel-selected,source=/out/wheels,target=/wheels \
    --mount=type=bind,source=scripts/docker/trim_install.sh,target=/trim_install.sh \
    python3 -m pip install --no-index --no-deps --find-links /wheels --no-cache-dir --no-compile \
      --root-user-action=ignore --prefix=/install meerschaum \
  && /trim_install.sh /install \
  && ls -l /install/bin


### ---------------------------------------------------------------------------------------------
### py-full-deps: third-party extras selected by `dep_group` (`full`, `api`, `minimal`), without
### the Meerschaum wheel. Cached across releases until `requirements/*.txt` changes.
### `--ignore-installed` so a package already provided by the builder's own Python is still
### installed into the prefix that is copied to the final image.
FROM py-base AS py-full-deps

ARG TARGETARCH
ARG dep_group=full

COPY requirements/full.txt requirements/cli-tools.txt requirements/gui.txt \
     requirements/sql-mssql.txt requirements/sql-oracle.txt \
     requirements/sql-duckdb.txt requirements/sql-cockroachdb.txt /reqs/
RUN --mount=type=bind,source=scripts/docker/trim_install.sh,target=/trim_install.sh \
    --mount=type=cache,id=mrsm-pip-${TARGETARCH},target=/root/.cache/pip \
    case "$dep_group" in \
      minimal) \
        mkdir -p /install ;; \
      *) \
        python3 -m pip install --no-compile --ignore-installed \
          --root-user-action=ignore --prefix=/install \
          -r /reqs/full.txt -r /reqs/cli-tools.txt -r /reqs/gui.txt \
          -r /reqs/sql-mssql.txt -r /reqs/sql-oracle.txt \
          -r /reqs/sql-duckdb.txt -r /reqs/sql-cockroachdb.txt ;; \
    esac \
  && MRSM_KEEP_HEADERS=1 /trim_install.sh /install \
  && if [ -d /install/bin ]; then ls -l /install/bin; fi


### ---------------------------------------------------------------------------------------------
### duckdb-driver: DuckDB's own shared library, optionally pre-installed in the image.
### DuckDB is not compiled into Meerschaum: it loads DuckDB's official `libduckdb` at run time and
### downloads it on first use. `--build-arg duckdb_driver=1` downloads it here instead (checksum from
### the pinned value in the Rust source, same file as `mrsm install driver duckdb` writes) and the
### rootfs stages copy it to /usr/local/lib/meerschaum/duckdb/libduckdb.so. That is outside the root
### (`MRSM_ROOT_DIR` is a VOLUME: a mounted volume hides anything baked into it, and the first use
### would download the library again), and the `slim` / `full` stages set `MRSM_DUCKDB_LIB` to it, only
### when the arg is set (an empty `MRSM_DUCKDB_LIB` counts as unset). The default (empty) leaves the
### image without it: /out is empty and nothing is copied. Both images ship `libstdc++`, which
### DuckDB's library needs.
FROM fedora:44 AS duckdb-driver

ARG TARGETARCH
ARG duckdb_driver=

RUN --mount=type=bind,source=scripts/rust/duckdb_lib.sh,target=/scripts/rust/duckdb_lib.sh \
    --mount=type=bind,source=crates/meerschaum-connector-sql/src/duckdb_lib,target=/crates/meerschaum-connector-sql/src/duckdb_lib \
    --mount=type=cache,id=mrsm-dnf-${TARGETARCH},target=/var/cache/libdnf5,sharing=locked \
    mkdir -p /out \
  && if [ -n "$duckdb_driver" ]; then \
       dnf install -y --setopt=install_weak_deps=False --nodocs python3 curl-minimal \
       && lib="$(MRSM_CI_CACHE=/tmp/duckdb-cache bash /scripts/rust/duckdb_lib.sh)" \
       && mkdir -p /out/usr/local/lib/meerschaum/duckdb \
       && cp "$lib" /out/usr/local/lib/meerschaum/duckdb/libduckdb.so \
       && chmod 0755 /out/usr/local/lib/meerschaum/duckdb/libduckdb.so; \
     fi


### ---------------------------------------------------------------------------------------------
### Full images include DuckDB without forcing Python callers away from their Python driver.
### Install the C API on the system library path; do not set an explicit driver override.
FROM fedora:44 AS full-duckdb-driver
ARG TARGETARCH
RUN --mount=type=bind,source=scripts/rust/duckdb_lib.sh,target=/scripts/rust/duckdb_lib.sh \
    --mount=type=bind,source=crates/meerschaum-connector-sql/src/duckdb_lib,target=/crates/meerschaum-connector-sql/src/duckdb_lib \
    --mount=type=cache,id=mrsm-dnf-${TARGETARCH},target=/var/cache/libdnf5,sharing=locked \
    dnf install -y --setopt=install_weak_deps=False --nodocs python3 curl-minimal \
  && lib="$(MRSM_CI_CACHE=/tmp/duckdb-cache bash /scripts/rust/duckdb_lib.sh)" \
  && install -D -m 0755 "$lib" /out/usr/local/lib/libduckdb.so

### slim-os-rootfs: a Fedora 44 root filesystem with only python3 and its runtime libraries (and
### `curl`, which the stack's API healthcheck runs: `curl --fail http://localhost:<port>/healthcheck`;
### `curl-minimal` + `libcurl-minimal` add ~6 MB, the full `libcurl` twice that), installed
### with `dnf --installroot` so the image carries no dnf, rpm tooling, compilers or pip. The rpm
### database is kept so image scanners still see the package list. The CA trust *sources*
### (`update-ca-trust` input; the extracted bundles stay), the Java/EDK2 trust stores, the
### OpenSSL FIPS provider module (never loaded outside FIPS mode) and the `posix/` and `right/`
### copies of the zoneinfo tree, the Fedora repository GPG keys, `pydoc_data` (the `help()` topics)
### and the interpreter's `config-3*` (embedding) directory are removed: ~8 MB that the `slim`
### budget needs. `slim` also drops `ldconfig`, which only `ctypes.util.find_library`
### calls (it returns None without it).
FROM fedora:44 AS slim-os-rootfs

ARG TARGETARCH
ARG mrsm_user=meerschaum \
    mrsm_root_dir=/meerschaum \
    mrsm_uid=1000 \
    mrsm_gid=1000

RUN --mount=type=cache,id=mrsm-dnf-${TARGETARCH},target=/var/cache/libdnf5,sharing=locked \
    dnf install -y --installroot=/rootfs --use-host-config --releasever=44 \
      --setopt=install_weak_deps=False --nodocs \
      python3 python-unversioned-command bash coreutils-single ca-certificates tzdata vim-minimal libstdc++ curl-minimal libcurl-minimal \
  && dnf --installroot=/rootfs --use-host-config clean all \
  && ln -sf vi /rootfs/usr/bin/vim \
  && rm -rf /rootfs/var/cache/* /rootfs/var/log/* /rootfs/var/lib/dnf /rootfs/usr/share/locale/* \
  && rm -rf /rootfs/usr/share/pki/ca-trust-source /rootfs/usr/share/zoneinfo/right \
            /rootfs/usr/share/zoneinfo/posix /rootfs/etc/pki/ca-trust/extracted/java \
            /rootfs/etc/pki/ca-trust/extracted/edk2 /rootfs/usr/lib64/ossl-modules/fips.so \
            /rootfs/usr/lib64/python3*/pydoc_data /rootfs/usr/lib64/python3*/config-3* \
            /rootfs/usr/share/pki/rpm-gpg /rootfs/etc/pki/rpm-gpg \
  && find /rootfs/usr/lib64/python3* -name '*.opt-[12].pyc' -delete \
  && rm -f /rootfs/usr/bin/ldconfig \
  && echo "$mrsm_user:x:$mrsm_gid:" >> /rootfs/etc/group \
  && echo "$mrsm_user:x:$mrsm_uid:$mrsm_gid::/home/$mrsm_user:/bin/bash" >> /rootfs/etc/passwd \
  && install -d -o "$mrsm_uid" -g "$mrsm_gid" "/rootfs/home/$mrsm_user" "/rootfs$mrsm_root_dir" \
  && install -d -m 0700 -o "$mrsm_uid" -g "$mrsm_gid" "/rootfs$mrsm_root_dir/executors"

### `mrsm` is a Python console script now (the CLI runs from the extension), so every start imports the
### launcher's modules. The wheel is installed without byte code and /usr/local is root-owned at run
### time, so Python would compile them again on every start (about 6 ms; the interpreter's own library
### has its byte code from the rpm). Run the CLI once in the root filesystem, as root, to write it,
### then export only the generated `.pyc` files so `slim` layers `slim-os-rootfs` + `py-slim` cleanly.
### A warm-up that fails means the image's `mrsm` cannot start, so it fails the build (the root and
### plugins directories must exist: the CLI refuses a root that is not a directory).
FROM slim-os-rootfs AS slim-warm
COPY --from=py-slim /install/ /rootfs/usr/local/
RUN mkdir -p /rootfs/tmp/mrsm-warm/root /rootfs/tmp/mrsm-warm/plugins \
  && chroot /rootfs env HOME=/tmp/mrsm-warm MRSM_ROOT_DIR=/tmp/mrsm-warm/root MRSM_PLUGINS_DIR=/tmp/mrsm-warm/plugins \
       MRSM_NO_AUTO_INSTALL=1 PATH=/usr/local/bin:/usr/bin mrsm --version >/dev/null; \
  status=$?; rm -rf /rootfs/tmp/mrsm-warm; exit $status
RUN mkdir -p /warm-out \
  && (cd /rootfs && find usr/local -name '*.pyc' -exec cp --parents -a -t /warm-out/ {} +)


### ---------------------------------------------------------------------------------------------
### slim: python3 (also as `python`) and the wheel (no extras) on the slim-os-rootfs. Size gate: ≤ 250 MB.
FROM scratch AS slim

ARG duckdb_driver= \
    mrsm_user=meerschaum \
    mrsm_root_dir=/meerschaum \
    mrsm_uid=1000 \
    mrsm_gid=1000

ENV MRSM_DUCKDB_LIB=${duckdb_driver:+/usr/local/lib/meerschaum/duckdb/libduckdb.so} \
    MRSM_USER=$mrsm_user \
    MRSM_DEP_GROUP=slim \
    MRSM_NO_AUTO_INSTALL=1 \
    MRSM_UID=$mrsm_uid \
    MRSM_GID=$mrsm_gid \
    MRSM_ROOT_DIR=$mrsm_root_dir \
    MRSM_WORK_DIR=$mrsm_root_dir \
    MRSM_RUNTIME=docker \
    MRSM_HOME=/home/$mrsm_user \
    HOME=/home/$mrsm_user \
    PATH=/usr/local/bin:/usr/bin \
    LANG=C.UTF-8

# Stable OS + optional DuckDB layers first, volatile Meerschaum wheel + warmed bytecode on top.
COPY --from=slim-os-rootfs /rootfs/ /
COPY --from=duckdb-driver /out/ /
COPY --from=py-slim /install/ /usr/local/
COPY --from=slim-warm /warm-out/ /

USER $MRSM_UID:$MRSM_GID
WORKDIR $MRSM_ROOT_DIR
VOLUME $MRSM_ROOT_DIR
ENTRYPOINT ["mrsm"]


### ---------------------------------------------------------------------------------------------
### full-os-rootfs: the slim rootfs (which has `libstdc++` too: DuckDB's library and numpy, pyarrow, pandas
### and the other compiled extras link it dynamically) plus `pip` (plugins and `attempt_import` install their requirements into
### `mrsm` venvs with the interpreter's pip), the few interactive tools the image has always shipped (tmux for
### `mrsm` sessions, less, htop, openssl, `python` -> python3, `curl` for the stack's API healthcheck, and
### `git` (`git-core`, which brings `ssh`) so plugins can require `git+https://...` packages: the v4 images
### had all three; ~46 MB), and the user's dotfiles. Kept free of Meerschaum/Python extras so it stays
### cached across releases.
FROM fedora:44 AS full-os-rootfs

ARG TARGETARCH
ARG mrsm_user=meerschaum \
    mrsm_root_dir=/meerschaum \
    mrsm_uid=1000 \
    mrsm_gid=1000

RUN --mount=type=cache,id=mrsm-dnf-${TARGETARCH},target=/var/cache/libdnf5,sharing=locked \
    dnf install -y --installroot=/rootfs --use-host-config --releasever=44 \
      --setopt=install_weak_deps=False --nodocs \
      python3 python-unversioned-command bash coreutils-single ca-certificates tzdata \
      vim-minimal libstdc++ python3-pip python3-devel ncurses tmux less htop openssl curl-minimal libcurl-minimal git-core postgresql \
      gcc gcc-c++ make cmake pkgconf-pkg-config openssl-devel libffi-devel unixODBC unixODBC-devel \
  && dnf --installroot=/rootfs --use-host-config clean all \
  && ln -sf vi /rootfs/usr/bin/vim \
  && rm -rf /rootfs/var/cache/* /rootfs/var/log/* /rootfs/var/lib/dnf /rootfs/usr/share/locale/* \
  && rm -rf /rootfs/usr/share/pki/ca-trust-source /rootfs/usr/share/zoneinfo/right \
            /rootfs/usr/share/zoneinfo/posix /rootfs/etc/pki/ca-trust/extracted/java \
            /rootfs/etc/pki/ca-trust/extracted/edk2 /rootfs/usr/lib64/ossl-modules/fips.so \
            /rootfs/usr/lib64/python3*/pydoc_data \
            /rootfs/usr/share/pki/rpm-gpg /rootfs/etc/pki/rpm-gpg \
  && find /rootfs/usr/lib64/python3* -name '*.opt-[12].pyc' -delete \
  && echo "$mrsm_user:x:$mrsm_gid:" >> /rootfs/etc/group \
  && echo "$mrsm_user:x:$mrsm_uid:$mrsm_gid::/home/$mrsm_user:/bin/bash" >> /rootfs/etc/passwd \
  && install -d -o "$mrsm_uid" -g "$mrsm_gid" \
      "/rootfs/home/$mrsm_user" "/rootfs/home/$mrsm_user/.cache" "/rootfs/home/$mrsm_user/.cache/pycache" \
      "/rootfs/home/$mrsm_user/.cargo" \
      "/rootfs$mrsm_root_dir" \
  && install -d -m 0700 -o "$mrsm_uid" -g "$mrsm_gid" "/rootfs$mrsm_root_dir/executors" \
  && install -d -m 0755 -o 0 -g 0 /rootfs/var/lib/meerschaum \
  && install -d -m 0700 -o "$mrsm_uid" -g "$mrsm_gid" /rootfs/var/lib/meerschaum/factory
COPY --chown=$mrsm_uid:$mrsm_gid scripts/docker/bashrc /rootfs/home/$mrsm_user/.bashrc
COPY --chown=$mrsm_uid:$mrsm_gid scripts/docker/tmux.conf /rootfs/home/$mrsm_user/.tmux.conf

### The image sets PYTHONPYCACHEPREFIX, and with it Python reads byte code only from that directory (the
### interpreter's own library included), so a fresh container compiled the standard library and the
### launcher's modules at every start (measured: about 130 ms against 35 ms for `mrsm show version`).
### Run the CLI once as the image's user to fill the prefix, then copy only that tiny pycache directory
### into `full`. A warm-up that fails means the image's `mrsm` cannot start, so it fails the build.
FROM full-os-rootfs AS full-warm
ARG mrsm_user=meerschaum \
    mrsm_uid=1000 \
    mrsm_gid=1000
COPY --from=py-full-deps /install/ /rootfs/usr/local/
COPY --from=py-slim /install/ /rootfs/usr/local/
RUN mkdir -p /rootfs/tmp/mrsm-warm/root /rootfs/tmp/mrsm-warm/plugins \
  && chown -R "$mrsm_uid:$mrsm_gid" /rootfs/tmp/mrsm-warm \
  && chroot --userspec="$mrsm_uid:$mrsm_gid" /rootfs env HOME="/home/$mrsm_user" \
       PYTHONPYCACHEPREFIX="/home/$mrsm_user/.cache/pycache" \
       MRSM_ROOT_DIR=/tmp/mrsm-warm/root MRSM_PLUGINS_DIR=/tmp/mrsm-warm/plugins MRSM_NO_AUTO_INSTALL=1 \
       PATH=/usr/local/bin:/usr/bin python3 -P -m meerschaum --version >/dev/null; \
  status=$?; rm -rf /rootfs/tmp/mrsm-warm; exit $status

FROM factory-manifest-tools AS full-attested
ARG TARGETARCH
ARG mrsm_uid=1000
COPY --from=py-slim /install/ /attest-root/usr/local/
RUN mkdir -p /attest-root/usr/local/bin /attest-root/usr/local/lib/meerschaum/compiler /attest-root/etc/meerschaum
RUN --mount=type=bind,from=confined-compiler,source=/confined-compiler/rootfs/usr/local/lib/meerschaum/compiler,target=/attest-root/usr/local/lib/meerschaum/compiler,readonly \
    --mount=type=bind,from=compiler-policy,source=/compiler-policy/etc/meerschaum,target=/attest-root/etc/meerschaum,readonly \
    --mount=type=bind,from=native-selected,source=/out,target=/attest-root/usr/local/bin,readonly \
    set -eu; \
    case "$TARGETARCH" in amd64) architecture=x86_64 ;; arm64) architecture=aarch64 ;; *) exit 1 ;; esac; \
    sdk_root=; \
    for candidate in /attest-root/usr/local/lib/python*/site-packages /attest-root/usr/local/lib64/python*/site-packages; do \
      if [ -f "$candidate/meerschaum/__init__.py" ]; then \
        test -z "$sdk_root"; sdk_root="${candidate#/attest-root}"; \
      fi; \
    done; \
    test -n "$sdk_root"; \
    release="$(python3 -c 'import tomllib; print(tomllib.load(open("/factory-workspace.toml", "rb"))["workspace"]["package"]["version"])')"; \
    python3 /factory-tools/write_native_factory_manifest.py --rootfs /attest-root \
      --release "$release" --architecture "$architecture" --sdk-root "$sdk_root" --runtime-uid "$mrsm_uid"


### ---------------------------------------------------------------------------------------------
### full: native CLI, Python SDK + extras, and Rust/native build toolchains.
###
### The packages are installed without byte code (it is ~125 MB for `[full]`). Python compiles
### each module on first import and caches the result under $PYTHONPYCACHEPREFIX (user-owned, so
### the root-owned site-packages stays read-only); mount a volume there to keep the cache across
### containers.
FROM scratch AS full

ARG dep_group=full \
    duckdb_driver= \
    mrsm_user=meerschaum \
    mrsm_root_dir=/meerschaum \
    mrsm_uid=1000 \
    mrsm_gid=1000

ENV MRSM_DUCKDB_LIB=${duckdb_driver:+/usr/local/lib/meerschaum/duckdb/libduckdb.so} \
    MRSM_USER=$mrsm_user \
    MRSM_DEP_GROUP=$dep_group \
    RUSTUP_HOME=/usr/local/rustup \
    CARGO_HOME=/home/$mrsm_user/.cargo \
    MRSM_UID=$mrsm_uid \
    MRSM_GID=$mrsm_gid \
    MRSM_ROOT_DIR=$mrsm_root_dir \
    MRSM_WORK_DIR=$mrsm_root_dir \
    MRSM_RUNTIME=docker \
    MRSM_HOME=/home/$mrsm_user \
    MRSM_SRC=/home/$mrsm_user/src \
    HOME=/home/$mrsm_user \
    PATH=/usr/local/cargo/bin:/usr/local/bin:/usr/bin:/home/$mrsm_user/.local/bin \
    PYTHONPYCACHEPREFIX=/home/$mrsm_user/.cache/pycache \
    LANG=C.UTF-8

# Stable layers (cached across releases; compiler + toolchain layers shared with `rust-full`).
COPY --from=full-os-rootfs /rootfs/ /
COPY --from=duckdb-driver /out/ /
COPY --from=full-duckdb-driver /out/ /
COPY --from=confined-compiler /confined-compiler/rootfs/usr/local/lib/meerschaum/compiler/ /usr/local/lib/meerschaum/compiler/
COPY --from=confined-compiler /confined-compiler/compat/ /
COPY --from=rust-toolchain /usr/local/cargo/bin/ /usr/local/cargo/bin/
COPY --chown=$mrsm_uid:$mrsm_gid --from=plugin-dependencies /plugin-cargo/registry/ /home/$mrsm_user/.cargo/registry/
COPY --from=py-full-deps /install/ /usr/local/
# Volatile layers (change per release; wheel shared with `slim`, `mrsm` shared with `rust`/`rust-full`).
COPY --from=py-slim /install/ /usr/local/
COPY --from=native-selected /out/mrsm /usr/local/bin/mrsm
COPY --from=compiler-policy /compiler-policy/etc/meerschaum/ /etc/meerschaum/
COPY --from=full-attested /attest-root/usr/local/share/meerschaum/native-factory.json /usr/local/share/meerschaum/native-factory.json
COPY --chown=$mrsm_uid:$mrsm_gid --from=full-warm /rootfs/home/$mrsm_user/.cache/pycache/ /home/$mrsm_user/.cache/pycache/

USER $MRSM_UID:$MRSM_GID
WORKDIR $MRSM_WORK_DIR
VOLUME $MRSM_ROOT_DIR
ENTRYPOINT ["mrsm"]


### ---------------------------------------------------------------------------------------------
### Native runtimes. The final root filesystems never receive the builder's Python.
### OS rootfs stages stay free of `mrsm` so `rust-full-os-rootfs` does not rerun `dnf install`
### when only the binary changes.
FROM fedora:44 AS rust-os-rootfs
ARG TARGETARCH
ARG mrsm_user=meerschaum mrsm_root_dir=/meerschaum mrsm_uid=1000 mrsm_gid=1000
RUN --mount=type=cache,id=mrsm-dnf-${TARGETARCH},target=/var/cache/libdnf5,sharing=locked \
    dnf install -y --installroot=/rootfs --use-host-config --releasever=44 \
      --setopt=install_weak_deps=False --nodocs \
      bash coreutils-single ca-certificates tzdata libstdc++ curl-minimal vim-minimal \
  && dnf --installroot=/rootfs --use-host-config clean all \
  && rm -rf /rootfs/var/cache/* /rootfs/var/log/* /rootfs/var/lib/dnf /rootfs/usr/share/locale/* \
  && rm -rf /rootfs/usr/share/pki/ca-trust-source /rootfs/usr/share/zoneinfo/right \
            /rootfs/usr/share/zoneinfo/posix /rootfs/etc/pki/ca-trust/extracted/java \
            /rootfs/etc/pki/ca-trust/extracted/edk2 /rootfs/usr/lib64/ossl-modules/fips.so \
            /rootfs/usr/share/pki/rpm-gpg /rootfs/etc/pki/rpm-gpg \
  && ln -sf vi /rootfs/usr/bin/vim \
  && echo "$mrsm_user:x:$mrsm_gid:" >> /rootfs/etc/group \
  && echo "$mrsm_user:x:$mrsm_uid:$mrsm_gid::/home/$mrsm_user:/bin/bash" >> /rootfs/etc/passwd \
  && install -d -o "$mrsm_uid" -g "$mrsm_gid" "/rootfs/home/$mrsm_user" "/rootfs$mrsm_root_dir" \
  && install -d -m 0700 -o "$mrsm_uid" -g "$mrsm_gid" "/rootfs$mrsm_root_dir/executors" \
  && install -d -m 0755 -o 0 -g 0 /rootfs/var/lib/meerschaum \
  && install -d -m 0700 -o "$mrsm_uid" -g "$mrsm_gid" /rootfs/var/lib/meerschaum/factory

FROM rust-os-rootfs AS rust-full-os-rootfs
ARG TARGETARCH
ARG mrsm_user=meerschaum mrsm_uid=1000 mrsm_gid=1000
RUN --mount=type=cache,id=mrsm-dnf-${TARGETARCH},target=/var/cache/libdnf5,sharing=locked \
    dnf install -y --installroot=/rootfs --use-host-config --releasever=44 \
      --setopt=install_weak_deps=False --nodocs \
      gcc gcc-c++ make cmake pkgconf-pkg-config openssl-devel git-core \
  && dnf --installroot=/rootfs --use-host-config clean all \
  && rm -rf /rootfs/var/cache/* /rootfs/var/log/* /rootfs/var/lib/dnf /rootfs/usr/share/locale/* \
  && rm -rf /rootfs/usr/share/pki/ca-trust-source /rootfs/usr/share/zoneinfo/right \
            /rootfs/usr/share/zoneinfo/posix /rootfs/etc/pki/ca-trust/extracted/java \
            /rootfs/etc/pki/ca-trust/extracted/edk2 /rootfs/usr/lib64/ossl-modules/fips.so \
            /rootfs/usr/share/pki/rpm-gpg /rootfs/etc/pki/rpm-gpg \
  && install -d -o "$mrsm_uid" -g "$mrsm_gid" "/rootfs/home/$mrsm_user/.cargo"

FROM factory-manifest-tools AS rust-attested
ARG TARGETARCH
ARG mrsm_uid=1000
RUN mkdir -p /attest-root/usr/local/bin
RUN --mount=type=bind,from=native-selected,source=/out,target=/attest-root/usr/local/bin,readonly \
    set -eu; \
    case "$TARGETARCH" in amd64) architecture=x86_64 ;; arm64) architecture=aarch64 ;; *) exit 1 ;; esac; \
    release="$(python3 -c 'import tomllib; print(tomllib.load(open("/factory-workspace.toml", "rb"))["workspace"]["package"]["version"])')"; \
    python3 /factory-tools/write_native_factory_manifest.py --rootfs /attest-root \
      --release "$release" --architecture "$architecture" --runtime-uid "$mrsm_uid"

FROM factory-manifest-tools AS rust-full-attested
ARG TARGETARCH
ARG mrsm_uid=1000
RUN mkdir -p /attest-root/usr/local/bin /attest-root/usr/local/lib/meerschaum/compiler /attest-root/etc/meerschaum
RUN --mount=type=bind,from=confined-compiler,source=/confined-compiler/rootfs/usr/local/lib/meerschaum/compiler,target=/attest-root/usr/local/lib/meerschaum/compiler,readonly \
    --mount=type=bind,from=compiler-policy,source=/compiler-policy/etc/meerschaum,target=/attest-root/etc/meerschaum,readonly \
    --mount=type=bind,from=native-selected,source=/out,target=/attest-root/usr/local/bin,readonly \
    set -eu; \
    case "$TARGETARCH" in amd64) architecture=x86_64 ;; arm64) architecture=aarch64 ;; *) exit 1 ;; esac; \
    release="$(python3 -c 'import tomllib; print(tomllib.load(open("/factory-workspace.toml", "rb"))["workspace"]["package"]["version"])')"; \
    python3 /factory-tools/write_native_factory_manifest.py --rootfs /attest-root \
      --release "$release" --architecture "$architecture" --runtime-uid "$mrsm_uid"


FROM scratch AS rust
ARG duckdb_driver= mrsm_user=meerschaum mrsm_root_dir=/meerschaum mrsm_uid=1000 mrsm_gid=1000
ENV MRSM_DUCKDB_LIB=${duckdb_driver:+/usr/local/lib/meerschaum/duckdb/libduckdb.so} \
    MRSM_USER=$mrsm_user MRSM_DEP_GROUP=rust MRSM_NO_AUTO_INSTALL=1 \
    MRSM_UID=$mrsm_uid MRSM_GID=$mrsm_gid MRSM_ROOT_DIR=$mrsm_root_dir \
    MRSM_WORK_DIR=$mrsm_root_dir MRSM_RUNTIME=docker \
    HOME=/home/$mrsm_user MRSM_HOME=/home/$mrsm_user PATH=/usr/local/bin:/usr/bin LANG=C.UTF-8
COPY --from=rust-os-rootfs /rootfs/ /
COPY --from=duckdb-driver /out/ /
COPY --from=native-selected /out/mrsm /usr/local/bin/mrsm
COPY --from=rust-attested /attest-root/usr/local/share/meerschaum/native-factory.json /usr/local/share/meerschaum/native-factory.json
USER $MRSM_UID:$MRSM_GID
WORKDIR $MRSM_ROOT_DIR
VOLUME $MRSM_ROOT_DIR
ENTRYPOINT ["mrsm"]

FROM scratch AS rust-full
ARG duckdb_driver= mrsm_user=meerschaum mrsm_root_dir=/meerschaum mrsm_uid=1000 mrsm_gid=1000
ENV MRSM_DUCKDB_LIB=${duckdb_driver:+/usr/local/lib/meerschaum/duckdb/libduckdb.so} \
    MRSM_USER=$mrsm_user MRSM_DEP_GROUP=rust-full MRSM_NO_AUTO_INSTALL=1 \
    MRSM_UID=$mrsm_uid MRSM_GID=$mrsm_gid MRSM_ROOT_DIR=$mrsm_root_dir \
    MRSM_WORK_DIR=$mrsm_root_dir MRSM_RUNTIME=docker \
    HOME=/home/$mrsm_user MRSM_HOME=/home/$mrsm_user \
    RUSTUP_HOME=/usr/local/rustup CARGO_HOME=/home/$mrsm_user/.cargo \
    PATH=/usr/local/cargo/bin:/usr/local/bin:/usr/bin LANG=C.UTF-8
COPY --from=rust-full-os-rootfs /rootfs/ /
COPY --from=duckdb-driver /out/ /
COPY --from=confined-compiler /confined-compiler/rootfs/usr/local/lib/meerschaum/compiler/ /usr/local/lib/meerschaum/compiler/
COPY --from=confined-compiler /confined-compiler/compat/ /
COPY --from=rust-toolchain /usr/local/cargo/bin/ /usr/local/cargo/bin/
COPY --chown=$mrsm_uid:$mrsm_gid --from=plugin-dependencies /plugin-cargo/registry/ /home/$mrsm_user/.cargo/registry/
COPY --from=native-selected /out/mrsm /usr/local/bin/mrsm
COPY --from=compiler-policy /compiler-policy/etc/meerschaum/ /etc/meerschaum/
COPY --from=rust-full-attested /attest-root/usr/local/share/meerschaum/native-factory.json /usr/local/share/meerschaum/native-factory.json
USER $MRSM_UID:$MRSM_GID
WORKDIR $MRSM_ROOT_DIR
VOLUME $MRSM_ROOT_DIR
ENTRYPOINT ["mrsm"]

### `docker build .` remains the complete, beginner-friendly image.
FROM full AS default
