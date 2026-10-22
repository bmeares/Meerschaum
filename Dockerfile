### Meerschaum images (docs/design/v5-build.md, docs/design/v5.0.0.md §12.1).
###
### Every image installs the one maturin wheel built from this tree: `meerschaum` (the Python
### package + the `meerschaum._core` extension). Its `mrsm` console script runs the CLI in-process
### from the extension (D8, revised), so the image's `mrsm` is that script.
###
### Targets:
###   (default) `docker build .`           the `full` image; `--build-arg dep_group=` selects the
###                                        extras: `full` (default: `[full]`), `api`
###                                        (`[full]`) or `minimal` (the base wheel, no extras).
###   --target full                        the same image (kept so `buildx.sh` can name the target).
###                                        Size gate: < 900 MB.
###   --target slim                        the wheel, no extras. Size gate: ≤ 250 MB.
###   --target rust-builder / py-slim / py-full / slim-rootfs / full-rootfs   build stages only.
###   --build-arg duckdb_driver=1          pre-install DuckDB's shared library at
###                                        /usr/local/lib/meerschaum/duckdb/libduckdb.so and set
###                                        MRSM_DUCKDB_LIB to it (outside the root volume, which hides
###                                        anything baked into it). Default: downloaded on first use
###                                        into the root (`mrsm install driver duckdb`).
###
### Both images are assembled the same way, so neither carries a package manager or a toolchain:
###   1. `rust-builder` compiles the wheel once (BuildKit cache mounts keep `target/`).
###   2. `py-slim` / `py-full` `pip install` the wheels (and the extras, for `full`) into a prefix
###      with `--no-compile`, then `scripts/docker/trim_install.sh` drops tests, headers, byte code
###      and debug symbols (the `[full]` extras are ~890 MB untrimmed).
###   3. `slim-rootfs` / `full-rootfs` are Fedora 44 root filesystems made with `dnf --installroot`
###      (python3 and its libraries only; no dnf, rpm tooling, compilers or pip) with the prefix
###      copied into /usr/local.
###   4. `slim` / `full` are `FROM scratch` + that rootfs.
###
### Requires BuildKit (`docker buildx build`).
###
### Every stage supports linux/amd64 and linux/arm64. Docker CI uses self-hosted x86_64 runners:
### amd64 natively, arm64 under QEMU, with persistent caches per architecture. Prereleases select only
### amd64; finals build both and join them with `buildx.sh --merge`.

### ---------------------------------------------------------------------------------------------
### rust-builder: `maturin build --release` for the `meerschaum` wheel.
### Fedora 44 (not rust:1-slim) so the extension links against the same glibc and Python
### as the Fedora 44 runtimes. rust-toolchain.toml is honored through rustup. The release profile
### is the workspace's (Cargo.toml `[profile.release]`), the same as the PyPI wheels'.
FROM fedora:44 AS rust-builder

ENV RUSTUP_HOME=/usr/local/rustup \
    CARGO_HOME=/usr/local/cargo \
    PATH=/usr/local/cargo/bin:$PATH \
    CARGO_TERM_COLOR=never \
    CARGO_INCREMENTAL=0

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
COPY pyproject.toml README.md LICENSE NOTICE _meerschaum_mrsm.py ./
COPY meerschaum ./meerschaum
### `target/` and the registry live in BuildKit cache mounts, so this layer only holds /out/wheels.
### `meerschaum/_core/_native*.so` from a developer's tree is excluded by the pyproject (and by
### .dockerignore), so the wheel always carries the freshly compiled extension.
RUN --mount=type=cache,id=mrsm-v5-cargo-registry,target=/usr/local/cargo/registry \
    --mount=type=cache,id=mrsm-v5-cargo-target,target=/src/target,sharing=locked \
    maturin build --release --locked -o /out/wheels \
  && ls -l /out/wheels


### ---------------------------------------------------------------------------------------------
### py-slim: the wheel with no extras, installed into /install.
FROM fedora:44 AS py-slim

RUN dnf install -y --setopt=install_weak_deps=False --nodocs python3 python3-pip binutils findutils \
  && dnf clean all
RUN --mount=type=bind,from=rust-builder,source=/out/wheels,target=/wheels \
    --mount=type=bind,source=scripts/docker/trim_install.sh,target=/trim_install.sh \
    python3 -m pip install --no-index --find-links /wheels --no-cache-dir --no-compile \
      --root-user-action=ignore --prefix=/install meerschaum \
  && /trim_install.sh /install \
  && ls -l /install/bin


### ---------------------------------------------------------------------------------------------
### py-full: the wheel and the extras selected by `dep_group` (`full`, `api`, `minimal`).
### The extras' dependencies come from PyPI; the Meerschaum wheel comes from rust-builder.
### `--ignore-installed` so a package already provided by the builder's own Python is still
### installed into the prefix that is copied to the final image.
### `PyYAML` is not an extra: the Python facade's `meerschaum.utils.yaml` still imports it at the
### first config or pipe operation (D20 moves that into `_core`), and without it the facade would
### try to `pip install` it at run time. Baked in here until the facade stops needing it.
FROM fedora:44 AS py-full

ARG dep_group=full

RUN dnf install -y --setopt=install_weak_deps=False --nodocs python3 python3-pip binutils findutils \
  && dnf clean all
RUN --mount=type=bind,from=rust-builder,source=/out/wheels,target=/wheels \
    --mount=type=bind,source=scripts/docker/trim_install.sh,target=/trim_install.sh \
    case "$dep_group" in \
      minimal) extras=""; set -- ;; \
      *)       extras="[full]"; set -- 'pgcli>=3.1.0' 'litecli>=1.5.0' ;; \
    esac \
  && wheel="$(ls /wheels/meerschaum-[0-9]*.whl)" \
  && python3 -m pip install --find-links /wheels --no-cache-dir --no-compile --ignore-installed \
      --root-user-action=ignore --prefix=/install "$wheel$extras" PyYAML "$@" \
  && /trim_install.sh /install \
  && ls -l /install/bin


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

ARG duckdb_driver=

RUN --mount=type=bind,source=scripts/rust/duckdb_lib.sh,target=/scripts/rust/duckdb_lib.sh \
    --mount=type=bind,source=crates/meerschaum-connector-sql/src/duckdb_lib,target=/crates/meerschaum-connector-sql/src/duckdb_lib \
    mkdir -p /out \
  && if [ -n "$duckdb_driver" ]; then \
       dnf install -y --setopt=install_weak_deps=False --nodocs python3 curl-minimal \
       && lib="$(MRSM_CI_CACHE=/tmp/duckdb-cache bash /scripts/rust/duckdb_lib.sh)" \
       && mkdir -p /out/usr/local/lib/meerschaum/duckdb \
       && cp "$lib" /out/usr/local/lib/meerschaum/duckdb/libduckdb.so \
       && chmod 0755 /out/usr/local/lib/meerschaum/duckdb/libduckdb.so; \
     fi


### ---------------------------------------------------------------------------------------------
### slim-rootfs: a Fedora 44 root filesystem with only python3 and its runtime libraries (and
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
FROM fedora:44 AS slim-rootfs

ARG mrsm_user=meerschaum \
    mrsm_root_dir=/meerschaum \
    mrsm_uid=1000 \
    mrsm_gid=1000

RUN dnf install -y --installroot=/rootfs --use-host-config --releasever=44 \
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
  && install -d -o "$mrsm_uid" -g "$mrsm_gid" "/rootfs/home/$mrsm_user" "/rootfs$mrsm_root_dir"
COPY --from=py-slim /install/ /rootfs/usr/local/
### `mrsm` is a Python console script now (the CLI runs from the extension), so every start imports the
### launcher's modules. The wheel is installed without byte code and /usr/local is root-owned at run
### time, so Python would compile them again on every start (about 6 ms; the interpreter's own library
### has its byte code from the rpm). Run the CLI once in the root filesystem, as root, to write it.
### A warm-up that fails means the image's `mrsm` cannot start, so it fails the build (the root and
### plugins directories must exist: the CLI refuses a root that is not a directory).
RUN mkdir -p /rootfs/tmp/mrsm-warm/root /rootfs/tmp/mrsm-warm/plugins \
  && chroot /rootfs env HOME=/tmp/mrsm-warm MRSM_ROOT_DIR=/tmp/mrsm-warm/root MRSM_PLUGINS_DIR=/tmp/mrsm-warm/plugins \
       MRSM_NO_AUTO_INSTALL=1 PATH=/usr/local/bin:/usr/bin mrsm --version >/dev/null; \
  status=$?; rm -rf /rootfs/tmp/mrsm-warm; exit $status
COPY --from=duckdb-driver /out/ /rootfs/


### ---------------------------------------------------------------------------------------------
### slim: python3 (also as `python`) and the wheel (no extras) on the slim-rootfs. Size gate: ≤ 250 MB.
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
### full-rootfs: the slim rootfs (which has `libstdc++` too: DuckDB's library and numpy, pyarrow, pandas
### and the other compiled extras link it dynamically) plus `pip` (plugins and `attempt_import` install their requirements into
### `mrsm` venvs with the interpreter's pip), the few interactive tools the image has always shipped (tmux for
### `mrsm` sessions, less, htop, openssl, `python` -> python3, `curl` for the stack's API healthcheck, and
### `git` (`git-core`, which brings `ssh`) so plugins can require `git+https://...` packages: the v4 images
### had all three; ~46 MB), the user's dotfiles, and the trimmed `py-full` prefix in /usr/local.
FROM fedora:44 AS full-rootfs

ARG mrsm_user=meerschaum \
    mrsm_root_dir=/meerschaum \
    mrsm_uid=1000 \
    mrsm_gid=1000

RUN dnf install -y --installroot=/rootfs --use-host-config --releasever=44 \
      --setopt=install_weak_deps=False --nodocs \
      python3 python-unversioned-command bash coreutils-single ca-certificates tzdata \
      vim-minimal libstdc++ python3-pip ncurses tmux less htop openssl curl-minimal libcurl-minimal git-core postgresql \
  && dnf --installroot=/rootfs --use-host-config clean all \
  && ln -sf vi /rootfs/usr/bin/vim \
  && rm -rf /rootfs/var/cache/* /rootfs/var/log/* /rootfs/var/lib/dnf /rootfs/usr/share/locale/* \
  && rm -rf /rootfs/usr/share/pki/ca-trust-source /rootfs/usr/share/zoneinfo/right \
            /rootfs/usr/share/zoneinfo/posix /rootfs/etc/pki/ca-trust/extracted/java \
            /rootfs/etc/pki/ca-trust/extracted/edk2 /rootfs/usr/lib64/ossl-modules/fips.so \
            /rootfs/usr/lib64/python3*/pydoc_data /rootfs/usr/lib64/python3*/config-3* \
            /rootfs/usr/share/pki/rpm-gpg /rootfs/etc/pki/rpm-gpg \
  && find /rootfs/usr/lib64/python3* -name '*.opt-[12].pyc' -delete \
  && echo "$mrsm_user:x:$mrsm_gid:" >> /rootfs/etc/group \
  && echo "$mrsm_user:x:$mrsm_uid:$mrsm_gid::/home/$mrsm_user:/bin/bash" >> /rootfs/etc/passwd \
  && install -d -o "$mrsm_uid" -g "$mrsm_gid" \
      "/rootfs/home/$mrsm_user" "/rootfs/home/$mrsm_user/.cache" "/rootfs/home/$mrsm_user/.cache/pycache" \
      "/rootfs$mrsm_root_dir"
COPY --chown=$mrsm_uid:$mrsm_gid scripts/docker/bashrc /rootfs/home/$mrsm_user/.bashrc
COPY --chown=$mrsm_uid:$mrsm_gid scripts/docker/tmux.conf /rootfs/home/$mrsm_user/.tmux.conf
COPY --from=py-full /install/ /rootfs/usr/local/
### The image sets PYTHONPYCACHEPREFIX, and with it Python reads byte code only from that directory (the
### interpreter's own library included), so a fresh container compiled the standard library and the
### launcher's modules at every start (measured: about 130 ms against 35 ms for `mrsm show version`).
### Run the CLI once as the image's user to fill the prefix, which is part of the image.
### A warm-up that fails means the image's `mrsm` cannot start, so it fails the build (the root and
### plugins directories must exist: the CLI refuses a root that is not a directory).
RUN mkdir -p /rootfs/tmp/mrsm-warm/root /rootfs/tmp/mrsm-warm/plugins \
  && chown -R "$mrsm_uid:$mrsm_gid" /rootfs/tmp/mrsm-warm \
  && chroot --userspec="$mrsm_uid:$mrsm_gid" /rootfs env HOME="/home/$mrsm_user" \
       PYTHONPYCACHEPREFIX="/home/$mrsm_user/.cache/pycache" \
       MRSM_ROOT_DIR=/tmp/mrsm-warm/root MRSM_PLUGINS_DIR=/tmp/mrsm-warm/plugins MRSM_NO_AUTO_INSTALL=1 \
       PATH=/usr/local/bin:/usr/bin mrsm --version >/dev/null; \
  status=$?; rm -rf /rootfs/tmp/mrsm-warm; exit $status
COPY --from=duckdb-driver /out/ /rootfs/


### ---------------------------------------------------------------------------------------------
### full: the wheels and their extras (`dep_group`) on the full-rootfs. Size gate: < 900 MB.
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
    MRSM_UID=$mrsm_uid \
    MRSM_GID=$mrsm_gid \
    MRSM_ROOT_DIR=$mrsm_root_dir \
    MRSM_WORK_DIR=$mrsm_root_dir \
    MRSM_RUNTIME=docker \
    MRSM_HOME=/home/meerschaum \
    MRSM_SRC=/home/meerschaum/src \
    HOME=/home/meerschaum \
    PATH=/usr/local/bin:/usr/bin:/home/meerschaum/.local/bin \
    PYTHONPYCACHEPREFIX=/home/meerschaum/.cache/pycache \
    LANG=C.UTF-8

COPY --from=full-rootfs /rootfs/ /

USER $MRSM_UID:$MRSM_GID
WORKDIR $MRSM_WORK_DIR
VOLUME $MRSM_ROOT_DIR
ENTRYPOINT ["mrsm"]


### ---------------------------------------------------------------------------------------------
### The last stage is what `docker build .` produces: the `full` image.
FROM full AS default
