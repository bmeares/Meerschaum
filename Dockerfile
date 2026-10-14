### Meerschaum images (docs/design/v5-build.md, docs/design/v5.0.0.md §12.1).
###
### Every image installs the two maturin wheels built from this tree: `meerschaum` (the Python
### package + the `meerschaum._core` extension) and `meerschaum-cli` (the `mrsm` binary).
###
### Targets:
###   (default) `docker build .`           the `full` image; `--build-arg dep_group=` selects the
###                                        extras: `full` (default: `[full]`), `api`
###                                        (`[api,sql,pandas]`) or `minimal` (the base wheel, no extras).
###   --target full                        the same image (kept so `buildx.sh` can name the target).
###                                        Size gate: < 900 MB.
###   --target slim                        the two wheels, no extras. Size gate: < 200 MB.
###   --target rust-builder / py-slim / py-full / slim-rootfs / full-rootfs   build stages only.
###
### Both images are assembled the same way, so neither carries a package manager or a toolchain:
###   1. `rust-builder` compiles the two wheels once (BuildKit cache mounts keep `target/`).
###   2. `py-slim` / `py-full` `pip install` the wheels (and the extras, for `full`) into a prefix
###      with `--no-compile`, then `scripts/docker/trim_install.sh` drops tests, headers, byte code
###      and debug symbols (the `[full]` extras are ~890 MB untrimmed).
###   3. `slim-rootfs` / `full-rootfs` are Fedora 44 root filesystems made with `dnf --installroot`
###      (python3 and its libraries only; no dnf, rpm tooling, compilers or pip) with the prefix
###      copied into /usr/local.
###   4. `slim` / `full` are `FROM scratch` + that rootfs.
###
### Requires BuildKit (`docker buildx build`).

### ---------------------------------------------------------------------------------------------
### rust-builder: `maturin build --release` for the `meerschaum` and `meerschaum-cli` wheels.
### Fedora 44 (not rust:1-slim) so the binary and extension link against the same glibc and Python
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
### py-slim: the two wheels with no extras, installed into /install.
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
### py-full: the wheels and the extras selected by `dep_group` (`full`, `api`, `minimal`).
### The extras' dependencies come from PyPI; the two Meerschaum wheels come from rust-builder.
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
      minimal) extras="" ;; \
      api)     extras="[api,sql,pandas]" ;; \
      *)       extras="[full]" ;; \
    esac \
  && wheel="$(ls /wheels/meerschaum-[0-9]*.whl)" \
  && python3 -m pip install --find-links /wheels --no-cache-dir --no-compile --ignore-installed \
      --root-user-action=ignore --prefix=/install "$wheel$extras" PyYAML \
  && /trim_install.sh /install \
  && ls -l /install/bin


### ---------------------------------------------------------------------------------------------
### slim-rootfs: a Fedora 44 root filesystem with only python3 and its runtime libraries, installed
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
      python3 bash coreutils-single ca-certificates tzdata \
  && dnf --installroot=/rootfs --use-host-config clean all \
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
### full-rootfs: the slim rootfs plus `libstdc++` (numpy, pyarrow, pandas and the other compiled
### extras link it dynamically), `pip` (plugins and `attempt_import` install their requirements into
### `mrsm` venvs with the interpreter's pip), the few interactive tools the image has always shipped (tmux for
### `mrsm` sessions, less, htop, openssl, `python` -> python3), the user's dotfiles, and the
### trimmed `py-full` prefix in /usr/local.
FROM fedora:44 AS full-rootfs

ARG mrsm_user=meerschaum \
    mrsm_root_dir=/meerschaum \
    mrsm_uid=1000 \
    mrsm_gid=1000

RUN dnf install -y --installroot=/rootfs --use-host-config --releasever=44 \
      --setopt=install_weak_deps=False --nodocs \
      python3 python-unversioned-command bash coreutils-single ca-certificates tzdata \
      libstdc++ python3-pip ncurses tmux less htop openssl \
  && dnf --installroot=/rootfs --use-host-config clean all \
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


### ---------------------------------------------------------------------------------------------
### full: the wheels and their extras (`dep_group`) on the full-rootfs. Size gate: < 900 MB.
###
### The packages are installed without byte code (it is ~125 MB for `[full]`). Python compiles
### each module on first import and caches the result under $PYTHONPYCACHEPREFIX (user-owned, so
### the root-owned site-packages stays read-only); mount a volume there to keep the cache across
### containers.
FROM scratch AS full

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
