FROM python:3.11-slim-bookworm AS runtime

ARG dep_group=full \
    mrsm_user=meerschaum \
    mrsm_root_dir=/meerschaum

ENV MRSM_USER=$mrsm_user \
    MRSM_DEP_GROUP=$dep_group \
    MRSM_USER=meerschaum \
    MRSM_ROOT_DIR=$mrsm_root_dir \
    MRSM_WORK_DIR=$mrsm_root_dir \
    MRSM_RUNTIME=docker \
    MRSM_HOME=/home/meerschaum \
    MRSM_SRC=/home/meerschaum/src \
    PATH="/home/meerschaum/.local/bin/:$PATH"

### Layer 1: Install static dependencies.
### Should not rebuild cache unless the base Python image has changed.
COPY scripts/docker/image_setup.sh /setup/
RUN /setup/image_setup.sh && rm -rf /setup/

### From this point on, run as a non-privileged user for security.
USER $MRSM_USER
WORKDIR $MRSM_WORK_DIR

### Layer 2: Install Python packages.
### Only rebuilds cache if dependencies have changed.
COPY --chown=$MRSM_USER:$MRSM_USER requirements $MRSM_HOME/requirements
RUN python -m pip install --user --no-cache-dir -r $MRSM_HOME/requirements/$MRSM_DEP_GROUP.txt && \
  rm -rf $MRSM_HOME/requirements

### Layer 3: Install Meerschaum.
### Recache this every build.
COPY --chown=$MRSM_USER:$MRSM_USER setup.py README.md $MRSM_SRC/
COPY --chown=$MRSM_USER:$MRSM_USER meerschaum $MRSM_SRC/meerschaum
RUN python -m pip install --user --no-cache-dir $MRSM_SRC && rm -rf $MRSM_SRC

### Start up Meerschaum to bootstrap its environment.
RUN cd $MRSM_WORK_DIR && [ "$MRSM_DEP_GROUP" != "minimal" ] && \
  mrsm show version || \
  mrsm --version

COPY --chown=$MRSM_USER:$MRSM_USER scripts/docker/entrypoint.sh /mrsm-entrypoint.sh
ENTRYPOINT ["python", "-m", "meerschaum"]
