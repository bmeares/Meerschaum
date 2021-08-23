FROM python:3.9-slim-bullseye AS runtime

ARG dep_group=full
ENV dep_group $dep_group

WORKDIR /src

### Layer 1: Install static dependencies.
### Does not rebuild cache.
COPY scripts/docker/image_setup.sh /setup/
RUN /setup/image_setup.sh && rm -rf /setup/

### Layer 2: Install Python packages.
### Only rebuilds cache if dependencies have changed.
COPY requirements /requirements
RUN python -m pip install --no-cache-dir -r /requirements/$dep_group.txt && rm -rf /requirements

### Layer 3: Install Meerschaum.
### Rebuilds every build.
COPY setup.py README.md ./
COPY meerschaum ./meerschaum
RUN python -m pip install --no-cache-dir . && cd /root && rm -rf /src

### Start up Meerschaum to bootstrap its environment.
WORKDIR /meerschaum
RUN cd /meerschaum && [ "$dep_group" != "minimal" ] && \
  python -m meerschaum show version || \
  python -m meerschaum --version

COPY scripts/docker/entrypoint.sh /entrypoint.sh

ENTRYPOINT ["/entrypoint.sh"]
