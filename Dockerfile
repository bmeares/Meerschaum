FROM python:3.7-slim-buster AS runtime
MAINTAINER Bennett Meares <bennett.meares@gmail.com>

ARG dep_group=full
ENV dep_group $dep_group

WORKDIR /src

### Layer 1: Install static dependencies.
### Does not rebuild cache.
RUN python -m pip install wheel pip ; \
    [ "$dep_group" != "minimal" ] && \
      apt-get update && \
      apt-get install -y --no-install-recommends \
        gcc libpq-dev python3-dev python3-venv && \
      apt-get clean && \
      rm -rf /var/lib/apt/lists/* && \
      python -m pip install --no-cache-dir --upgrade psycopg2 ; \
    [ "$dep_group" != "minimal" ] && \
      apt-get purge -y `apt-get -s purge python3-dev | grep '^ ' | tr -d '*'` ; \
    exit 0

### Layer 2: Install Python packages.
### Only rebuilds cache if dependencies have changed.
COPY requirements /requirements
RUN python -m pip install -r /requirements/$dep_group.txt && rm -rf /requirements

### Layer 3: Install Meerschaum.
### Rebuilds every build.
COPY setup.py README.md ./
COPY meerschaum ./meerschaum
RUN python -m pip install --no-cache-dir . && cd /root && rm -rf /src

WORKDIR /root
ENTRYPOINT ["python", "-m", "meerschaum"]
