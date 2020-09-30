FROM python:3.8-slim-buster as common-base
MAINTAINER Bennett Meares <bennett.meares@gmail.com>

FROM common-base as base-builder

RUN mkdir -p /src
WORKDIR /src

FROM base-builder as dependencies

### Step 1: extract dependencies for caching
COPY setup.py .
COPY README.md .
COPY meerschaum/config/_version.py ./meerschaum/config/
RUN python setup.py egg_info

### Step 2: Install dependencies
FROM base-builder as builder
RUN mkdir -p /install
COPY --from=dependencies /src/meerschaum.egg-info/requires.txt /tmp/
RUN sh -c 'pip install --no-warn-script-location --prefix=/install $(sed "s/[[][^]]*[]]//g" /tmp/requires.txt)'

### copy project files
COPY . .

### Step 3: install Meerschaum
RUN sh -c 'pip install --no-warn-script-location --prefix=/install .[full]'

### Step 4: install into clean image
FROM common-base

RUN mkdir -p /src
WORKDIR /src
COPY --from=builder /install /usr/local

### default: launch into the mrsm shell
ENTRYPOINT ["mrsm"]
