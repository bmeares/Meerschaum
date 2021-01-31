FROM python:3.8-slim-buster
MAINTAINER Bennett Meares <bennett.meares@gmail.com>

RUN mkdir -p /src
WORKDIR /src

COPY setup.py README.md ./
COPY meerschaum ./meerschaum
RUN pip install --no-cache-dir .[full]

### default: launch into the mrsm shell
ENTRYPOINT ["mrsm"]
