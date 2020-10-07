FROM python:3.8-slim-buster
MAINTAINER Bennett Meares <bennett.meares@gmail.com>

RUN mkdir -p /src
WORKDIR /src

# RUN apt-get update && \
  # apt-get install gcc libpq-dev -y && \
  # pip install pgcli \
  # && apt-get purge gcc libpq-dev -y && \
  # apt-get autoremove -y && \
  # apt-get clean

COPY . .
# RUN apt-get install unixodbc-dev postgresql-server-dev-12 -y
# RUN pip install --no-cache-dir .[full,cli]
RUN pip install --no-cache-dir .[full]

### default: launch into the mrsm shell
ENTRYPOINT ["mrsm"]
