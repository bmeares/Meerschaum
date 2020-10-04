FROM python:3.8-slim-buster
MAINTAINER Bennett Meares <bennett.meares@gmail.com>

RUN mkdir -p /src
WORKDIR /src

# RUN pip install pgcli --only-binary psycopg2

# RUN apt-get update && apt-get install --no-install-recommends libpq-dev python-dev -y
  # pip install psycopg2-binary

COPY . .
# RUN apt-get install unixodbc-dev postgresql-server-dev-12 -y
RUN pip install --no-cache-dir .[full]

### default: launch into the mrsm shell
ENTRYPOINT ["mrsm"]
