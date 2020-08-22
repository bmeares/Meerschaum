FROM mariadb:latest
MAINTAINER Bennett Meares <bennett.meares@gmail.com>
ADD ./src /src
RUN /src/setup.sh
