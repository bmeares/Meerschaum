FROM python:latest
MAINTAINER Bennett Meares <bennett.meares@gmail.com>

### install system requirements
ADD ./scripts/setup.sh /root/setup.sh
RUN cd /root/ && /root/setup.sh && rm -f /root/setup.sh

### install python package
ADD ./src /src
RUN pip install /src

### default: launch into the mrsm shell
ENTRYPOINT ["mrsm"]
