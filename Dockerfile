FROM python:3.8-slim-buster
MAINTAINER Bennett Meares <bennett.meares@gmail.com>

### copy project files
COPY . /src

### install system requirements
RUN cd /src/ && /src/scripts/setup.sh && rm -rf /src && mkdir -p /src

### install python package and make /src empty 
# RUN pip install --no-cache-dir --upgrade /src[full] && rm -rf /src && mkdir -p /src

### Run post-install script
# ADD ./scripts/post_install.sh /root/post_install.sh
# RUN cd /root/ && /root/post_install.sh && rm -f /root/post_install.sh

### shells launch inside /src (where we mount development files)
WORKDIR /src

### default: launch into the mrsm shell
ENTRYPOINT ["mrsm"]
