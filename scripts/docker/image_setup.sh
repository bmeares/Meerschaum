#! /usr/bin/env bash

### Create the user and home directory.
groupadd -r $MRSM_USER \
  && useradd -r -g $MRSM_USER $MRSM_USER \
  && mkdir -p $MRSM_HOME \
  && mkdir -p $MRSM_WORK_DIR \
  && chown -R $MRSM_USER $MRSM_HOME \
  && chown -R $MRSM_USER $MRSM_ROOT_DIR \
  && chown -R $MRSM_USER $MRSM_WORK_DIR

### We need sudo to switch from root to the user.
apt-get update && apt-get install sudo -y --no-install-recommends

### Install user-level build tools.
sudo -u $MRSM_USER python -m pip install --user --upgrade wheel pip setuptools

if [ "$MRSM_DEP_GROUP" != "minimal" ]; then
  apt-get install -y --no-install-recommends \
    g++ \
    make \
    libpq-dev \
    libffi-dev \
    python3-dev \
    || exit 1

  ### Install graphics dependencies for the full version only.
  if [ "$MRSM_DEP_GROUP" == "full" ]; then
    apt-get install -y --no-install-recommends \
      libglib2.0-dev \
      libgirepository1.0-dev \
      libcairo2-dev \
      pkg-config \
      libgtk-3-dev \
      gir1.2-webkit2-4.0 \
      || exit 1
  fi

  sudo -u $MRSM_USER python -m pip install --no-cache-dir --upgrade --user psycopg2 || exit 1
  sudo -u $MRSM_USER python -m pip install --no-cache-dir --upgrade --user pandas || exit 1
fi


### Remove apt lists, sudo, and cache.
### We're done installing system-level packages,
### so prevent futher packages from being installed.
apt-get clean && \
  apt-get purge -s sudo && \
  rm -rf /var/lib/apt/lists/*


### Also remove python3-dev and dependencies to get the image size down.
if [ "$MRSM_DEP_GROUP" != "minimal" ]; then
  apt-get purge -y $(apt-get -s purge python3-dev | grep '^ ' | tr -d '*')
fi
