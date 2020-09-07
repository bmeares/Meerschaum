#! /bin/sh
### Executed inside the image when building

### update system packages
apt-get update && apt-get upgrade -y

### ensure latest version of pip
pip install --upgrade pip

### install pip packages
pip install --upgrade\
  sqlalchemy\
  pandas\
  pyyaml\
  lazy_import\
  pytz\
  numpy\
  python-dateutil\
  psycopg2-binary\
  six
