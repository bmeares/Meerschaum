#! /bin/sh
### Executed inside the image when building

### set apt-get to noninteractive
export DEBIAN_FRONTEND=noninteractive

### update system packages
apt-get update && apt-get upgrade -y

### install system packages
# apt-get install\
	# nano\
	# -y\
  # --no-install-recommends

### cleanup
apt-get clean
rm -rf /var/lib/apt/lists/*

### ensure latest version of pip
pip install --no-cache-dir --upgrade pip

### install pip packages
pip install --no-cache-dir --upgrade\
  sqlalchemy\
  pandas\
  pyyaml\
  lazy_import\
  pytz\
  numpy\
  python-dateutil\
  psycopg2-binary\
  six\
  uvicorn\
  databases\
  aiosqlite\
  graphene\
  asyncpg\
  cascadict\
  fastapi\
  pydantic\
  pprintpp\
  requests
