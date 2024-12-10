#! /bin/bash

DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" >/dev/null 2>&1 && pwd )"
. "$DIR"/config.sh
cd "$PARENT"

if [ "$EUID" -ne 0 ]; then
  echo "This script must be run as root.";
  exit 1;
fi

if [ -z "$DISTRO" ]; then
  DISTRO="$(lsb_release -si)";
fi

install_mssql_ubuntu(){
  if ! [[ "18.04 20.04 22.04 24.04 24.10 26.04" == *"$(lsb_release -rs)"* ]]; then
    echo "Ubuntu $(lsb_release -rs) is not currently supported for MSSQL ODBC.";
    exit 1;
  fi
  curl https://packages.microsoft.com/keys/microsoft.asc | apt-key add -
  curl https://packages.microsoft.com/config/ubuntu/$(lsb_release -rs)/prod.list > /etc/apt/sources.list.d/mssql-release.list
  apt-get update
  ACCEPT_EULA=Y apt-get install -y msodbcsql18
  apt-get install -y unixodbc-dev
}


install_mssql_fedora(){
  curl https://packages.microsoft.com/config/rhel/8/prod.repo > /etc/yum.repos.d/mssql-release.repo
  yum remove unixODBC-utf16 unixODBC-utf16-devel
  ACCEPT_EULA=Y yum install -y msodbcsql18
  yum install -y unixODBC-devel
}


install_oracle_ubuntu(){
  add-apt-repository universe
  apt-get update
  apt-get install -y alien

  if [ ! -f /tmp/_oracle.rpm ]; then
    curl https://download.oracle.com/otn_software/linux/instantclient/217000/oracle-instantclient-basic-21.7.0.0.0-1.el8.x86_64.rpm \
      -o /tmp/_oracle.rpm
  fi
  alien -i /tmp/_oracle.rpm
}


install_oracle_fedora(){
  if [ ! -f /tmp/_oracle.rpm ]; then
    curl https://download.oracle.com/otn_software/linux/instantclient/217000/oracle-instantclient-basic-21.7.0.0.0-1.el8.x86_64.rpm \
      -o /tmp/_oracle.rpm
  fi
  yum install /tmp/_oracle.rpm
}

case $DISTRO in
  Ubuntu)
    install_mssql_ubuntu;
    install_oracle_ubuntu;
    ;;
  Debian)
    install_mssql_ubuntu;
    install_oracle_ubuntu;
    ;;
  Fedora)
    install_mssql_fedora;
    install_oracle_fedora;
    ;;
  CentOS)
    install_mssql_fedora;
    install_oracle_fedora;
    ;;
esac
