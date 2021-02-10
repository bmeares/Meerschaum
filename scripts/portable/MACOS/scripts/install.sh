#!/usr/bin/env bash
DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" >/dev/null 2>&1 && pwd )"
TERMINFO_DIRS=/usr/share/terminfo \
"$DIR"/../python/bin/python3.9 -m pip install "$DIR"/install[full] --no-warn-script-location
