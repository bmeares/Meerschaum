#!/usr/bin/env bash
# harness editor: add a connector + a comment to the file given as $1
python3 - "$1" <<'PY'
import sys,re
p=sys.argv[1]; s=open(p).read()
s=s.replace("connectors:\n  sql:\n", "connectors:\n  sql:\n    trn_edited:\n      flavor: sqlite\n      database: /nonexistent-trn/edited.db\n",1)
open(p,'w').write(s)
PY
