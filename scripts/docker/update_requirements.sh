#! /usr/bin/env bash

DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" >/dev/null 2>&1 && pwd )"
. "$DIR"/../config.sh
cd "$PARENT"

[ ! -d requirements ] && mkdir -p requirements
for t in "${tags[@]}"; do
  new_reqs=$(python -m meerschaum show packages "$t" --nopretty)
  old_reqs=$(cat requirements/"$t".txt 2>/dev/null)
  [ -z "$old_reqs" ] || [ "$new_reqs" != "$old_reqs" ] && echo "$new_reqs" > requirements/"$t".txt
done


