#!/usr/bin/env bash
### Build the console's plugin source editor (CodeMirror 6) into one minified script:
###   meerschaum/api/dash/assets/codemirror.bundle.js  (+ codemirror.bundle.LICENSE)
### Versions are pinned in package.json and package-lock.json here; `npm ci` installs exactly
### those. To upgrade: edit package.json, run `npm install --package-lock-only`, then this script,
### and commit the three files with the regenerated bundle. Needs Node.js 20+ and npm.
### The Dash app does not load `*.bundle.js` on every page (`assets_ignore`); the page loads it
### when a file is opened for editing.
set -euo pipefail
here="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
repo="$(cd "$here/../../.." && pwd)"
assets="$repo/meerschaum/api/dash/assets"
cd "$here"
npm ci --no-audit --no-fund --ignore-scripts >/dev/null
# esbuild ships its binary as an optional dependency; `--ignore-scripts` skips its check only.
node_modules/.bin/esbuild entry.mjs --bundle --minify --format=iife --target=es2020 \
  --legal-comments=none --charset=utf8 --outfile="$assets/codemirror.bundle.js"
### Every bundled package's license, in one file next to the bundle.
node --input-type=module - "$assets/codemirror.bundle.LICENSE" <<'JS'
import fs from 'node:fs';
import path from 'node:path';
const out = process.argv[2];
const lock = JSON.parse(fs.readFileSync('package-lock.json', 'utf8'));
const parts = ['Third-party software bundled in codemirror.bundle.js (built by scripts/console/editor/build.sh).\n'];
for (const [key, meta] of Object.entries(lock.packages).sort()) {
  if (!key || meta.dev) continue;
  const dir = key;
  const name = key.replace(/^.*node_modules\//, '');
  const file = fs.readdirSync(dir).find((f) => /^licen[cs]e/i.test(f));
  const text = file ? fs.readFileSync(path.join(dir, file), 'utf8').trim() : `License: ${meta.license}`;
  parts.push(`\n==== ${name}@${meta.version} (${meta.license}) ====\n\n${text}\n`);
}
fs.writeFileSync(out, parts.join(''));
JS
ls -l "$assets/codemirror.bundle.js" "$assets/codemirror.bundle.LICENSE"
