# -*- mode: python ; coding: utf-8 -*-

### My last spec file was overwritten because I didn't realize *.spec files
### were defined in .gitignore. This is my second attempt.

### NOTE: This must be run from the top level of the Meerschaum directory.

from PyInstaller.utils.hooks import collect_data_files
import os, pathlib
from meerschaum.utils.packages import all_packages
from meerschaum.utils.misc import _pyinstaller_traverse_dir
Meerschaum_dir = os.path.join(os.getcwd())

block_cipher = None

a = Analysis(
  ['../meerschaum/__init__.py'],
  pathex = [Meerschaum_dir],
  binaries = [],
  datas = _pyinstaller_traverse_dir(Meerschaum_dir),
#  datas = collect_data_files('meerschaum'),
  hiddenimports = list(all_packages),
  hookspath=[os.path.join(Meerschaum_dir, '__pyinstaller')],
  runtime_hooks = [],
  excludes = [],
  win_no_prefer_redirects = False,
  win_private_assemblies = False,
  cipher = block_cipher,
  noarchive = False
)
pyz = PYZ(
  a.pure,
  a.zipped_data,
  cipher = block_cipher
)
exe = EXE(
  pyz,
  a.scripts,
  [],
  exclude_binaries = True,
  name = 'mrsm',
  debug = False,
  bootloader_ignore_signals = False,
  strip = False,
  upx = True,
  console = True
)
#coll = COLLECT(
#  exe,
#  a.binaries,
#  a.zipfiles,
#  a.datas,
#  strip = False,
#  upx = True,
#  upx_exclude = [],
#  name = 'meerschaum'
#)
