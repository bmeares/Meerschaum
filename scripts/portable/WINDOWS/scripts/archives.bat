@ECHO OFF
SET DIR="%~dp0"
SET ROOT=%DIR%\..

ECHO Will create mrsm-minimal-windows.zip and mrsm-full-windows.zip.
ECHO Please run this after generating the files from Unix with scripts/portable/build.sh.

PAUSE

cd %DIR%
ECHO | CALL minimal.bat

cd %DIR%
ECHO | CALL full.bat

ECHO Finished creating archives.

PAUSE