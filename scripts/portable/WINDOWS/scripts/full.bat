@ECHO OFF
SET DIR="%~dp0"
SET ROOT=%DIR%\..

ECHO About to create a full Meerschaum portable archive.

PAUSE

PUSHD %DIR%
CALL install.bat
PUSHD %DIR%
CALL compress.bat
PUSHD %DIR%
CALL uninstall.bat
PUSHD %DIR%
IF EXIST mrsm-full-windows.zip (
  DEL /q /f mrsm-full-windows.zip
)
REN mrsm.zip mrsm-full-windows.zip
ECHO Created file mrsm-full-windows.zip

PAUSE
