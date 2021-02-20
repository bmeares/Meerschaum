@ECHO OFF
SET DIR="%~dp0"
SET ROOT=%DIR%\..

ECHO About to create a full Meerschaum portable archive.

PAUSE

cd %DIR%
CALL install.bat
cd %DIR%
CALL compress.bat
cd %DIR%
CALL uninstall.bat
cd %DIR%
IF EXIST mrsm-full-windows.zip (
  del /q /f mrsm-full-windows.zip
)
REN mrsm.zip mrsm-full-windows.zip
ECHO Created file mrsm-full-windows.zip

PAUSE