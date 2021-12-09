@ECHO OFF
SET DIR="%~dp0"
SET ROOT=%DIR%\..

ECHO About to create a minimal Meerschaum portable archive.

PAUSE
ECHO Uninstalling site-packages...
PUSHD %DIR%
CALL uninstall.bat

ECHO Ensuring latest pip...
PUSHD %ROOT%
.\python\python.exe -m pip uninstall pip setuptools -y
.\python\python.exe ..\cache\get-pip.py

PUSHD %DIR%
CALL compress.bat
PUSHD %DIR%
IF EXIST mrsm-minimal-windows.zip (
  DEL /q /f mrsm-minimal-windows.zip
)
REN mrsm.zip mrsm-minimal-windows.zip
ECHO Created file mrsm-minimal-windows.zip

PAUSE
