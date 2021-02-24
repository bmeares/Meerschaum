@ECHO OFF
SET DIR="%~dp0"
SET ROOT=%DIR%\..

ECHO About to create a minimal Meerschaum portable archive.

PAUSE
ECHO Uninstalling site-packages...
cd %DIR%
CALL uninstall.bat

ECHO Ensuring latest pip...
CD %ROOT%
.\python\python.exe -m pip uninstall pip setuptools -y
.\python\python.exe ..\cache\get-pip.py

cd %DIR%
CALL compress.bat
cd %DIR%
IF EXIST mrsm-minimal-windows.zip (
  del /q /f mrsm-minimal-windows.zip
)
REN mrsm.zip mrsm-minimal-windows.zip
ECHO Created file mrsm-minimal-windows.zip

PAUSE
