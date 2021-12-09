@ECHO OFF
SET DIR="%~dp0"
SET ROOT=%DIR%\..
SET MRSM_ROOT_DIR_REL=%ROOT%\root\
PUSHD %MRSM_ROOT_DIR_REL%
SET MRSM_ROOT_ROOT_DIR=%CD%


PUSHD %ROOT%
IF NOT EXIST .\scripts\_site-packages_original (
  ECHO Backing up site-packages...
  MD .\scripts\_site-packages_original
  .\python\python.exe -m pip install wheel --no-warn-script-location -q
  XCOPY .\python\Lib\site-packages .\scripts\_site-packages_original /E >NUL
)

ECHO Ensuring latest pip...
PUSHD %ROOT%
.\python\python.exe -m pip uninstall pip setuptools -y
.\python\python.exe ..\cache\get-pip.py

ECHO Installing Meerschaum and dependencies (this might take awhile)...
.\python\python.exe -m pip install scripts/install[full] --no-warn-script-location -q

ECHO Finished installing.
