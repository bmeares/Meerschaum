@ECHO OFF
SET DIR="%~dp0"
SET ROOT=%DIR%\..
cd %ROOT%
IF NOT EXIST .\scripts\_site-packages_original (
  ECHO Backing up site-packages...
  .\python\python.exe -m pip install wheel --no-warn-script-location -q
  xcopy .\python\Lib\site-packages\ .\scripts\_site-packages_original\ /E >NUL
)

ECHO Ensuring latest pip...
CD %ROOT%
.\python\python.exe -m pip uninstall pip setuptools -y
.\python\python.exe ..\cache\get-pip.py

ECHO Installing Meerschaum and dependencies (this might take awhile)...
.\python\python.exe -m pip install scripts/install[full] --no-warn-script-location -q

ECHO Finished installing.
