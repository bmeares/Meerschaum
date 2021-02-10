@ECHO OFF
SET DIR="%~dp0"
SET ROOT=%DIR%\..
cd %ROOT%
IF NOT EXIST .\scripts\_site-packages_original (
  ECHO Backing up site-packages...
  .\python\python.exe -m pip install wheel -q
  xcopy .\python\Lib\site-packages\ .\scripts\_site-packages_original\ /E >NUL
)

ECHO Installing Meerschaum and dependencies (this might take awhile)...
.\python\python.exe -m pip install scripts/install[full] --no-warn-script-location -q

ECHO Finished installing.