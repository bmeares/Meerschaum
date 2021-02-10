@ECHO OFF
SET DIR="%~dp0"
SET ROOT=%DIR%\..
cd %ROOT%

IF NOT EXIST .\scripts\_site-packages_original (
  ECHO Not installed. Continuing...
) ELSE (
  cd .\scripts\
  REN _site-packages_original site-packages
  cd %ROOT%
  rmdir /s /q .\python\Lib\site-packages
  MOVE /Y .\scripts\site-packages .\python\Lib >NUL
)

cd %ROOT%
rmdir /s /q root
mkdir root