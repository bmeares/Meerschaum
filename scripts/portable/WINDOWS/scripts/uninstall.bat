@ECHO OFF
SET DIR="%~dp0"
SET ROOT=%DIR%\..
PUSHD %ROOT%

IF NOT EXIST .\scripts\_site-packages_original (
  ECHO Not installed. Continuing...
) ELSE (
  PUSHD .\scripts\
  REN _site-packages_original site-packages
  PUSHD %ROOT%
  rmdir /s /q .\python\Lib\site-packages
  MOVE /Y .\scripts\site-packages .\python\Lib >NUL
)

PUSHD %ROOT%
RMDIR /s /q root
MKDIR root
