@ECHO OFF

SET DIR="%~dp0"
PUSHD %DIR%
SET bsdtar="%WINDIR%"\System32\tar.exe

IF NOT EXIST %bsdtar% (
  ECHO Could not find tar.exe.
  ECHO If you are running an older version of Windows, Meerschaum might not work!
  ECHO Please extract mrsm.tar.gz with your archive utility ^(e.g. 7-Zip or WinRAR^).
  EXIT
)

ECHO Thank you for installing Meerschaum!
ECHO Meerschaum is open source software written and maintained by Bennett Meares.
ECHO Read more at https://meerschaum.io/
ECHO Please wait while Meerschaum Portable is installed...

%bsdtar% -xf mrsm.tar.gz
DEL /q mrsm.tar.gz
CALL mrsm.bat
(GOTO) 2>NUL & DEL "%~f0"
