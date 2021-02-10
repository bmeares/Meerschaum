@ECHO OFF

SET DIR="%~dp0"
SET tar=%DIR%\tar.exe
cd %DIR%

ECHO Thank you for installing Meerschaum!
ECHO Meerschaum is open source software written and maintained by Bennett Meares.
ECHO Read more at https://meerschaum.io.
ECHO Please wait while Meerschaum Portable is installed...
%tar% -xf mrsm.tar.gz
CALL mrsm.bat
rm mrsm.tar.gz tar.exe
(GOTO) 2>NUL & DEL "%~f0"