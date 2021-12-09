@ECHO OFF

SET DIR="%~dp0"
PUSHD %DIR%\..

CHOICE /C YN /M "Are you sure you want to uninstall Meerschaum Portable? Data will be lost!" /T 10 /D n
IF %ERRORLEVEL% == 2 (
  echo Cancelling uninstall!
  EXIT
)
ECHO Uninstalling Meerschaum Portable. Thank you for trying Meerschaum!
RMDIR /Q /S %DIR%
ECHO Uninstallation complete. You can now close this window.
PAUSE
