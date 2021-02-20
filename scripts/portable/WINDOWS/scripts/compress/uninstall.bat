@ECHO OFF

SET DIR="%~dp0"
cd %DIR%\..

CHOICE /C YN /M "Are you sure you want to uninstall Meerschaum Portable? Data will be lost!" /T 10 /D n
IF %ERRORLEVEL% == 2 (
  echo Cancelling uninstall!
  EXIT
)
echo Uninstalling Meerschaum Portable. Thank you for trying Meerschaum!
rmdir /Q /S %DIR%
echo Uninstallation complete. You can now close this window.
PAUSE