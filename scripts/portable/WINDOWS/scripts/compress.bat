@ECHO OFF
SET DIR="%~dp0"
SET ROOT=%DIR%\..
SET gittar="%PROGRAMFILES%"\Git\usr\bin\tar.exe
SET bsdtar="%WINDIR%"\System32\tar.exe

IF NOT EXIST %gittar% (
  ECHO Could not find tar.exe. Please install Git for Windows: https://gitforwindows.org/
  EXIT
)

CD "%ROOT%"\scripts
ROBOCOPY .\compress\ .\_compress\ /E >NUL

cd %ROOT%\..
ECHO Compressing files, please wait...
%gittar% --exclude='./scripts' -czf ./WINDOWS/scripts/_compress/mrsm.tar.gz -C WINDOWS .
ECHO Done creating tar archive.

CD "%ROOT%"\scripts
ECHO Creating ZIP archive...
powershell Compress-Archive -Path .\_compress\* -DestinationPath .\mrsm.zip -CompressionLevel NoCompression -Force
RMDIR .\_compress /s /q

ECHO Compression complete.
