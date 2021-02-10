@ECHO OFF
SET DIR="%~dp0"
SET ROOT=%DIR%\..
SET gittar="%PROGRAMFILES%"\Git\usr\bin\tar.exe

IF NOT EXIST %gittar% (
  ECHO Could not find tar.exe from Git for Windows. Please install Git for Windows: https://gitforwindows.org/
  EXIT
) ELSE (
  copy %gittar% %ROOT%\scripts\ >NUL
)
SET tar=%ROOT%\scripts\tar.exe

cd "%ROOT%"\scripts
xcopy .\compress\ .\_compress\ /E >NUL

cd %ROOT%\..
echo Compressing files, please wait...
.\WINDOWS\scripts\tar.exe --exclude='./scripts' -czf ./WINDOWS/scripts/_compress/mrsm.tar.gz -C WINDOWS .
copy %tar% ""%ROOT%"\scripts\_compress\" >NUL

cd "%ROOT%"\scripts
powershell Compress-Archive -Path .\_compress\* -DestinationPath .\mrsm.zip -CompressionLevel NoCompression -Force
rmdir /s /q _compress

echo Compression complete.
