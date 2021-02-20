@ECHO OFF
SET MRSM_ROOT_DIR=root
SET MRSM_RUNTIME=portable
SET DIR="%~dp0"
%DIR%\python\python.exe -m meerschaum %*
