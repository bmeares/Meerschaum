@ECHO OFF
SET OLD_CD=%CD%
SET DIR="%~dp0"
CD %DIR%root
SET MRSM_ROOT_DIR=%CD%
CD %OLD_CD%
SET MRSM_RUNTIME=portable

%DIR%\python\python.exe -m meerschaum %*
