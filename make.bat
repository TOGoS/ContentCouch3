@echo off

rem If make worked, I wouldn't have a separate make.bat!
rem make.exe "find_exe=C:\Program Files\Git\usr\bin\find.exe" "pathsep=;" 

setlocal

set any_errors=

if not defined UNIX_FIND_EXE echo Plase define UNIX_FIND_EXE>&2 && set any_errors=1

if not defined JAVAC_CMD set "JAVAC_CMD=javac"
if not defined JAR_CMD set "JAR_CMD=jar"

if defined any_errors goto fail

"%UNIX_FIND_EXE%" src/main ext-lib/*/src/main -name '*.java' >.java-main-src.lst
if errorlevel 1 goto fail
rmdir /s/q target\main
mkdir target/main
%JAVAC_CMD% -source 1.7 -target 1.7 -sourcepath src/main/java -d target/main @.java-main-src.lst
if errorlevel 1 goto fail
del CCouch3.jar
%JAR_CMD% -ce togos.ccouch3.CCouch3Command -C target/main . >CCouch3.jar
if errorlevel 1 goto fail

goto eof


:fail
echo make.bat: Failed>&2
exit /B 1


:eof
