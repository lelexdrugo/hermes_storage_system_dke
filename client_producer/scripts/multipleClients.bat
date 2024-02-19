@echo off
setlocal ENABLEDELAYEDEXPANSION

set nfile=%1
set dimension=%2

set startTime=%time%
set myTitle=MAIN_%RANDOM%_%CD%

for /l %%x in (1, 1, %nfile%) do (
   set filename=!random!_!random!_!random!
   echo %%x
   start /B "myTitle" "singleClient.bat" !filename! %dimension%
)

echo Clients started!

:waittofinish
echo At least one process is still running...
timeout /T 2 /nobreak >nul
tasklist.exe /fi "WINDOWTITLE eq %myTitle%" | find ":" >nul
if errorlevel 1 goto waittofinish
echo Finished!

echo Start Time: %startTime%
echo This doesn't work. Doesn't wait for jar executable to finish. Finish Time: %time%
PAUSE