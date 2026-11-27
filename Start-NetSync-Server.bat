@echo off
setlocal EnableExtensions

set "REPO_DIR=%~dp0"
if "%REPO_DIR:~-1%"=="\" set "REPO_DIR=%REPO_DIR:~0,-1%"
set "APP_JAR=%REPO_DIR%\target\netsync-1.0.0-SNAPSHOT.jar"
set "LIB_DIR=%REPO_DIR%\target\lib"

where java >nul 2>&1
if errorlevel 1 (
    echo [ERROR] Java 21 was not found on PATH. Install JDK 21 and try again.
    pause
    exit /b 1
)

if not exist "%APP_JAR%" (
    echo [ERROR] Missing build artifact: "%APP_JAR%"
    echo Run Build-NetSync.bat once before launching the NetSync Server GUI.
    pause
    exit /b 1
)

if not exist "%LIB_DIR%\javafx-controls-*.jar" (
    echo [ERROR] Missing JavaFX runtime libraries in "%LIB_DIR%".
    echo Run Build-NetSync.bat once to prepare runtime dependencies.
    pause
    exit /b 1
)

set "JAVA_EXE=javaw"
where javaw >nul 2>&1
if errorlevel 1 set "JAVA_EXE=java"

pushd "%REPO_DIR%" || exit /b 1
start "NetSync Server" "%JAVA_EXE%" -cp "%APP_JAR%;%LIB_DIR%\*" com.netsync.gui.NetSyncServerGuiLauncher
popd
exit /b 0
