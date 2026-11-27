@echo off
setlocal EnableExtensions

set "REPO_DIR=%~dp0"
if "%REPO_DIR:~-1%"=="\" set "REPO_DIR=%REPO_DIR:~0,-1%"

echo ============================================================
echo  NetSync - One-Time Build and Runtime Preparation
echo ============================================================
echo Repository: "%REPO_DIR%"
echo.

where java >nul 2>&1
if errorlevel 1 (
    echo [ERROR] Java 21 was not found on PATH. Install JDK 21 and try again.
    pause
    exit /b 1
)

where mvn >nul 2>&1
if errorlevel 1 (
    echo [ERROR] Apache Maven was not found on PATH. Install Maven 3.9+ and try again.
    pause
    exit /b 1
)

pushd "%REPO_DIR%" || exit /b 1
echo Building NetSync JAR and copying runtime dependencies (including JavaFX)...
call mvn -B clean package -DskipTests
set "BUILD_EXIT=%ERRORLEVEL%"
popd

if not "%BUILD_EXIT%"=="0" (
    echo.
    echo [ERROR] Build failed with exit code %BUILD_EXIT%.
    pause
    exit /b %BUILD_EXIT%
)

echo.
echo [OK] Build complete.
echo   - Server launcher: Start-NetSync-Server.bat
echo   - Client launcher: Start-NetSync-Client.bat
echo.
pause
exit /b 0
