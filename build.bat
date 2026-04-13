@echo off
echo ================================================
echo   Build EXE
echo ================================================
echo.

REM 1. Install packages
echo [1/3] Installing packages...
pip install pyinstaller python-tds scramp asn1crypto PySide6 openpyxl requests --quiet
if errorlevel 1 goto ERROR

REM 2. Clean previous build
echo [2/3] Cleaning previous build...
if exist dist  rmdir /s /q dist
if exist build rmdir /s /q build

REM 3. PyInstaller build
echo [3/3] Building EXE...
pyinstaller build.spec
if errorlevel 1 goto ERROR

echo.
echo ================================================
echo   Build complete!
echo   Output: dist\release\
echo ================================================
echo.
pause
goto END

:ERROR
echo.
echo [ERROR] Build failed.
pause

:END