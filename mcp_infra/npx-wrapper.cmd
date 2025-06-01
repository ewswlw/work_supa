@echo off
REM Custom npx wrapper to bypass corruption
set NPM_DIR=%~dp0package
set NODE_PATH=%NPM_DIR%;%NODE_PATH%
C:\nvm4w\nodejs\node.exe "%NPM_DIR%\bin\npx-cli.js" %* 