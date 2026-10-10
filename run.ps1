$ErrorActionPreference = 'Stop'
Set-Location -LiteralPath $PSScriptRoot
$taskTmp = Join-Path $PSScriptRoot 'tmp'
New-Item -ItemType Directory -Force -Path $taskTmp | Out-Null
$env:TEMP = $taskTmp
$env:TMP = $taskTmp
$taskPython = Join-Path $PSScriptRoot '.venv\Scripts\python.exe'
if (-not (Test-Path -LiteralPath $taskPython)) { throw 'Create .venv and install requirements.txt as described in README first.' }
& $taskPython download_data.py prepare
if ($LASTEXITCODE -ne 0) { throw 'Dataset preparation failed.' }
& $taskPython -m unittest discover -s tests -v
if ($LASTEXITCODE -ne 0) { throw 'Correctness tests failed.' }
& $taskPython main.py --repeats 3 --memory-gib 2 --threads 4 --blocksize 64MiB
if ($LASTEXITCODE -ne 0) { throw 'Full benchmark or numerical validation failed.' }
& $taskPython build_report.py
if ($LASTEXITCODE -ne 0) { throw 'Report generation failed.' }
