[CmdletBinding()]
param([switch]$Dev)

$ErrorActionPreference = 'Stop'
if ($env:OS -ne 'Windows_NT') {
    throw 'This package supports Windows 64-bit. Use Windows PowerShell.'
}
$projectRoot = [System.IO.Path]::GetFullPath((Join-Path $PSScriptRoot '..'))
$launcher = Get-Command py.exe -ErrorAction SilentlyContinue
if (-not $launcher) {
    throw 'Install 64-bit Python 3.10 with the Windows Python launcher first.'
}
Write-Host 'Checking Python 3.10 (64-bit)...'
& $launcher.Source -3.10 -c "import struct,sys; raise SystemExit(0 if sys.version_info[:2] == (3,10) and struct.calcsize('P') == 8 else '64-bit Python 3.10 required')"
if ($LASTEXITCODE -ne 0) { throw 'Python prerequisite check failed.' }

$venvRoot = Join-Path $projectRoot '.venv'
$python = Join-Path $venvRoot 'Scripts/python.exe'
if (-not (Test-Path -LiteralPath $venvRoot)) {
    Write-Host 'Creating local .venv...'
    & $launcher.Source -3.10 -m venv $venvRoot
    if ($LASTEXITCODE -ne 0) { throw 'Virtual environment creation failed.' }
}
if (-not (Test-Path -LiteralPath $python -PathType Leaf)) {
    throw 'Existing .venv does not contain a Windows Python executable.'
}
& $python -c "import struct,sys; raise SystemExit(0 if sys.version_info[:2] == (3,10) and struct.calcsize('P') == 8 else 'Existing .venv must use 64-bit Python 3.10')"
if ($LASTEXITCODE -ne 0) { throw 'Existing virtual environment is incompatible.' }

Write-Host 'Installing the pinned CPU PyTorch dependency...'
& $python -m pip install 'torch==2.9.1' --index-url 'https://download.pytorch.org/whl/cpu'
if ($LASTEXITCODE -ne 0) { throw 'CPU PyTorch installation failed.' }

$requirements = if ($Dev) { 'requirements-dev.txt' } else { 'requirements.txt' }
Write-Host "Installing $requirements..."
& $python -m pip install -r (Join-Path $projectRoot $requirements)
if ($LASTEXITCODE -ne 0) { throw 'Dependency installation failed; do not relax pins silently.' }

Write-Host 'Dependency setup complete.'
Write-Host 'Next: follow README.md to provision the model cache and private provider JSON.'
Write-Host 'Then use the documented check-config and loopback launch commands.'
Write-Host 'No model, credentials, data root, service, or public tunnel was created by this script.'

