# Thin wrapper for export-schema.py (Windows PowerShell)
$ErrorActionPreference = "Stop"
$ScriptDir = Split-Path -Parent $MyInvocation.MyCommand.Path
$Python = if (Get-Command py -ErrorAction SilentlyContinue) { "py" }
          elseif (Get-Command python -ErrorAction SilentlyContinue) { "python" }
          else { "python3" }
if ($Python -eq "py") {
    & py -3 (Join-Path $ScriptDir "export-schema.py") @args
} else {
    & $Python (Join-Path $ScriptDir "export-schema.py") @args
}
exit $LASTEXITCODE
