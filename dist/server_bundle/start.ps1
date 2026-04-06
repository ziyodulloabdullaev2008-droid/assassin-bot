Set-StrictMode -Version Latest
$ErrorActionPreference = "Stop"

Set-Location $PSScriptRoot

if (Get-Command py -ErrorAction SilentlyContinue) {
    py -3 .\start.py @args
} else {
    python .\start.py @args
}
