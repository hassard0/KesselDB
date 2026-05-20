# kesseldb-tla/verify.ps1 — Windows TLC runner for the S1 replication-safety spec.
# Thin wrapper: detects tla2tools.jar via $env:TLA2TOOLS_JAR or $env:TLC_JAR,
# then invokes TLC against Replication.tla / Replication.cfg in this directory.
# Requires: java 11+ on PATH; $env:TLA2TOOLS_JAR (or $env:TLC_JAR) set.
# Windows PowerShell 5.1 — no ternary, no &&, no null-coalescing ??.
$ErrorActionPreference = "Stop"

# Accept either $env:TLA2TOOLS_JAR (canonical) or $env:TLC_JAR (plan alias).
$TlaJar = $null
if ($env:TLA2TOOLS_JAR) {
    $TlaJar = $env:TLA2TOOLS_JAR
} elseif ($env:TLC_JAR) {
    $TlaJar = $env:TLC_JAR
}

if (-not $TlaJar) {
    Write-Host "verify.ps1: ERROR -- jar not found." -ForegroundColor Red
    Write-Host ""
    Write-Host "Set TLA2TOOLS_JAR (or TLC_JAR) to the path of tla2tools.jar, e.g.:"
    Write-Host "  `$env:TLA2TOOLS_JAR = 'C:\path\to\tla2tools.jar'"
    Write-Host ""
    Write-Host "Download from: https://github.com/tlaplus/tlaplus/releases/latest"
    exit 2
}

if (-not (Test-Path $TlaJar)) {
    Write-Host "verify.ps1: ERROR -- jar not found at: $TlaJar" -ForegroundColor Red
    exit 2
}

$JavaExe = $null
try {
    $JavaExe = (Get-Command java -ErrorAction Stop).Source
} catch {
    Write-Host "verify.ps1: ERROR -- java not found on PATH." -ForegroundColor Red
    Write-Host "Install Java 11+ and ensure it is on PATH."
    exit 2
}

# Change to the directory containing this script.
Set-Location $PSScriptRoot

$Stamp = (Get-Date).ToUniversalTime().ToString("yyyy-MM-ddTHH-mm-ssZ")
$ResultsDir = Join-Path $PSScriptRoot "results"
New-Item -ItemType Directory -Force -Path $ResultsDir | Out-Null
$Out = Join-Path $ResultsDir "$Stamp.txt"

Write-Host "Running TLC on Replication.tla / Replication.cfg ..."
Write-Host "Output teed to: $Out"

& java -XX:+UseParallelGC -cp $TlaJar tlc2.TLC `
    -workers auto `
    -config Replication.cfg `
    Replication 2>&1 | Tee-Object -FilePath $Out

$Rc = $LASTEXITCODE
Write-Host ""
Write-Host "TLC exit code: $Rc"
exit $Rc
