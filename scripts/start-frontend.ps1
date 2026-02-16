Set-StrictMode -Version Latest
$ErrorActionPreference = "Stop"

$repoRoot = Resolve-Path (Join-Path $PSScriptRoot "..")
$npm = Get-Command npm -ErrorAction SilentlyContinue

function Test-BackendUrl {
  param([string]$Url)
  $probe = @"
import sys, urllib.request
base = sys.argv[1].rstrip('/')
url = base + '/health'
try:
    with urllib.request.urlopen(url, timeout=2) as r:
        ok = (r.status == 200)
except Exception:
    ok = False
sys.exit(0 if ok else 1)
"@
  python -c $probe $Url 2>$null | Out-Null
  return $LASTEXITCODE -eq 0
}

function Resolve-ApiBase {
  $preferred = $env:VITE_API_BASE
  if ($preferred -and (Test-BackendUrl -Url $preferred)) {
    return $preferred
  }
  if (Test-BackendUrl -Url "http://localhost:8000") {
    return "http://localhost:8000"
  }
  if (Test-BackendUrl -Url "http://localhost:8001") {
    return "http://localhost:8001"
  }
  if ($preferred) {
    return $preferred
  }
  return "http://localhost:8000"
}

if ($null -ne $npm) {
  $env:VITE_API_BASE = Resolve-ApiBase
  Write-Host "Frontend API base: $env:VITE_API_BASE"
  Push-Location (Join-Path $repoRoot "frontend")
  try {
    npm install
    npm run dev -- --host 0.0.0.0 --port 5173
  }
  finally {
    Pop-Location
  }
  return
}

Write-Host "npm not found on PATH. Starting frontend in Docker profile 'ui'..."
Push-Location $repoRoot
try {
  $env:VITE_API_BASE = Resolve-ApiBase
  Write-Host "Frontend API base: $env:VITE_API_BASE"
  docker compose --profile ui up -d --force-recreate frontend
  Write-Host "Frontend running in Docker at http://localhost:5173"
  Write-Host "Backend should be reachable at $env:VITE_API_BASE"
}
finally {
  Pop-Location
}
