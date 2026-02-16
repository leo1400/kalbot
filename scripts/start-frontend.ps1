Set-StrictMode -Version Latest
$ErrorActionPreference = "Stop"

$repoRoot = Resolve-Path (Join-Path $PSScriptRoot "..")
$npm = Get-Command npm -ErrorAction SilentlyContinue

function Test-BackendUrl {
  param([string]$Url)
  try {
    $health = Invoke-WebRequest -UseBasicParsing -TimeoutSec 2 -Uri "$Url/health"
    return $health.StatusCode -eq 200
  }
  catch {
    return $false
  }
}

if ($null -ne $npm) {
  if (-not $env:VITE_API_BASE) {
    if (Test-BackendUrl -Url "http://localhost:8000") {
      $env:VITE_API_BASE = "http://localhost:8000"
    }
    elseif (Test-BackendUrl -Url "http://localhost:8001") {
      $env:VITE_API_BASE = "http://localhost:8001"
    }
    else {
      $env:VITE_API_BASE = "http://localhost:8000"
    }
  }
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
  if (-not $env:VITE_API_BASE) {
    $env:VITE_API_BASE = "http://localhost:8000"
  }
  docker compose --profile ui up -d frontend
  Write-Host "Frontend running in Docker at http://localhost:5173"
  Write-Host "Backend should be reachable at $env:VITE_API_BASE"
}
finally {
  Pop-Location
}
