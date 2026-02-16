Set-StrictMode -Version Latest
$ErrorActionPreference = "Stop"

$repoRoot = Resolve-Path (Join-Path $PSScriptRoot "..")
$npm = Get-Command npm -ErrorAction SilentlyContinue

if ($null -ne $npm) {
  $env:VITE_API_BASE = "http://localhost:8000"
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
  docker compose --profile ui up -d frontend
  Write-Host "Frontend running in Docker at http://localhost:5173"
  Write-Host "Backend should be reachable at http://localhost:8000"
}
finally {
  Pop-Location
}
