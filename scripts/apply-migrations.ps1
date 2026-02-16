Set-StrictMode -Version Latest
$ErrorActionPreference = "Stop"

$repoRoot = Resolve-Path (Join-Path $PSScriptRoot "..")
$migrations = @(
  "infra/migrations/001_initial_schema.sql",
  "infra/migrations/002_bot_intel.sql"
)

Push-Location $repoRoot
try {
  foreach ($migration in $migrations) {
    $path = Join-Path $repoRoot $migration
    if (-not (Test-Path $path)) {
      throw "Migration file not found: $migration"
    }

    Write-Host "Applying $migration ..."
    Get-Content -Raw $path | docker compose exec -T postgres psql -U postgres -d kalbot -v ON_ERROR_STOP=1
  }
}
finally {
  Pop-Location
}

Write-Host "Migrations applied."
