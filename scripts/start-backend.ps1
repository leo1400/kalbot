param(
  [int]$Port = 8000,
  [switch]$Reload
)

$env:PYTHONPATH = "."
$reloadArg = if ($Reload) { "--reload" } else { "" }
python -m uvicorn backend.app.main:app --host 0.0.0.0 --port $Port $reloadArg
