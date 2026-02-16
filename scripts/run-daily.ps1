param(
  [string]$Date = (Get-Date -Format "yyyy-MM-dd")
)

$env:PYTHONPATH = "."
python -m workers.kalbot_workers.cli --date $Date
