param(
  [int]$Port = 8000
)

$env:PYTHONPATH = "."
python -m uvicorn backend.app.main:app --host 0.0.0.0 --port $Port --reload
