$env:VITE_API_BASE = "http://localhost:8000"
Set-Location frontend
npm install
npm run dev -- --host 0.0.0.0 --port 5173
