#!/bin/bash

# Terminate background processes on exit (Ctrl+C)
trap "kill 0" EXIT

echo "🚀 Starting Crypto Tax Application..."

# 1. Start Backend (FastAPI)
echo "➜ Starting Backend on http://localhost:8000"
(cd backend && ./venv/bin/uvicorn main:app --reload --port 8000 --host 0.0.0.0) &

# 2. Start Frontend (Next.js)
echo "➜ Starting Frontend on http://localhost:3000"
(cd frontend && npm run dev) &

# Wait for both processes
wait
