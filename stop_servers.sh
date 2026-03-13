#!/bin/bash

echo "Stopping servers..."

# Function to kill process by pattern
kill_pattern() {
    local pattern=$1
    local name=$2
    
    # Find PIDs matching the pattern
    # We filter out our own PID ($$) and our parent PID ($PPID) to avoid self-termination
    local pids=$(pgrep -f "$pattern" 2>/dev/null | grep -v "^$$$" | grep -v "^$PPID$")
    
    if [ ! -z "$pids" ]; then
        echo "✔ Stopping $name (PIDs: $pids)..."
        for pid in $pids; do
            kill $pid 2>/dev/null
        done
        
        sleep 1
        
        # Check if they are still running
        local remaining_pids=$(pgrep -f "$pattern" 2>/dev/null | grep -v "^$$$" | grep -v "^$PPID$")
        if [ ! -z "$remaining_pids" ]; then
            echo "⚠️  $name still running, using SIGKILL on $remaining_pids..."
            for pid in $remaining_pids; do
                kill -9 $pid 2>/dev/null
            done
        fi
    else
        echo "ℹ $name was not running."
    fi
}

# Kill backend (uvicorn)
kill_pattern "uvicorn" "Backend (Uvicorn)"

# Kill frontend (Next.js/Node)
kill_pattern "node.*next" "Frontend (Next.js)"

# General cleanup for any orphaned python main.py processes
kill_pattern "python.*main.py" "Orphaned Python processes"

# Port-based cleanup as a final fallback
for port in 8000 3000; do
    pids=$(lsof -t -i:$port 2>/dev/null)
    if [ ! -z "$pids" ]; then
        # Filter out current and parent PID here too just in case
        filtered_pids=$(echo "$pids" | grep -v "^$$$" | grep -v "^$PPID$")
        if [ ! -z "$filtered_pids" ]; then
            echo "⚠️  Found processes ($filtered_pids) on port $port, killing..."
            for pid in $filtered_pids; do
                kill -9 $pid > /dev/null 2>&1
            done
        fi
    fi
done

echo "Done."
