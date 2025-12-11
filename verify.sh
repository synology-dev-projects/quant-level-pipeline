#!/bin/bash

# 1. Exit immediately if any command fails
# This is crucial: It ensures if pytest fails, the script returns an error code,
# which tells GitHub Actions to STOP the pipeline (Turn Red).
set -e

echo "========================================"
echo "    VERIFICATION SCRIPT STARTED"
echo "========================================"

# 2. Ensure we are running inside the correct folder
# This trick ensures the script always runs relative to itself,
# no matter where you call it from.
SCRIPT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
cd "$SCRIPT_DIR"

echo "📂 Working Directory: $(pwd)"

# This makes sure we use the isolated environment if it exists.
if [ -d ".venv" ]; then
    echo "⚙️  Found .venv... Activating."
    # We use . (dot) to source, covering both Linux/Mac and some Git Bash setups
    if [ -f ".venv/bin/activate" ]; then
        source .venv/bin/activate
    elif [ -f ".venv/Scripts/activate" ]; then
        source .venv/Scripts/activate
    fi
else
    echo "⚠️  No .venv found. Using system Python:"
fi

# 3. Safety Check: Do tests exist?
if [ ! -d "tests" ]; then
    echo "❌ Error: 'tests' directory not found in Staging!"
    echo "   Did you remember to copy the 'tests' folder in deploy.yml?"
    exit 1
fi

# Debug: Print which python we are actually using
echo "🐍 Using Python: $(which python3)"

if [ ! -d "tests" ]; then
    echo "❌ Error: 'tests' directory not found in Staging!"
    exit 1
fi

# 4. Run the Tests
# Using 'python3 -m pytest' is often safer than just 'pytest'
# to ensure it uses the correct Python interpreter.
echo "🚀 Running Pytest..."
python -m pytest -v tests/

echo "✅ SUCCESS: All tests passed."
echo "========================================"