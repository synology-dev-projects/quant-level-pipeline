#!/bin/bash
set -e

# 1. System Path Recovery (Safe for Linux & Windows)
export PATH="/usr/bin:/bin:/usr/local/bin:$PATH"

echo "========================================"
echo "    VERIFICATION SCRIPT STARTED"
echo "========================================"

# 2. Set Directory Context
SCRIPT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
cd "$SCRIPT_DIR"

# 3. Set PYTHONPATH (Directly to 'src')
# This ensures modules in 'src/' are importable from 'tests/'
export PYTHONPATH="$(pwd)/src:$PYTHONPATH"

echo "📂 Working Directory: $(pwd)"
echo "🐍 Python Path: $PYTHONPATH"

# 4. Activate Virtual Environment (Cross-Platform)
if [ -d ".venv" ]; then
    echo "⚙️  Found .venv... Activating."
    if [ -f ".venv/bin/activate" ]; then
        source .venv/bin/activate
    elif [ -f ".venv/Scripts/activate" ]; then
        source .venv/Scripts/activate
    fi
else
    echo "⚠️  No .venv found."
fi

# 5. Run Tests
echo "🐍 Using Python: $(which python)"
echo "🚀 Running Pytest..."
python -m pytest -v tests/