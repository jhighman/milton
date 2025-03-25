#!/bin/bash

# Ensure virtual environment is active
if [ -z "$VIRTUAL_ENV" ]; then
    if [ -d "venv" ]; then
        source venv/bin/activate
    else
        echo "Virtual environment not found. Creating one..."
        python -m venv venv
        source venv/bin/activate
        pip install -r requirements.txt
    fi
fi

# Run tests with coverage
echo "Running tests with coverage..."
pytest tests/ -v --cov=storage_providers --cov=storage_manager --cov-report=term-missing

# Run example script if tests pass
if [ $? -eq 0 ]; then
    echo -e "\nRunning example script..."
    python examples/storage_example.py
fi 