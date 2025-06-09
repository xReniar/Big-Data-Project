#!/usr/bin

if [ "$1" != "local[*]" ] && [ "$1" != "yarn" ]; then
    echo "Error: Invalid master argument. Use 'local[*]' or 'yarn'."
    exit 1
fi

python3 benchmark.py job-1 $1 --fraction "0.01 0.2 0.5 0.7"
python3 benchmark.py job-2 $1 --fraction "0.01 0.2 0.5 0.7"