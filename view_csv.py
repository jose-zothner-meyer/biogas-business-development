#!/usr/bin/env python3
"""
CSV viewer wrapper - view large CSV files
"""
import sys
import os

# Change to project root directory
project_root = os.path.dirname(os.path.abspath(__file__))
os.chdir(project_root)

# Add scripts directory to path and execute
sys.path.insert(0, os.path.join(project_root, 'scripts'))

# Import and run the view_csv script
if __name__ == "__main__":
    script_path = os.path.join(project_root, 'scripts', 'view_csv.py')
    with open(script_path, 'r', encoding='utf-8') as f:
        exec(f.read())
