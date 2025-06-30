#!/usr/bin/env python3
"""
Analysis runner wrapper - executes enhanced data processing
"""
import sys
import os

# Change to project root directory
project_root = os.path.dirname(os.path.abspath(__file__))
os.chdir(project_root)

# Execute the analysis script directly
script_path = os.path.join(project_root, 'scripts', 'run_analysis.py')
exec(open(script_path).read())
