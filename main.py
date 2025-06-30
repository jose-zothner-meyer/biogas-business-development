#!/usr/bin/env python3
"""
Main pipeline wrapper - executes the main biogas database pipeline
"""
import sys
import os

# Change to project root directory
project_root = os.path.dirname(os.path.abspath(__file__))
os.chdir(project_root)

# Execute the main script directly
script_path = os.path.join(project_root, 'scripts', 'main.py')
exec(open(script_path).read())
