#!/usr/bin/env python3
"""
CSV Viewer wrapper for organized project structure
"""
import sys
import os

# Add tools directory to Python path (go up one level to project root)
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'tools'))

# Change to project root directory to find data files
project_root = os.path.join(os.path.dirname(__file__), '..')
os.chdir(project_root)

# Import and run CSV viewer
from csv_viewer import main

if __name__ == "__main__":
    main()
