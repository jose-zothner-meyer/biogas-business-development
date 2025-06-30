#!/usr/bin/env python3
"""
Run Enhanced Data Processing from organized project structure
"""
import sys
import os

# Add src directory to Python path (go up one level to project root)
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'src'))

from src.analysis.enhanced_data_processing import main

if __name__ == "__main__":
    # Ensure we're working with the project root directory
    project_root = os.path.join(os.path.dirname(__file__), '..')
    os.chdir(project_root)
    main()
