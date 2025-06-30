#!/usr/bin/env python3
"""
CSV Viewer for Large Files
Interactive viewer for the large operator database that VS Code can't open directly
"""

import pandas as pd
import sys

class LargeCSVViewer:
    def __init__(self, filename):
        self.filename = filename
        self.chunk_size = 10000
        
    def show_info(self):
        """Show basic file information"""
        print(f"📁 FILE: {self.filename}")
        print("=" * 50)
        
        # Count total rows (this might take a moment for very large files)
        print("Counting rows... (this may take a moment)")
        total_rows = sum(1 for _ in open(self.filename)) - 1  # -1 for header
        print(f"Total rows: {total_rows:,}")
        
        # Show column info
        df_sample = pd.read_csv(self.filename, nrows=1)
        print(f"Columns: {list(df_sample.columns)}")
        print(f"File size: {self._get_file_size()}")
        
    def show_head(self, n=10):
        """Show first n rows"""
        print(f"\n📊 FIRST {n} ROWS:")
        print("-" * 50)
        df = pd.read_csv(self.filename, nrows=n)
        print(df.to_string(index=False))
        
    def show_tail(self, n=10):
        """Show last n rows (approximate for large files)"""
        print(f"\n📊 LAST {n} ROWS (approximate):")
        print("-" * 50)
        # For very large files, we'll read the last chunk
        df = pd.read_csv(self.filename, skiprows=lambda x: x % 1000 != 0, nrows=n)
        print(df.to_string(index=False))
        
    def search_by_keyword(self, keyword, column=None, n=10):
        """Search for rows containing a keyword"""
        print(f"\n🔍 SEARCHING FOR '{keyword}' (showing first {n} matches):")
        print("-" * 50)
        
        matches_found = 0
        chunk_num = 0
        
        for chunk in pd.read_csv(self.filename, chunksize=self.chunk_size):
            chunk_num += 1
            print(f"Searching chunk {chunk_num}...", end="\r")
            
            if column and column in chunk.columns:
                # Search specific column
                mask = chunk[column].astype(str).str.contains(keyword, case=False, na=False)
            else:
                # Search all columns
                mask = chunk.astype(str).apply(lambda x: x.str.contains(keyword, case=False, na=False)).any(axis=1)
            
            matches = chunk[mask]
            
            if len(matches) > 0:
                for _, row in matches.iterrows():
                    if matches_found >= n:
                        break
                    print(f"\nMatch {matches_found + 1}:")
                    for col in chunk.columns:
                        print(f"  {col}: {row[col]}")
                    matches_found += 1
                
                if matches_found >= n:
                    break
        
        print(f"\n\nFound {matches_found} matches (searched {chunk_num} chunks)")
        
    def show_column_stats(self, column):
        """Show statistics for a specific column"""
        print(f"\n📈 STATISTICS FOR COLUMN '{column}':")
        print("-" * 50)
        
        non_null_count = 0
        unique_values = set()
        total_rows = 0
        sample_values = []
        
        for chunk in pd.read_csv(self.filename, chunksize=self.chunk_size):
            if column in chunk.columns:
                col_data = chunk[column]
                total_rows += len(chunk)
                non_null_count += col_data.notna().sum()
                
                # Collect unique values (limit to avoid memory issues)
                if len(unique_values) < 1000:
                    unique_values.update(col_data.dropna().astype(str).unique())
                
                # Collect sample values
                sample_values.extend(col_data.dropna().head(5).tolist())
                
        print(f"Total rows: {total_rows:,}")
        print(f"Non-null values: {non_null_count:,} ({non_null_count/total_rows*100:.1f}%)")
        print(f"Unique values: {len(unique_values):,}+ (sampled)")
        print(f"Sample values: {sample_values[:10]}")
        
    def _get_file_size(self):
        """Get human-readable file size"""
        import os
        size_bytes = os.path.getsize(self.filename)
        for unit in ['B', 'KB', 'MB', 'GB']:
            if size_bytes < 1024:
                return f"{size_bytes:.1f} {unit}"
            size_bytes /= 1024
        return f"{size_bytes:.1f} TB"

def main():
    filename = "data/processed/german_biogas_all_operators_deduplicated.csv"
    viewer = LargeCSVViewer(filename)
    
    if len(sys.argv) > 1:
        command = sys.argv[1]
        
        if command == "info":
            viewer.show_info()
        elif command == "head":
            n = int(sys.argv[2]) if len(sys.argv) > 2 else 10
            viewer.show_head(n)
        elif command == "tail":
            n = int(sys.argv[2]) if len(sys.argv) > 2 else 10
            viewer.show_tail(n)
        elif command == "search":
            keyword = sys.argv[2] if len(sys.argv) > 2 else "biogas"
            column = sys.argv[3] if len(sys.argv) > 3 else None
            viewer.search_by_keyword(keyword, column)
        elif command == "stats":
            column = sys.argv[2] if len(sys.argv) > 2 else "market_actor_name"
            viewer.show_column_stats(column)
    else:
        # Interactive mode
        print("🔍 LARGE CSV VIEWER")
        print("=" * 50)
        viewer.show_info()
        viewer.show_head(5)
        
        print("\n💡 USAGE EXAMPLES:")
        print("python csv_viewer.py info                    # File information")
        print("python csv_viewer.py head 20                 # First 20 rows")
        print("python csv_viewer.py search biogas           # Search for 'biogas'")
        print("python csv_viewer.py search energy email     # Search 'energy' in email column")
        print("python csv_viewer.py stats market_actor_name # Column statistics")

if __name__ == "__main__":
    main()
