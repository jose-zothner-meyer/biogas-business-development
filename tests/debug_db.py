#!/usr/bin/env python3
"""Debug script to check the MaStR database and export data properly."""

from mastr_fetcher import MaStrFetcher
import pandas as pd

def main():
    print("🔍 Debugging MaStR database...")
    
    # Initialize fetcher
    fetcher = MaStrFetcher()
    
    # Print database summary
    fetcher.print_database_summary()
    
    # Get available tables
    tables = fetcher.get_available_tables()
    print(f"\n📋 Available tables ({len(tables)}):")
    for i, table in enumerate(tables, 1):
        print(f"   {i:2d}. {table}")
    
    # Try to manually export the tables we need
    print("\n📤 Attempting manual export...")
    
    required_tables = {
        "market_actors": ["market_actors", "market", "Marktakteure"],
        "biomass": ["biomass_extended", "biomass_eeg", "biomass", "Biomasse"],
        "gas_producer": ["gas_producer", "gas", "Gas"],
        "locations": ["locations_extended", "locations_basic", "location", "Lokationen", "locations"]
    }
    
    result_paths = {}
    
    for dataset_key, possible_names in required_tables.items():
        found = False
        for table_name in possible_names:
            if table_name in tables:
                print(f"✅ Found table '{table_name}' for dataset '{dataset_key}'")
                try:
                    output_path = fetcher.tmp / f"{dataset_key}.csv"
                    fetcher.export_table(table_name, output_path)
                    result_paths[dataset_key] = output_path
                    found = True
                    break
                except Exception as e:
                    print(f"❌ Failed to export {table_name}: {e}")
        
        if not found:
            print(f"⚠️  No table found for dataset '{dataset_key}'")
            # Create empty CSV with proper structure
            output_path = fetcher.tmp / f"{dataset_key}.csv"
            pd.DataFrame().to_csv(output_path, index=False)
            result_paths[dataset_key] = output_path
    
    print(f"\n✅ Export complete! Files:")
    for key, path in result_paths.items():
        size = path.stat().st_size if path.exists() else 0
        print(f"   {key}: {path} ({size:,} bytes)")
    
    return result_paths

if __name__ == "__main__":
    main()
