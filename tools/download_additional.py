#!/usr/bin/env python3
"""Try to download additional data types to get market actors and locations."""

from open_mastr import Mastr
import pandas as pd

def main():
    print("🔍 Attempting to download additional MaStR data types...")
    
    db = Mastr()
    
    # Try to download specific data types that should include market actors and locations
    print("\n📤 Attempting to download market_actors and locations_extended...")
    
    try:
        # Try downloading just the missing data types - use correct bulk data type names
        additional_data_types = ["market", "location"]
        
        print(f"Downloading: {additional_data_types}")
        db.download(method="bulk", data=additional_data_types)
        
        print("✅ Additional download completed!")
        
        # Check if we now have data in these tables
        query_market = "SELECT COUNT(*) as count FROM market_actors"
        query_locations = "SELECT COUNT(*) as count FROM locations_extended"
        
        try:
            market_count = pd.read_sql_query(query_market, con=db.engine)['count'].iloc[0]
            locations_count = pd.read_sql_query(query_locations, con=db.engine)['count'].iloc[0]
            
            print(f"📊 Market actors: {market_count} rows")
            print(f"📊 Locations: {locations_count} rows")
            
        except Exception as e:
            print(f"Error checking table counts: {e}")
        
    except Exception as e:
        print(f"❌ Error downloading additional data: {e}")
        
        # Try alternative approach - download all extended types
        print("\n🔄 Trying to download all extended data types...")
        try:
            extended_types = ['balancing_area', 'electricity_consumer', 'gas_consumer', 'gas_producer', 
                            'gas_storage', 'gas_storage_extended', 'grid_connections', 'grids', 
                            'locations_extended', 'market_actors', 'market_roles', 'permit', 
                            'deleted_units', 'deleted_market_actors', 'retrofit_units', 
                            'changed_dso_assignment', 'storage_units']
            
            print(f"Downloading extended types: {extended_types}")
            db.download(method="bulk", data=extended_types)
            print("✅ Extended download completed!")
            
        except Exception as e2:
            print(f"❌ Error downloading extended data: {e2}")

if __name__ == "__main__":
    main()
