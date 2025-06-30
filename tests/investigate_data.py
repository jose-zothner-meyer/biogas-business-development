#!/usr/bin/env python3
"""Check what data types are available for download and what we need."""

from open_mastr import Mastr
import pandas as pd

def main():
    print("🔍 Investigating available MaStR data types...")
    
    db = Mastr()
    
    # Check what data types can be downloaded
    print("\n📋 Let's see what data types contain market actors and locations...")
    
    # Try to inspect the download options
    try:
        # Check if there are other data types we should download
        print("\n🧪 Testing different download configurations...")
        
        # Let's look for market actor data by checking if we can query existing operators
        # from the biomass data to see what operator IDs we have
        print("\n📊 Checking operator IDs from biomass data:")
        query = "SELECT DISTINCT AnlagenbetreiberMastrNummer FROM biomass_extended WHERE AnlagenbetreiberMastrNummer IS NOT NULL LIMIT 10"
        operators = pd.read_sql_query(query, con=db.engine)
        print(f"Sample operator IDs from biomass: {list(operators['AnlagenbetreiberMastrNummer'])}")
        
        print("\n📊 Checking location IDs from biomass data:")
        query = "SELECT DISTINCT LokationMastrNummer FROM biomass_extended WHERE LokationMastrNummer IS NOT NULL LIMIT 10"
        locations = pd.read_sql_query(query, con=db.engine)
        print(f"Sample location IDs from biomass: {list(locations['LokationMastrNummer'])}")
        
        # Check what the MaStR documentation says about data types
        print("\n💡 Need to download additional data types:")
        print("   - For market actors: need 'market' or 'actors' data type")
        print("   - For locations: need 'locations' data type")
        print("   - Current download only includes: ['biomass', 'gas']")
        
    except Exception as e:
        print(f"Error: {e}")

if __name__ == "__main__":
    main()
