#!/usr/bin/env python3
"""Test different data types for download to get market actors and locations."""

from open_mastr import Mastr
import pandas as pd

def main():
    print("🔍 Testing different data types for download...")
    
    db = Mastr()
    
    # Based on the earlier error message, these are allowed values:
    # ['wind', 'solar', 'biomass', 'hydro', 'gsgk', 'combustion', 'nuclear', 'storage'] 
    # or 
    # ['balancing_area', 'electricity_consumer', 'gas_consumer', 'gas_producer', 'gas_storage', 
    #  'gas_storage_extended', 'grid_connections', 'grids', 'locations_extended', 'market_actors', 
    #  'market_roles', 'permit', 'deleted_units', 'deleted_market_actors', 'retrofit_units', 
    #  'changed_dso_assignment', 'storage_units']
    
    print("\n💡 From the error message, these data types are available:")
    basic_types = ['wind', 'solar', 'biomass', 'hydro', 'gsgk', 'combustion', 'nuclear', 'storage']
    extended_types = ['balancing_area', 'electricity_consumer', 'gas_consumer', 'gas_producer', 'gas_storage', 
                     'gas_storage_extended', 'grid_connections', 'grids', 'locations_extended', 'market_actors', 
                     'market_roles', 'permit', 'deleted_units', 'deleted_market_actors', 'retrofit_units', 
                     'changed_dso_assignment', 'storage_units']
    
    print(f"Basic types: {basic_types}")
    print(f"Extended types: {extended_types}")
    
    print("\n🎯 We need to download:")
    print("   - 'market_actors' (to get contact info)")
    print("   - 'locations_extended' (to get coordinates)")
    print("   - Keep 'biomass' and 'gas' (for plant data)")
    
    # So our data types should be: ["biomass", "gas", "market_actors", "locations_extended"]
    # But wait - let me check if we can download these as separate data types
    
    recommended_data_types = ["biomass", "gas"]  # Current
    needed_additional = ["market_actors", "locations_extended"]
    
    print(f"\n📋 Current data types: {recommended_data_types}")
    print(f"📋 Additional needed: {needed_additional}")
    print(f"📋 Combined should be: {recommended_data_types + needed_additional}")

if __name__ == "__main__":
    main()
