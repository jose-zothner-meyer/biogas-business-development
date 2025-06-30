#!/usr/bin/env python3
"""
Business Logic Analysis for Biogas/Biomethane Trading
Analyzing the data structure for green gas certificate trading
"""

import pandas as pd
import numpy as np

def analyze_business_data():
    print("🔍 BIOGAS/BIOMETHANE BUSINESS LOGIC ANALYSIS")
    print("=" * 60)
    
    # 1. Analyze the plant CSV data (production side)
    print("\n📍 PRODUCTION SIDE ANALYSIS (CSV)")
    print("-" * 40)
    
    plants_df = pd.read_csv("german_biogas_plants_with_contacts.csv")
    
    print(f"Total plants in database: {len(plants_df):,}")
    print(f"Plants with contact info: {plants_df[['email', 'phone']].notna().any(axis=1).sum():,}")
    
    # Check plant types
    plant_types = plants_df['plant_type'].value_counts()
    print(f"\nPlant type distribution:")
    for ptype, count in plant_types.items():
        print(f"  • {ptype}: {count:,} plants")
    
    # Check capacity distribution
    print(f"\nCapacity analysis:")
    print(f"  • Electrical capacity (kW): {plants_df['capacity_el_kW'].sum():,} kW total")
    print(f"  • Gas capacity (m³/h): {plants_df['capacity_gas_m3/h'].sum():,} m³/h total")
    
    # Check for actual gas injection plants
    gas_injection_plants = plants_df[plants_df['capacity_gas_m3/h'] > 0]
    print(f"  • Plants with gas injection capacity: {len(gas_injection_plants)}")
    
    if len(gas_injection_plants) > 0:
        print(f"    - Total gas injection capacity: {gas_injection_plants['capacity_gas_m3/h'].sum():,} m³/h")
        print(f"    - Average gas capacity: {gas_injection_plants['capacity_gas_m3/h'].mean():.1f} m³/h")
    
    # Geographic distribution
    print(f"\nGeographic distribution (top 10 postal codes):")
    top_postcodes = plants_df['postal_code'].value_counts().head(10)
    for postcode, count in top_postcodes.items():
        print(f"  • {postcode}: {count} plants")
    
    # Commissioning timeline
    print(f"\nCommissioning timeline:")
    recent_plants = plants_df[plants_df['commissioning_year'] >= 2020]
    print(f"  • Plants commissioned since 2020: {len(recent_plants):,}")
    print(f"  • Peak commissioning year: {plants_df['commissioning_year'].mode().iloc[0]}")
    
    # 2. Analyze the operator Excel data (trading/certificate side)
    print("\n\n🏢 TRADING/CERTIFICATE SIDE ANALYSIS (XLSX)")
    print("-" * 50)
    
    try:
        # Read first sheet to understand structure
        excel_file = pd.ExcelFile("german_biogas_operator_contacts.xlsx")
        print(f"Excel file contains {len(excel_file.sheet_names)} sheets:")
        for sheet in excel_file.sheet_names:
            print(f"  • {sheet}")
        
        # Read the first sheet
        operators_df = pd.read_excel("german_biogas_operator_contacts.xlsx", sheet_name=excel_file.sheet_names[0])
        
        print(f"\nOperator database:")
        print(f"  • Total operators: {len(operators_df):,}")
        print(f"  • Operators with email: {operators_df['email'].notna().sum():,}")
        print(f"  • Operators with phone: {operators_df['phone'].notna().sum():,}")
        print(f"  • Operators with website: {operators_df['website'].notna().sum():,}")
        
        # Check operator types/roles
        if 'market_actor_name' in operators_df.columns:
            print(f"\nOperator name analysis:")
            print(f"  • Named operators: {operators_df['market_actor_name'].notna().sum():,}")
            
            # Look for specific business types
            names = operators_df['market_actor_name'].fillna('').str.lower()
            biogas_operators = names.str.contains('biogas|bio-gas', na=False).sum()
            gas_operators = names.str.contains('gas', na=False).sum()
            energy_operators = names.str.contains('energie|energy', na=False).sum()
            
            print(f"  • Biogas-specific operators: {biogas_operators:,}")
            print(f"  • Gas-related operators: {gas_operators:,}")
            print(f"  • Energy companies: {energy_operators:,}")
        
        # Sample some operator names
        print(f"\nSample operator names (biogas-related):")
        if 'market_actor_name' in operators_df.columns:
            biogas_names = operators_df[operators_df['market_actor_name'].str.contains('biogas|Bio', case=False, na=False)]['market_actor_name'].head(10)
            for name in biogas_names:
                print(f"  • {name}")
    
    except Exception as e:
        print(f"Error reading Excel file: {e}")
    
    # 3. Business Logic Analysis
    print("\n\n💰 BUSINESS LOGIC FOR GREEN GAS CERTIFICATES")
    print("-" * 55)
    
    print("🏭 PRODUCTION SIDE (CSV - Plant Data):")
    print("  • Physical biogas/biomethane production facilities")
    print("  • Contact details for sustainability proof/verification")
    print("  • Technical capacity for production volume calculation")
    print("  • Geographic location for logistics planning")
    print("  • Commissioning date for technology assessment")
    
    print("\n🏢 TRADING SIDE (XLSX - Operator Data):")
    print("  • Market actors/operators who inject gas into grid")
    print("  • Contact details for green certificate negotiations")
    print("  • Company information for contract establishment")
    print("  • Registration details for compliance verification")
    
    print("\n💎 VALUE CHAIN OPPORTUNITIES:")
    print("  1. GREEN CERTIFICATE TRADING:")
    print("     - Contact grid injection operators (XLSX)")
    print("     - Negotiate green gas certificate purchases")
    print("     - Main revenue stream from certificate trading")
    
    print("\n  2. SUSTAINABILITY VERIFICATION:")
    print("     - Contact production plants (CSV)")
    print("     - Verify sustainable feedstock sources")
    print("     - Ensure compliance with renewable energy standards")
    
    print("\n  3. SUPPLY CHAIN OPTIMIZATION:")
    print("     - Match production capacity with injection points")
    print("     - Optimize geographic distribution")
    print("     - Coordinate production timing with grid demand")
    
    # 4. Identify key opportunities
    plants_with_contacts = plants_df[plants_df[['email', 'phone']].notna().any(axis=1)]
    high_capacity_plants = plants_df[plants_df['capacity_el_kW'] > 500]  # Large plants
    recent_plants_contacted = plants_with_contacts[plants_with_contacts['commissioning_year'] >= 2015]
    
    print(f"\n📊 KEY BUSINESS OPPORTUNITIES:")
    print(f"  • Contactable production plants: {len(plants_with_contacts):,}")
    print(f"  • High-capacity plants (>500kW): {len(high_capacity_plants):,}")
    print(f"  • Recent plants with contacts: {len(recent_plants_contacted):,}")
    print(f"  • Total market actors for trading: {len(operators_df) if 'operators_df' in locals() else 'N/A'}")

if __name__ == "__main__":
    analyze_business_data()
