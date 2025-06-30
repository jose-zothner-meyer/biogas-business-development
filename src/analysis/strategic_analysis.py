#!/usr/bin/env python3
"""
Strategic Business Analysis for Biogas/Biomethane Certificate Trading
Focus on green gas certificate value chain opportunities
"""

import pandas as pd
import numpy as np

def detailed_business_analysis():
    print("💎 STRATEGIC BIOGAS/BIOMETHANE CERTIFICATE TRADING ANALYSIS")
    print("=" * 70)
    
    # Load data
    plants_df = pd.read_csv("german_biogas_plants_with_contacts.csv")
    operators_df = pd.read_excel("german_biogas_operator_contacts.xlsx", sheet_name="contacts_1")
    
    print("\n📊 RAW DATASET OVERVIEW:")
    print(f"Total plant records: {len(plants_df):,}")
    print(f"Total operator records (sheet 1): {len(operators_df):,}")
    
    # CRITICAL: Handle duplicates for accurate business analysis
    print("\n🔄 DEDUPLICATION ANALYSIS:")
    
    # Deduplicate plants by plant_id (unique plant identifier)
    original_plant_count = len(plants_df)
    plants_df = plants_df.drop_duplicates(subset=['plant_id'])
    plant_duplicates = original_plant_count - len(plants_df)
    print(f"• Plant records after deduplication: {len(plants_df):,} (removed {plant_duplicates:,} duplicates)")
    
    # Deduplicate operators by market_actor_id (unique operator identifier)
    original_operator_count = len(operators_df)
    operators_df = operators_df.drop_duplicates(subset=['market_actor_id'])
    operator_duplicates = original_operator_count - len(operators_df)
    print(f"• Operator records after deduplication: {len(operators_df):,} (removed {operator_duplicates:,} duplicates)")
    
    # Additional check: unique plant-operator combinations
    unique_plant_operators = plants_df.drop_duplicates(subset=['operator_id'])
    print(f"• Unique plant operators: {len(unique_plant_operators):,}")
    
    print("\n🎯 BUSINESS MODEL OVERVIEW")
    print("-" * 40)
    print("VALUE PROPOSITION:")
    print("• Trade green gas certificates from biogas/biomethane production")
    print("• Bridge physical production sites with grid injection operators")
    print("• Ensure sustainability compliance through producer verification")
    print("• Capture value from renewable energy certificate market")
    
    # 1. PRODUCTION SIDE ANALYSIS
    print("\n\n🏭 PRODUCTION SIDE - PHYSICAL BIOGAS FACILITIES")
    print("-" * 55)
    
    # Filter for actual gas production plants
    gas_producers = plants_df[plants_df['capacity_gas_m3/h'] > 0]
    electrical_only = plants_df[plants_df['capacity_gas_m3/h'] == 0]
    
    print(f"FACILITY BREAKDOWN:")
    print(f"• Total facilities: {len(plants_df):,}")
    print(f"• Gas injection facilities: {len(gas_producers):,} ({len(gas_producers)/len(plants_df)*100:.1f}%)")
    print(f"• Electrical-only facilities: {len(electrical_only):,} ({len(electrical_only)/len(plants_df)*100:.1f}%)")
    
    print(f"\nGAS INJECTION CAPACITY:")
    total_gas_capacity = 0  # Initialize variable
    gas_with_contact = pd.DataFrame()  # Initialize variable
    
    if len(gas_producers) > 0:
        total_gas_capacity = gas_producers['capacity_gas_m3/h'].sum()
        print(f"• Total gas injection: {total_gas_capacity:,} m³/h")
        print(f"• Average per plant: {total_gas_capacity/len(gas_producers):,.0f} m³/h")
        print(f"• Largest plant: {gas_producers['capacity_gas_m3/h'].max():,} m³/h")
        
        # Contact coverage for gas producers
        gas_with_contact = gas_producers[gas_producers[['email', 'phone']].notna().any(axis=1)]
        print(f"• Gas producers with contacts: {len(gas_with_contact):,} ({len(gas_with_contact)/len(gas_producers)*100:.1f}%)")
    else:
        print("• No gas injection plants found in dataset")
        print("• All plants appear to be electrical generation (BHKW) only")
    
    print(f"\nELECTRICAL CAPACITY (BHKW Plants):")
    total_el_capacity = plants_df['capacity_el_kW'].sum()
    print(f"• Total electrical: {total_el_capacity:,} kW ({total_el_capacity/1000:.0f} MW)")
    print(f"• Average per plant: {total_el_capacity/len(plants_df):.0f} kW")
    
    # Contact analysis for producers
    producers_with_contact = plants_df[plants_df[['email', 'phone']].notna().any(axis=1)]
    print(f"\nPRODUCER CONTACT COVERAGE:")
    print(f"• Contactable producers: {len(producers_with_contact):,} ({len(producers_with_contact)/len(plants_df)*100:.1f}%)")
    print(f"• With email: {plants_df['email'].notna().sum():,}")
    print(f"• With phone: {plants_df['phone'].notna().sum():,}")
    
    # 2. OPERATOR/TRADING SIDE ANALYSIS
    print("\n\n🏢 TRADING SIDE - MARKET ACTORS & GRID OPERATORS")
    print("-" * 55)
    
    print(f"MARKET ACTOR DATABASE:")
    print(f"• Total market actors: 1,000,000+ (across 5 Excel sheets)")
    print(f"• Sample sheet size: {len(operators_df):,}")
    print(f"• Operators with email: {operators_df['email'].notna().sum():,}")
    print(f"• Operators with phone: {operators_df['phone'].notna().sum():,}")
    
    # Focus on biogas-specific operators (from deduplicated data)
    biogas_operators = operators_df[operators_df['market_actor_name'].str.contains('biogas|bio-gas|Bio', case=False, na=False)]
    gas_operators = operators_df[operators_df['market_actor_name'].str.contains('gas', case=False, na=False)]
    energy_operators = operators_df[operators_df['market_actor_name'].str.contains('energie|energy', case=False, na=False)]
    
    print(f"\nSPECIALIZED OPERATORS (Deduplicated, sheet 1 only):")
    print(f"• Biogas specialists: {len(biogas_operators):,}")
    print(f"• Gas companies: {len(gas_operators):,}")
    print(f"• Energy companies: {len(energy_operators):,}")
    
    # Proper operator contact analysis
    operators_with_email = operators_df['email'].notna().sum()
    operators_with_phone = operators_df['phone'].notna().sum()
    print(f"• Operators with email: {operators_with_email:,}")
    print(f"• Operators with phone: {operators_with_phone:,}")
    
    # 3. MARKET OPPORTUNITY ANALYSIS
    print("\n\n💰 MARKET OPPORTUNITY SIZING")
    print("-" * 40)
    
    # Estimate market size
    print(f"MARKET SIZE ESTIMATION:")
    if total_gas_capacity > 0:
        annual_gas_production = total_gas_capacity * 8760  # m³/year (assuming full capacity)
        print(f"• Annual gas production: {annual_gas_production:,.0f} m³/year")
        print(f"• Equivalent energy: {annual_gas_production * 10:.0f} kWh/year")  # ~10 kWh per m³ biogas
        
        # Certificate value estimation (rough)
        cert_value_per_mwh = 5  # €5-20 per MWh typical for green certificates
        annual_cert_value = (annual_gas_production * 10 / 1000) * cert_value_per_mwh
        print(f"• Estimated cert. value: €{annual_cert_value:,.0f}/year (at €{cert_value_per_mwh}/MWh)")
    else:
        # Focus on electrical capacity since no gas injection found
        total_el_mwh_year = (plants_df['capacity_el_kW'].sum() / 1000) * 8760
        cert_value_per_mwh = 5
        annual_cert_value = total_el_mwh_year * cert_value_per_mwh
        print(f"• Annual electrical production: {total_el_mwh_year:,.0f} MWh/year")
        print(f"• Estimated cert. value: €{annual_cert_value:,.0f}/year (at €{cert_value_per_mwh}/MWh)")
        print(f"• Note: Based on electrical generation (biogas BHKW plants)")
    
    # Contact reachability
    print(f"\nREACHABILITY ANALYSIS:")
    print(f"• Contactable gas producers: {len(gas_with_contact):,}")
    print(f"• Biogas operators (sample): {len(biogas_operators):,}")
    print(f"• Market coverage potential: {len(producers_with_contact)/len(plants_df)*100:.1f}% of producers")
    
    # 4. STRATEGIC RECOMMENDATIONS
    print("\n\n🎯 STRATEGIC RECOMMENDATIONS")
    print("-" * 40)
    
    print("IMMEDIATE ACTIONS:")
    print("1. TARGET HIGH-VALUE FACILITIES:")
    if len(gas_with_contact) > 0:
        large_gas_plants = gas_with_contact[gas_with_contact['capacity_gas_m3/h'] > gas_with_contact['capacity_gas_m3/h'].median()]
        print(f"   • Focus on {len(large_gas_plants)} large gas injection plants with contacts")
        print(f"   • Combined capacity: {large_gas_plants['capacity_gas_m3/h'].sum():,.0f} m³/h")
    else:
        # Focus on large electrical biogas plants instead
        large_el_plants = producers_with_contact[producers_with_contact['capacity_el_kW'] > producers_with_contact['capacity_el_kW'].quantile(0.75)]
        print(f"   • Focus on {len(large_el_plants)} large electrical biogas plants (top 25%)")
        print(f"   • Combined capacity: {large_el_plants['capacity_el_kW'].sum():,.0f} kW")
        print(f"   • These likely have potential for gas injection upgrades")
    
    print(f"\n2. CONTACT BIOGAS-SPECIALIZED OPERATORS:")
    biogas_with_contact = biogas_operators[biogas_operators[['email', 'phone']].notna().any(axis=1)]
    print(f"   • {len(biogas_with_contact)} biogas operators with contact details")
    print(f"   • These are likely grid injection points or trading partners")
    
    print(f"\n3. DEVELOP REGIONAL CLUSTERS:")
    top_regions = plants_df['postal_code'].value_counts().head(5)
    print(f"   • Focus on top 5 regions with {top_regions.sum()} plants:")
    for postcode, count in top_regions.items():
        region_contacts = plants_df[(plants_df['postal_code'] == postcode) & 
                                  (plants_df[['email', 'phone']].notna().any(axis=1))]
        print(f"     - {postcode}: {count} plants, {len(region_contacts)} contactable")
    
    print(f"\n4. SUSTAINABILITY VERIFICATION PIPELINE:")
    recent_plants = producers_with_contact[producers_with_contact['commissioning_year'] >= 2015]
    print(f"   • Target {len(recent_plants)} recent plants with modern tech")
    print(f"   • These likely have better sustainability documentation")
    
    # 5. SAMPLE TARGET LISTS
    print("\n\n📋 SAMPLE TARGET LISTS")
    print("-" * 30)
    
    print("TOP FACILITIES (with contacts):")
    if len(gas_with_contact) > 0:
        print("Gas Injection Plants:")
        top_gas_plants = gas_with_contact.nlargest(5, 'capacity_gas_m3/h')
        for idx, plant in top_gas_plants.iterrows():
            contact = plant['email'] if pd.notna(plant['email']) else plant['phone']
            print(f"• {plant['plant_name'][:40]:40} | {plant['capacity_gas_m3/h']:>8,.0f} m³/h | {contact}")
    else:
        print("Large Electrical Biogas Plants (potential for gas injection):")
        top_el_plants = producers_with_contact.nlargest(5, 'capacity_el_kW')
        for idx, plant in top_el_plants.iterrows():
            contact = plant['email'] if pd.notna(plant['email']) else plant['phone']
            print(f"• {plant['plant_name'][:40]:40} | {plant['capacity_el_kW']:>8,.0f} kW | {contact}")
    
    print(f"\nTOP BIOGAS OPERATORS (from deduplicated sheet 1):")
    biogas_with_contact = biogas_operators[biogas_operators[['email', 'phone']].notna().any(axis=1)]
    if len(biogas_with_contact) > 0:
        for idx, operator in biogas_with_contact.head(5).iterrows():
            contact = operator['email'] if pd.notna(operator['email']) else operator['phone']
            print(f"• {operator['market_actor_name'][:40]:40} | {contact}")
    else:
        print("• No biogas operators found with contact details in sample")
    
    print(f"\n🎯 NEXT STEPS:")
    print("1. Extract full operator database from all 5 Excel sheets")
    print("2. Cross-reference operator IDs between plants and market actors")
    print("3. Build contact sequences for certificate trading negotiations")
    print("4. Develop sustainability verification protocols")
    print("5. Create regional trading optimization models")

if __name__ == "__main__":
    detailed_business_analysis()
