#!/usr/bin/env python3
"""
Enhanced Data Processing for Biogas Certificate Trading
- Extract and deduplicate full operator database from all Excel sheets
- Cross-reference operator IDs between plants and market actors
- Clean and optimize datasets for trading analysis
"""

import pandas as pd
import numpy as np
from pathlib import Path

def extract_full_operator_database():
    """Extract and deduplicate operators from all 5 Excel sheets"""
    print("📊 EXTRACTING FULL OPERATOR DATABASE")
    print("=" * 50)
    
    # Load all sheets from the Excel file
    excel_file = "german_biogas_operator_contacts.xlsx"
    all_operators = []
    
    print("Loading Excel sheets...")
    for i in range(1, 6):  # contacts_1 through contacts_5
        sheet_name = f"contacts_{i}"
        try:
            df = pd.read_excel(excel_file, sheet_name=sheet_name)
            print(f"• {sheet_name}: {len(df):,} records")
            all_operators.append(df)
        except Exception as e:
            print(f"• {sheet_name}: Error loading - {e}")
    
    # Combine all sheets
    if all_operators:
        combined_operators = pd.concat(all_operators, ignore_index=True)
        print(f"\nCombined: {len(combined_operators):,} total records")
        
        # Deduplicate by market_actor_id
        original_count = len(combined_operators)
        combined_operators = combined_operators.drop_duplicates(subset=['market_actor_id'])
        duplicates_removed = original_count - len(combined_operators)
        
        print(f"After deduplication: {len(combined_operators):,} unique operators")
        print(f"Duplicates removed: {duplicates_removed:,}")
        
        # Save to CSV
        output_file = "german_biogas_all_operators_deduplicated.csv"
        combined_operators.to_csv(output_file, index=False)
        print(f"✅ Saved to: {output_file}")
        
        return combined_operators
    else:
        print("❌ No sheets loaded successfully")
        return pd.DataFrame()

def cross_reference_operators_plants():
    """Cross-reference operator IDs between plants and market actors"""
    print("\n🔄 CROSS-REFERENCING PLANTS AND OPERATORS")
    print("=" * 50)
    
    # Load deduplicated data
    plants_df = pd.read_csv("german_biogas_plants_with_contacts.csv")
    operators_df = pd.read_csv("german_biogas_all_operators_deduplicated.csv")
    
    print(f"Plants: {len(plants_df):,}")
    print(f"Operators: {len(operators_df):,}")
    
    # Cross-reference analysis
    plant_operators = set(plants_df['operator_id'].dropna())
    market_actors = set(operators_df['market_actor_id'].dropna())
    
    matched_operators = plant_operators.intersection(market_actors)
    unmatched_plant_operators = plant_operators - market_actors
    unmatched_market_actors = market_actors - plant_operators
    
    print(f"\n📈 CROSS-REFERENCE RESULTS:")
    print(f"• Plant operators: {len(plant_operators):,}")
    print(f"• Market actors: {len(market_actors):,}")
    print(f"• Matched operators: {len(matched_operators):,}")
    print(f"• Unmatched plant operators: {len(unmatched_plant_operators):,}")
    print(f"• Unmatched market actors: {len(unmatched_market_actors):,}")
    print(f"• Match rate: {len(matched_operators)/len(plant_operators)*100:.1f}%")
    
    # Create enhanced plant dataset with full operator details
    plants_enhanced = plants_df.merge(
        operators_df[['market_actor_id', 'market_actor_name', 'email', 'phone', 'website']], 
        left_on='operator_id', 
        right_on='market_actor_id', 
        how='left',
        suffixes=('_plant', '_operator')
    )
    
    # Use operator details where plant details are missing
    plants_enhanced['email_final'] = plants_enhanced['email_operator'].fillna(plants_enhanced['email_plant'])
    plants_enhanced['phone_final'] = plants_enhanced['phone_operator'].fillna(plants_enhanced['phone_plant'])
    plants_enhanced['name_final'] = plants_enhanced['market_actor_name_operator'].fillna(plants_enhanced['market_actor_name_plant'])
    
    # Clean up columns
    plants_final = plants_enhanced[[
        'plant_id', 'plant_name', 'postal_code', 'commissioning_year',
        'capacity_el_kW', 'capacity_gas_m3/h', 'operator_id', 'latitude', 'longitude', 'plant_type',
        'name_final', 'email_final', 'phone_final', 'website'
    ]].rename(columns={
        'name_final': 'operator_name',
        'email_final': 'operator_email', 
        'phone_final': 'operator_phone',
        'website': 'operator_website'
    })
    
    # Save enhanced dataset
    output_file = "german_biogas_plants_enhanced.csv"
    plants_final.to_csv(output_file, index=False)
    
    print(f"\n✅ Enhanced plant dataset saved: {output_file}")
    print(f"   • Total plants: {len(plants_final):,}")
    print(f"   • With operator email: {plants_final['operator_email'].notna().sum():,}")
    print(f"   • With operator phone: {plants_final['operator_phone'].notna().sum():,}")
    
    return plants_final, operators_df

def deduplicate_and_clean_datasets():
    """Remove duplicates from all datasets and create clean versions"""
    print("\n🧹 CLEANING AND DEDUPLICATING ALL DATASETS")
    print("=" * 50)
    
    # 1. Clean plants dataset
    plants_df = pd.read_csv("german_biogas_plants_with_contacts.csv")
    original_plant_count = len(plants_df)
    
    # Remove duplicates by plant_id
    plants_clean = plants_df.drop_duplicates(subset=['plant_id'])
    plant_duplicates = original_plant_count - len(plants_clean)
    
    # Save cleaned plants
    plants_clean.to_csv("german_biogas_plants_with_contacts_clean.csv", index=False)
    
    print(f"PLANTS CLEANING:")
    print(f"• Original: {original_plant_count:,}")
    print(f"• After deduplication: {len(plants_clean):,}")
    print(f"• Duplicates removed: {plant_duplicates:,}")
    print(f"• Saved to: german_biogas_plants_with_contacts_clean.csv")
    
    # 2. Check if operators file exists and is already clean
    operators_file = "german_biogas_all_operators_deduplicated.csv"
    if Path(operators_file).exists():
        operators_df = pd.read_csv(operators_file)
        print(f"\nOPERATORS DATASET:")
        print(f"• Already deduplicated: {len(operators_df):,} records")
        print(f"• File: {operators_file}")
    else:
        print(f"\n⚠️  Operators file not found: {operators_file}")
        print("   Run extract_full_operator_database() first")
    
    return plants_clean

def generate_business_summary():
    """Generate summary statistics for business analysis"""
    print("\n📊 BUSINESS SUMMARY STATISTICS")
    print("=" * 40)
    
    # Load clean datasets
    try:
        plants_df = pd.read_csv("german_biogas_plants_enhanced.csv")
        operators_df = pd.read_csv("german_biogas_all_operators_deduplicated.csv")
        
        print("DATASET SIZES:")
        print(f"• Unique plants: {len(plants_df):,}")
        print(f"• Unique operators: {len(operators_df):,}")
        
        # Contact coverage
        plants_with_email = plants_df['operator_email'].notna().sum()
        plants_with_phone = plants_df['operator_phone'].notna().sum()
        plants_with_contact = plants_df[['operator_email', 'operator_phone']].notna().any(axis=1).sum()
        
        print(f"\nCONTACT COVERAGE:")
        print(f"• Plants with operator email: {plants_with_email:,} ({plants_with_email/len(plants_df)*100:.1f}%)")
        print(f"• Plants with operator phone: {plants_with_phone:,} ({plants_with_phone/len(plants_df)*100:.1f}%)")
        print(f"• Plants with any contact: {plants_with_contact:,} ({plants_with_contact/len(plants_df)*100:.1f}%)")
        
        # Capacity analysis
        gas_plants = plants_df[plants_df['capacity_gas_m3/h'] > 0]
        total_gas_capacity = gas_plants['capacity_gas_m3/h'].sum()
        total_el_capacity = plants_df['capacity_el_kW'].sum()
        
        print(f"\nCAPACITY ANALYSIS:")
        print(f"• Gas injection plants: {len(gas_plants):,}")
        print(f"• Total gas capacity: {total_gas_capacity:,.0f} m³/h")
        print(f"• Total electrical capacity: {total_el_capacity:,.0f} kW")
        
        # Market opportunity
        annual_gas_mwh = total_gas_capacity * 8760 * 10 / 1000  # Convert to MWh/year
        annual_el_mwh = total_el_capacity * 8760 / 1000
        cert_value_5eur = (annual_gas_mwh + annual_el_mwh) * 5
        
        print(f"\nMARKET OPPORTUNITY (at €5/MWh):")
        print(f"• Annual gas production: {annual_gas_mwh:,.0f} MWh/year")
        print(f"• Annual electrical production: {annual_el_mwh:,.0f} MWh/year")
        print(f"• Total certificate value: €{cert_value_5eur:,.0f}/year")
        
    except FileNotFoundError as e:
        print(f"❌ Error: {e}")
        print("   Make sure to run the data processing steps first")

def main():
    """Run all data processing steps"""
    print("🚀 BIOGAS DATABASE ENHANCEMENT PIPELINE")
    print("=" * 60)
    
    # Step 1: Extract full operator database
    operators_df = extract_full_operator_database()
    
    # Step 2: Cross-reference operators and plants
    if not operators_df.empty:
        plants_final, operators_final = cross_reference_operators_plants()
    
    # Step 3: Clean and deduplicate datasets
    plants_clean = deduplicate_and_clean_datasets()
    
    # Step 4: Generate business summary
    generate_business_summary()
    
    print(f"\n🎯 FINAL OUTPUT FILES:")
    print("• german_biogas_all_operators_deduplicated.csv - Complete operator database")
    print("• german_biogas_plants_enhanced.csv - Plants with full operator details")
    print("• german_biogas_plants_with_contacts_clean.csv - Original plants cleaned")
    print("\n✅ Data processing complete!")

if __name__ == "__main__":
    main()
