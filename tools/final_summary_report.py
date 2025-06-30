#!/usr/bin/env python3
"""
Final Dataset Summary Report

This script generates a comprehensive summary of the corrected German biogas database
after proper operator deduplication.
"""

import pandas as pd
import numpy as np

def generate_summary_report():
    """
    Generate a comprehensive summary report of the final dataset
    """
    print("GERMAN BIOGAS DATABASE - FINAL SUMMARY REPORT")
    print("=" * 60)
    
    # Load final dataset
    plants = pd.read_csv('data/processed/german_biogas_plants_final.csv')
    operators = pd.read_csv('data/processed/biogas_operators_consolidated.csv')
    
    print(f"Report Date: 2025-06-30")
    print(f"Data Source: Marktstammdatenregister (MaStR)")
    print()
    
    # Basic statistics
    print("📊 DATASET OVERVIEW")
    print("-" * 30)
    print(f"Total biogas plants: {len(plants):,}")
    print(f"Unique plant IDs: {plants['plant_id'].nunique():,}")
    print(f"Unique operator IDs (after consolidation): {plants['operator_id'].nunique():,}")
    print(f"Unique operator names: {plants['operator_name'].nunique():,}")
    print(f"Consolidated operators dataset: {len(operators):,} records")
    print()
    
    # Market concentration
    print("🏢 MARKET CONCENTRATION ANALYSIS")
    print("-" * 40)
    operator_counts = plants['operator_name'].value_counts()
    
    print(f"Total unique operators: {len(operator_counts):,}")
    print(f"Single-plant operators: {(operator_counts == 1).sum():,} ({(operator_counts == 1).sum()/len(operator_counts)*100:.1f}%)")
    print(f"Multi-plant operators: {(operator_counts > 1).sum():,} ({(operator_counts > 1).sum()/len(operator_counts)*100:.1f}%)")
    print()
    
    print("Market segmentation:")
    print(f"  • 2-4 plants: {((operator_counts >= 2) & (operator_counts <= 4)).sum():,} operators")
    print(f"  • 5-9 plants: {((operator_counts >= 5) & (operator_counts <= 9)).sum():,} operators")
    print(f"  • 10-19 plants: {((operator_counts >= 10) & (operator_counts <= 19)).sum():,} operators")
    print(f"  • 20+ plants: {(operator_counts >= 20).sum():,} operators")
    print()
    
    # Top operators
    print("🏆 TOP 15 LARGEST BIOGAS OPERATORS")
    print("-" * 45)
    for i, (name, count) in enumerate(operator_counts.head(15).items(), 1):
        print(f"{i:2d}. {name:<50} {count:3d} plants")
    print()
    
    # Geographic distribution
    print("📍 GEOGRAPHIC DISTRIBUTION (Top Postal Codes)")
    print("-" * 50)
    postal_counts = plants['postal_code'].value_counts().head(10)
    for i, (postal, count) in enumerate(postal_counts.items(), 1):
        print(f"{i:2d}. {postal}: {count} plants")
    print()
    
    # Capacity analysis
    print("⚡ CAPACITY ANALYSIS")
    print("-" * 25)
    total_el_capacity = plants['capacity_el_kW'].sum() / 1000  # Convert to MW
    avg_el_capacity = plants['capacity_el_kW'].mean()
    
    plants_with_gas = plants['capacity_gas_m3/h'].notna()
    total_gas_capacity = plants[plants_with_gas]['capacity_gas_m3/h'].sum() / 1_000_000  # Convert to million m³/h
    
    print(f"Total electrical capacity: {total_el_capacity:,.1f} MW")
    print(f"Average electrical capacity per plant: {avg_el_capacity:.1f} kW")
    print(f"Plants with gas capacity data: {plants_with_gas.sum():,} ({plants_with_gas.sum()/len(plants)*100:.1f}%)")
    print(f"Total gas capacity: {total_gas_capacity:.1f} million m³/h")
    print()
    
    # Commissioning timeline
    print("📅 COMMISSIONING TIMELINE")
    print("-" * 30)
    valid_years = plants['commissioning_year'].dropna()
    valid_years = valid_years[(valid_years >= 1990) & (valid_years <= 2025)]
    
    if len(valid_years) > 0:
        year_counts = valid_years.value_counts().sort_index()
        peak_year = year_counts.idxmax()
        peak_count = year_counts.max()
        
        print(f"Plants with valid commissioning data: {len(valid_years):,} ({len(valid_years)/len(plants)*100:.1f}%)")
        print(f"Peak construction year: {peak_year} ({peak_count} plants)")
        print(f"Most recent installations: {year_counts.tail(5).to_dict()}")
    print()
    
    # Contact coverage analysis
    print("📞 CONTACT COVERAGE ANALYSIS")
    print("-" * 35)
    
    # Plant-level contact coverage
    has_email = plants['operator_email'].notna().sum()
    has_phone = plants['operator_phone'].notna().sum() 
    has_website = plants['operator_website'].notna().sum()
    has_any_contact = plants[['operator_email', 'operator_phone', 'operator_website']].notna().any(axis=1).sum()
    
    print("Plant-level coverage:")
    print(f"  • Email: {has_email:,} plants ({has_email/len(plants)*100:.1f}%)")
    print(f"  • Phone: {has_phone:,} plants ({has_phone/len(plants)*100:.1f}%)")
    print(f"  • Website: {has_website:,} plants ({has_website/len(plants)*100:.1f}%)")
    print(f"  • Any contact: {has_any_contact:,} plants ({has_any_contact/len(plants)*100:.1f}%)")
    print()
    
    # Operator-level contact coverage
    unique_operators = plants.groupby('operator_name').first()
    op_has_email = unique_operators['operator_email'].notna().sum()
    op_has_phone = unique_operators['operator_phone'].notna().sum()
    op_has_website = unique_operators['operator_website'].notna().sum()
    op_has_any_contact = unique_operators[['operator_email', 'operator_phone', 'operator_website']].notna().any(axis=1).sum()
    
    print("Operator-level coverage:")
    print(f"  • Email: {op_has_email:,} operators ({op_has_email/len(unique_operators)*100:.1f}%)")
    print(f"  • Phone: {op_has_phone:,} operators ({op_has_phone/len(unique_operators)*100:.1f}%)")
    print(f"  • Website: {op_has_website:,} operators ({op_has_website/len(unique_operators)*100:.1f}%)")
    print(f"  • Any contact: {op_has_any_contact:,} operators ({op_has_any_contact/len(unique_operators)*100:.1f}%)")
    print()
    
    # Deduplication impact
    print("🔄 DEDUPLICATION IMPACT")
    print("-" * 30)
    original_operators = 11194  # From the deduplication output
    final_operators = len(operators)
    reduction = original_operators - final_operators
    
    print(f"Original operator records: {original_operators:,}")
    print(f"Final consolidated operators: {final_operators:,}")
    print(f"Records reduced: {reduction:,} ({reduction/original_operators*100:.1f}%)")
    print(f"Duplicate consolidation: 57 operator names had multiple IDs")
    print(f"Generic names preserved: 3,133 (kept as separate entities)")
    print()
    
    # Business insights
    print("💼 BUSINESS INTELLIGENCE INSIGHTS")
    print("-" * 40)
    
    multi_plant_ops = operator_counts[operator_counts > 1]
    major_ops = operator_counts[operator_counts >= 10]
    
    print("Strategic segments:")
    print(f"  • Single-plant operators: {(operator_counts == 1).sum():,} (individual farmers/small businesses)")
    print(f"  • Multi-plant portfolios: {len(multi_plant_ops):,} operators ({multi_plant_ops.sum():,} plants)")
    print(f"  • Major industry players: {len(major_ops):,} operators ({major_ops.sum():,} plants)")
    print()
    
    print("Target recommendations:")
    print(f"  • High-value prospects: {(operator_counts >= 5).sum():,} operators with 5+ plants")
    print(f"  • Direct outreach ready: {op_has_any_contact:,} operators with contact info")
    print(f"  • Market coverage potential: {has_any_contact/len(plants)*100:.1f}% of plants accessible")
    print()
    
    print("=" * 60)
    print("REPORT COMPLETE - Data ready for business development use")
    print(f"Primary dataset: data/processed/german_biogas_plants_final.csv")
    print(f"Operators dataset: data/processed/biogas_operators_consolidated.csv")

if __name__ == "__main__":
    generate_summary_report()
