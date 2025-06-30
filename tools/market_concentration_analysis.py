#!/usr/bin/env python3
"""
Market Concentration Analysis for Biogas Certificate Trading
Focus on operator consolidation and market structure
"""

import pandas as pd
import numpy as np

def analyze_market_concentration():
    """Analyze the concentration of the biogas market by operator"""
    
    print("🎯 BIOGAS MARKET CONCENTRATION ANALYSIS")
    print("=" * 60)
    
    # Load the clean dataset
    df = pd.read_csv('data/processed/german_biogas_plants_with_contacts_clean.csv')
    
    print(f"📊 DATASET OVERVIEW:")
    print(f"   Total plant records: {len(df):,}")
    print(f"   Unique physical plants: {df['plant_id'].nunique():,}")
    print(f"   Unique operators: {df['market_actor_name'].nunique():,}")
    
    # Analyze plants per operator
    plants_per_operator = df.groupby('market_actor_name').agg({
        'plant_id': 'count',
        'capacity_el_kW': 'sum',
        'email': lambda x: x.notna().any(),
        'phone': lambda x: x.notna().any()
    }).rename(columns={'plant_id': 'plant_count'})
    
    plants_per_operator['has_contact'] = (
        plants_per_operator['email'] | plants_per_operator['phone']
    )
    
    # Sort by plant count
    plants_per_operator = plants_per_operator.sort_values('plant_count', ascending=False)
    
    print(f"\n🏭 TOP 15 BIOGAS OPERATORS BY PLANT COUNT:")
    print("-" * 60)
    for i, (operator, data) in enumerate(plants_per_operator.head(15).iterrows(), 1):
        contact_status = "✅" if data['has_contact'] else "❌"
        operator_name = str(operator)[:50] if operator else "Unknown"
        print(f"{i:2d}. {operator_name:<50} {data['plant_count']:3d} plants {contact_status}")
    
    # Market concentration analysis
    print(f"\n📈 MARKET CONCENTRATION ANALYSIS:")
    print("-" * 40)
    
    # Top operators by different thresholds
    thresholds = [1, 5, 10, 20, 50]
    for threshold in thresholds:
        large_operators = plants_per_operator[plants_per_operator['plant_count'] >= threshold]
        plants_controlled = large_operators['plant_count'].sum()
        capacity_controlled = large_operators['capacity_el_kW'].sum()
        
        print(f"Operators with {threshold:2d}+ plants: {len(large_operators):3d} operators")
        print(f"   • Control {plants_controlled:5,} plants ({plants_controlled/len(df)*100:4.1f}%)")
        print(f"   • Control {capacity_controlled:8,.0f} kW ({capacity_controlled/df['capacity_el_kW'].sum()*100:4.1f}%)")
    
    # Contact coverage by operator size
    print(f"\n📞 CONTACT COVERAGE BY OPERATOR SIZE:")
    print("-" * 40)
    
    # Large operators (10+ plants)
    large_ops = plants_per_operator[plants_per_operator['plant_count'] >= 10]
    large_ops_with_contact = large_ops[large_ops['has_contact']]
    
    print(f"Large operators (10+ plants):")
    print(f"   • Total: {len(large_ops):,}")
    print(f"   • With contact info: {len(large_ops_with_contact):,} ({len(large_ops_with_contact)/len(large_ops)*100:.1f}%)")
    print(f"   • Plants controlled: {large_ops['plant_count'].sum():,}")
    
    # Small operators (1-2 plants)
    small_ops = plants_per_operator[plants_per_operator['plant_count'] <= 2]
    small_ops_with_contact = small_ops[small_ops['has_contact']]
    
    print(f"\nSmall operators (1-2 plants):")
    print(f"   • Total: {len(small_ops):,}")
    print(f"   • With contact info: {len(small_ops_with_contact):,} ({len(small_ops_with_contact)/len(small_ops)*100:.1f}%)")
    print(f"   • Plants controlled: {small_ops['plant_count'].sum():,}")
    
    print(f"\n🎯 BUSINESS STRATEGY RECOMMENDATIONS:")
    print("-" * 40)
    print(f"1. Target {len(large_ops):,} large operators first (control {large_ops['plant_count'].sum():,} plants)")
    print(f"2. Focus on {len(large_ops_with_contact):,} contactable large operators")
    print(f"3. Market is fragmented: {len(small_ops):,} small operators control {small_ops['plant_count'].sum():,} plants")
    print(f"4. Overall contact rate: {len(plants_per_operator[plants_per_operator['has_contact']]):,}/{len(plants_per_operator):,} operators ({len(plants_per_operator[plants_per_operator['has_contact']])/len(plants_per_operator)*100:.1f}%)")

if __name__ == "__main__":
    analyze_market_concentration()
