#!/usr/bin/env python3
"""
Advanced Operator Deduplication Tool

This script performs intelligent deduplication of market actors, handling cases where:
1. Same operator name with different IDs (legitimate duplicates)
2. Generic names like "Herr", "GbR", etc. that shouldn't be merged
3. Contact information consolidation for deduplicated operators

The goal is to create a truly unique operator database for business analysis.
"""

import pandas as pd
import numpy as np
import re
from collections import defaultdict

def is_generic_name(name):
    """
    Identify generic or placeholder names that shouldn't be deduplicated
    """
    if pd.isna(name) or name.strip() == '':
        return True
    
    name_lower = name.lower().strip()
    
    # Generic titles/placeholders
    generic_patterns = [
        r'^herr$',
        r'^frau$', 
        r'^eheleute$',
        r'^familie$',
        r'^dr\.$',
        r'^prof\.$',
        r'^photovoltaikanlage$',
        r'^solaranlage$',
        r'^windkraftanlage$',
        r'^biogasanlage$',
        # Generic business forms without company name
        r'^gbr$',
        r'^kg$',
        r'^gmbh$',
        r'^ag$',
        r'^ev$',
        r'^e\.v\.$',
        # Very short generic names
        r'^[a-z]{1,2}$',
        # Numbers only
        r'^\d+$'
    ]
    
    for pattern in generic_patterns:
        if re.match(pattern, name_lower):
            return True
    
    # Check for very common surnames that might be generic
    common_surnames = ['müller', 'schmidt', 'schneider', 'fischer', 'weber', 'meyer', 'wagner']
    # Only if it's just the surname + GbR/GmbH
    for surname in common_surnames:
        if re.match(f'^{surname}\\s+(gbr|gmbh)$', name_lower):
            return True
    
    return False

def clean_operator_name(name):
    """
    Clean operator name for better matching
    """
    if pd.isna(name):
        return name
    
    # Remove extra whitespace
    cleaned = re.sub(r'\s+', ' ', str(name).strip())
    
    # Standardize legal forms
    cleaned = re.sub(r'\bGmbH\s*&\s*Co\.\s*KG\b', 'GmbH & Co. KG', cleaned, flags=re.IGNORECASE)
    cleaned = re.sub(r'\bGmbH\s*u\.\s*Co\.\s*KG\b', 'GmbH & Co. KG', cleaned, flags=re.IGNORECASE)
    
    return cleaned

def consolidate_contacts(group):
    """
    Consolidate contact information for operators with the same name
    """
    # Get non-null values for each contact field
    emails = group['email'].dropna().unique()
    phones = group['phone'].dropna().unique()
    websites = group['website'].dropna().unique()
    
    # Take the first non-null value, or combine if multiple
    email = emails[0] if len(emails) > 0 else np.nan
    phone = phones[0] if len(phones) > 0 else np.nan
    website = websites[0] if len(websites) > 0 else np.nan
    
    # If multiple unique values, combine them (for manual review)
    if len(emails) > 1:
        email = '; '.join(emails)
    if len(phones) > 1:
        phone = '; '.join(phones)
    if len(websites) > 1:
        website = '; '.join(websites)
    
    return pd.Series({
        'market_actor_id': group['market_actor_id'].iloc[0],  # Keep first ID
        'market_actor_name': group['market_actor_name'].iloc[0],
        'email': email,
        'phone': phone,
        'website': website,
        'duplicate_count': len(group),
        'all_ids': '; '.join(group['market_actor_id'].astype(str))
    })

def advanced_operator_deduplication():
    """
    Perform advanced deduplication of operators
    """
    print("Loading operators dataset...")
    operators = pd.read_csv('data/processed/german_biogas_all_operators_deduplicated.csv')
    print(f"Original dataset: {len(operators):,} records")
    print(f"Unique names: {operators['market_actor_name'].nunique():,}")
    
    # Clean operator names
    print("\nCleaning operator names...")
    operators['cleaned_name'] = operators['market_actor_name'].apply(clean_operator_name)
    
    # Identify generic names
    print("Identifying generic names...")
    operators['is_generic'] = operators['cleaned_name'].apply(is_generic_name)
    
    generic_count = operators['is_generic'].sum()
    print(f"Generic/placeholder names identified: {generic_count:,}")
    
    # Split into generic and non-generic
    generic_operators = operators[operators['is_generic']].copy()
    unique_operators = operators[~operators['is_generic']].copy()
    
    print(f"Non-generic operators: {len(unique_operators):,}")
    
    # Deduplicate non-generic operators by cleaned name
    print("Deduplicating non-generic operators...")
    deduplicated = unique_operators.groupby('cleaned_name').apply(consolidate_contacts).reset_index(drop=True)
    
    # Add back generic operators (no deduplication)
    generic_final = generic_operators[['market_actor_id', 'market_actor_name', 'email', 'phone', 'website']].copy()
    generic_final['duplicate_count'] = 1
    generic_final['all_ids'] = generic_final['market_actor_id']
    
    # Combine results
    final_operators = pd.concat([deduplicated, generic_final], ignore_index=True)
    
    # Statistics
    print(f"\nDeduplication Results:")
    print(f"Final unique operators: {len(final_operators):,}")
    print(f"Reduction: {len(operators) - len(final_operators):,} records ({(len(operators) - len(final_operators))/len(operators)*100:.1f}%)")
    
    # Show operators with most duplicates
    high_dupes = final_operators[final_operators['duplicate_count'] > 1].sort_values('duplicate_count', ascending=False)
    print(f"\nOperators with duplicates: {len(high_dupes):,}")
    print("\nTop 10 most duplicated operators:")
    print(high_dupes[['market_actor_name', 'duplicate_count']].head(10).to_string(index=False))
    
    # Save results
    output_file = 'data/processed/german_biogas_operators_advanced_deduplicated.csv'
    final_operators.to_csv(output_file, index=False)
    print(f"\nSaved to: {output_file}")
    
    # Create a mapping file for operator consolidation
    mapping_file = 'data/processed/operator_consolidation_mapping.csv'
    mapping_data = []
    
    for _, row in final_operators.iterrows():
        if row['duplicate_count'] > 1:
            original_ids = row['all_ids'].split('; ')
            for original_id in original_ids:
                mapping_data.append({
                    'original_id': original_id,
                    'consolidated_id': row['market_actor_id'],
                    'consolidated_name': row['market_actor_name'],
                    'duplicate_count': row['duplicate_count']
                })
        else:
            mapping_data.append({
                'original_id': row['market_actor_id'],
                'consolidated_id': row['market_actor_id'],
                'consolidated_name': row['market_actor_name'],
                'duplicate_count': 1
            })
    
    mapping_df = pd.DataFrame(mapping_data)
    mapping_df.to_csv(mapping_file, index=False)
    print(f"Operator consolidation mapping saved to: {mapping_file}")
    
    return final_operators, mapping_df

def update_plants_with_consolidated_operators():
    """
    Update the plants dataset to use consolidated operator information
    """
    print("\nUpdating plants dataset with consolidated operators...")
    
    # Load plants and mapping
    plants = pd.read_csv('data/processed/german_biogas_plants_enhanced.csv')
    mapping = pd.read_csv('data/processed/operator_consolidation_mapping.csv')
    operators = pd.read_csv('data/processed/german_biogas_operators_advanced_deduplicated.csv')
    
    print(f"Plants before update: {len(plants):,}")
    print(f"Unique operator IDs in plants: {plants['operator_id'].nunique():,}")
    
    # Use efficient pandas merges instead of row iteration
    print("Merging with operator mapping...")
    plants_with_mapping = plants.merge(
        mapping[['original_id', 'consolidated_id', 'consolidated_name']], 
        left_on='operator_id', 
        right_on='original_id', 
        how='left'
    )
    
    print("Merging with consolidated operator details...")
    plants_updated = plants_with_mapping.merge(
        operators[['market_actor_id', 'market_actor_name', 'email', 'phone', 'website']], 
        left_on='consolidated_id', 
        right_on='market_actor_id', 
        how='left'
    )
    
    # Update columns with consolidated information
    plants_updated['operator_id'] = plants_updated['consolidated_id'].fillna(plants_updated['operator_id'])
    plants_updated['operator_name'] = plants_updated['market_actor_name'].fillna(plants_updated['operator_name'])
    plants_updated['operator_email'] = plants_updated['email'].fillna(plants_updated['operator_email'])
    plants_updated['operator_phone'] = plants_updated['phone'].fillna(plants_updated['operator_phone'])
    plants_updated['operator_website'] = plants_updated['website'].fillna(plants_updated['operator_website'])
    
    # Keep only original columns
    final_columns = plants.columns.tolist()
    plants_updated = plants_updated[final_columns]
    
    # Statistics
    print(f"Unique operator IDs after consolidation: {plants_updated['operator_id'].nunique():,}")
    print(f"Unique operator names after consolidation: {plants_updated['operator_name'].nunique():,}")
    
    # Contact coverage
    has_email = plants_updated['operator_email'].notna().sum()
    has_phone = plants_updated['operator_phone'].notna().sum()
    has_website = plants_updated['operator_website'].notna().sum()
    has_any_contact = plants_updated[['operator_email', 'operator_phone', 'operator_website']].notna().any(axis=1).sum()
    
    print(f"\nContact coverage after consolidation:")
    print(f"Plants with email: {has_email:,} ({has_email/len(plants_updated)*100:.1f}%)")
    print(f"Plants with phone: {has_phone:,} ({has_phone/len(plants_updated)*100:.1f}%)")
    print(f"Plants with website: {has_website:,} ({has_website/len(plants_updated)*100:.1f}%)")
    print(f"Plants with any contact: {has_any_contact:,} ({has_any_contact/len(plants_updated)*100:.1f}%)")
    
    # Save updated plants
    output_file = 'data/processed/german_biogas_plants_consolidated.csv'
    plants_updated.to_csv(output_file, index=False)
    print(f"\nUpdated plants saved to: {output_file}")
    
    return plants_updated

if __name__ == "__main__":
    print("Advanced Operator Deduplication")
    print("=" * 50)
    
    # Perform advanced deduplication
    operators, mapping = advanced_operator_deduplication()
    
    # Update plants dataset
    plants = update_plants_with_consolidated_operators()
    
    print("\n" + "=" * 50)
    print("Advanced deduplication completed!")
    print("\nOutput files:")
    print("- data/processed/german_biogas_operators_advanced_deduplicated.csv")
    print("- data/processed/operator_consolidation_mapping.csv")
    print("- data/processed/german_biogas_plants_consolidated.csv")
