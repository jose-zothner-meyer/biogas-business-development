#!/usr/bin/env python3
"""
Biogas Operator Deduplication Tool

Focused deduplication specifically for biogas plant operators.
This script is optimized to handle only the operators that actually operate biogas plants,
rather than the entire 4.7M operator database.
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

def deduplicate_biogas_operators():
    """
    Deduplicate only the operators that operate biogas plants
    """
    print("Loading biogas plants dataset...")
    plants = pd.read_csv('data/processed/german_biogas_plants_enhanced.csv')
    print(f"Biogas plants: {len(plants):,}")
    print(f"Unique plant IDs: {plants['plant_id'].nunique():,}")
    print(f"Unique operator IDs in plants: {plants['operator_id'].nunique():,}")
    print(f"Unique operator names in plants: {plants['operator_name'].nunique():,}")
    
    # Extract unique operators from plants
    plant_operators = plants[['operator_id', 'operator_name', 'operator_email', 'operator_phone', 'operator_website']].drop_duplicates()
    print(f"\nUnique biogas plant operators: {len(plant_operators):,}")
    
    # Clean operator names
    print("Cleaning operator names...")
    plant_operators['cleaned_name'] = plant_operators['operator_name'].apply(clean_operator_name)
    
    # Identify generic names
    plant_operators['is_generic'] = plant_operators['cleaned_name'].apply(is_generic_name)
    
    generic_count = plant_operators['is_generic'].sum()
    print(f"Generic/placeholder names: {generic_count:,}")
    
    # Analyze name duplicates
    name_counts = plant_operators['cleaned_name'].value_counts()
    duplicated_names = name_counts[name_counts > 1]
    print(f"Operator names with duplicates: {len(duplicated_names):,}")
    
    print("\nTop 20 most duplicated operator names:")
    for name, count in duplicated_names.head(20).items():
        is_gen = is_generic_name(name)
        print(f"  {name}: {count} occurrences {'(GENERIC)' if is_gen else ''}")
    
    # Group by cleaned name and consolidate
    print("\nConsolidating operators by name...")
    consolidated = []
    
    for name, group in plant_operators.groupby('cleaned_name'):
        if is_generic_name(name):
            # Keep generic names separate (don't consolidate)
            for _, row in group.iterrows():
                consolidated.append({
                    'operator_id': row['operator_id'],
                    'operator_name': row['operator_name'],
                    'operator_email': row['operator_email'],
                    'operator_phone': row['operator_phone'],
                    'operator_website': row['operator_website'],
                    'consolidated_name': row['cleaned_name'],
                    'duplicate_count': 1,
                    'all_ids': row['operator_id'],
                    'is_generic': True
                })
        else:
            # Consolidate non-generic names
            # Collect all contact info
            emails = group['operator_email'].dropna().unique()
            phones = group['operator_phone'].dropna().unique()
            websites = group['operator_website'].dropna().unique()
            
            # Take best contact info
            email = emails[0] if len(emails) > 0 else np.nan
            phone = phones[0] if len(phones) > 0 else np.nan
            website = websites[0] if len(websites) > 0 else np.nan
            
            # If multiple unique values, combine them
            if len(emails) > 1:
                email = '; '.join(emails)
            if len(phones) > 1:
                phone = '; '.join(phones)
            if len(websites) > 1:
                website = '; '.join(websites)
            
            consolidated.append({
                'operator_id': group['operator_id'].iloc[0],  # Use first ID as master
                'operator_name': group['operator_name'].iloc[0],  # Use first name variant
                'operator_email': email,
                'operator_phone': phone,
                'operator_website': website,
                'consolidated_name': name,
                'duplicate_count': len(group),
                'all_ids': '; '.join(group['operator_id'].astype(str)),
                'is_generic': False
            })
    
    consolidated_df = pd.DataFrame(consolidated)
    
    print(f"\nConsolidation results:")
    print(f"Original operators: {len(plant_operators):,}")
    print(f"Consolidated operators: {len(consolidated_df):,}")
    print(f"Reduction: {len(plant_operators) - len(consolidated_df):,} ({(len(plant_operators) - len(consolidated_df))/len(plant_operators)*100:.1f}%)")
    
    # Show consolidation stats
    non_generic = consolidated_df[~consolidated_df['is_generic']]
    with_duplicates = non_generic[non_generic['duplicate_count'] > 1]
    print(f"Non-generic operators consolidated: {len(with_duplicates):,}")
    print(f"Average duplicates per consolidated operator: {with_duplicates['duplicate_count'].mean():.1f}")
    
    print("\nTop 10 most consolidated operators:")
    top_consolidated = with_duplicates.sort_values('duplicate_count', ascending=False).head(10)
    for _, row in top_consolidated.iterrows():
        print(f"  {row['operator_name']}: {row['duplicate_count']} duplicates")
    
    # Save consolidated operators
    output_file = 'data/processed/biogas_operators_consolidated.csv'
    consolidated_df.to_csv(output_file, index=False)
    print(f"\nSaved consolidated operators to: {output_file}")
    
    # Create mapping for updating plants
    mapping_data = []
    for _, row in consolidated_df.iterrows():
        if row['duplicate_count'] > 1:
            original_ids = row['all_ids'].split('; ')
            for original_id in original_ids:
                mapping_data.append({
                    'original_id': original_id,
                    'consolidated_id': row['operator_id'],
                    'consolidated_name': row['operator_name']
                })
        else:
            mapping_data.append({
                'original_id': row['operator_id'],
                'consolidated_id': row['operator_id'],
                'consolidated_name': row['operator_name']
            })
    
    mapping_df = pd.DataFrame(mapping_data)
    mapping_file = 'data/processed/biogas_operator_mapping.csv'
    mapping_df.to_csv(mapping_file, index=False)
    print(f"Mapping saved to: {mapping_file}")
    
    return consolidated_df, mapping_df

def update_plants_with_consolidated_operators():
    """
    Update plants with consolidated operator information
    """
    print("\nUpdating plants with consolidated operators...")
    
    plants = pd.read_csv('data/processed/german_biogas_plants_enhanced.csv')
    mapping = pd.read_csv('data/processed/biogas_operator_mapping.csv')
    operators = pd.read_csv('data/processed/biogas_operators_consolidated.csv')
    
    print(f"Plants: {len(plants):,}")
    print(f"Original unique operators: {plants['operator_id'].nunique():,}")
    
    # Merge with mapping to get consolidated IDs
    plants_mapped = plants.merge(mapping, left_on='operator_id', right_on='original_id', how='left')
    
    # Merge with consolidated operator details
    plants_updated = plants_mapped.merge(
        operators[['operator_id', 'operator_name', 'operator_email', 'operator_phone', 'operator_website']], 
        left_on='consolidated_id', 
        right_on='operator_id', 
        how='left',
        suffixes=('_old', '_new')
    )
    
    # Update with consolidated information where available
    plants_updated['operator_id'] = plants_updated['consolidated_id'].fillna(plants_updated['operator_id_old'])
    plants_updated['operator_name'] = plants_updated['operator_name_new'].fillna(plants_updated['operator_name_old'])
    plants_updated['operator_email'] = plants_updated['operator_email_new'].fillna(plants_updated['operator_email_old'])
    plants_updated['operator_phone'] = plants_updated['operator_phone_new'].fillna(plants_updated['operator_phone_old'])
    plants_updated['operator_website'] = plants_updated['operator_website_new'].fillna(plants_updated['operator_website_old'])
    
    # Keep only original columns
    final_columns = ['plant_id', 'plant_name', 'postal_code', 'commissioning_year', 'capacity_el_kW', 
                    'capacity_gas_m3/h', 'operator_id', 'latitude', 'longitude', 'plant_type', 
                    'operator_name', 'operator_email', 'operator_phone', 'operator_website']
    plants_final = plants_updated[final_columns]
    
    print(f"Final unique operators: {plants_final['operator_id'].nunique():,}")
    print(f"Final unique operator names: {plants_final['operator_name'].nunique():,}")
    
    # Contact coverage stats
    has_email = plants_final['operator_email'].notna().sum()
    has_phone = plants_final['operator_phone'].notna().sum()
    has_website = plants_final['operator_website'].notna().sum()
    has_any_contact = plants_final[['operator_email', 'operator_phone', 'operator_website']].notna().any(axis=1).sum()
    
    print(f"\nContact coverage:")
    print(f"Plants with email: {has_email:,} ({has_email/len(plants_final)*100:.1f}%)")
    print(f"Plants with phone: {has_phone:,} ({has_phone/len(plants_final)*100:.1f}%)")
    print(f"Plants with website: {has_website:,} ({has_website/len(plants_final)*100:.1f}%)")
    print(f"Plants with any contact: {has_any_contact:,} ({has_any_contact/len(plants_final)*100:.1f}%)")
    
    # Operator-level stats
    unique_operators = plants_final.groupby('operator_id').first()
    op_has_email = unique_operators['operator_email'].notna().sum()
    op_has_phone = unique_operators['operator_phone'].notna().sum()
    op_has_website = unique_operators['operator_website'].notna().sum()
    op_has_any_contact = unique_operators[['operator_email', 'operator_phone', 'operator_website']].notna().any(axis=1).sum()
    
    print(f"\nOperator contact coverage:")
    print(f"Operators with email: {op_has_email:,} ({op_has_email/len(unique_operators)*100:.1f}%)")
    print(f"Operators with phone: {op_has_phone:,} ({op_has_phone/len(unique_operators)*100:.1f}%)")
    print(f"Operators with website: {op_has_website:,} ({op_has_website/len(unique_operators)*100:.1f}%)")
    print(f"Operators with any contact: {op_has_any_contact:,} ({op_has_any_contact/len(unique_operators)*100:.1f}%)")
    
    # Save final dataset
    output_file = 'data/processed/german_biogas_plants_final.csv'
    plants_final.to_csv(output_file, index=False)
    print(f"\nFinal plants dataset saved to: {output_file}")
    
    return plants_final

if __name__ == "__main__":
    print("Biogas Operator Deduplication Tool")
    print("=" * 50)
    
    # Deduplicate biogas operators
    operators, mapping = deduplicate_biogas_operators()
    
    # Update plants dataset
    plants = update_plants_with_consolidated_operators()
    
    print("\n" + "=" * 50)
    print("Biogas operator deduplication completed!")
    print("\nOutput files:")
    print("- data/processed/biogas_operators_consolidated.csv")
    print("- data/processed/biogas_operator_mapping.csv") 
    print("- data/processed/german_biogas_plants_final.csv")
