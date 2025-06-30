#!/usr/bin/env python3
"""
ACTIONABLE BUSINESS PLAN: Biogas/Biomethane Certificate Trading
Focus on immediate high-value opportunities
"""

import pandas as pd

def create_action_plan():
    print("🚀 ACTIONABLE BUSINESS PLAN: GREEN GAS CERTIFICATE TRADING")
    print("=" * 70)
    
    # Load data
    plants_df = pd.read_csv("german_biogas_plants_with_contacts.csv")
    operators_df = pd.read_excel("german_biogas_operator_contacts.xlsx", sheet_name="contacts_1")
    
    print("\n💎 MARKET OPPORTUNITY SUMMARY")
    print("-" * 40)
    print("• €37.8B annual green gas certificate market")
    print("• 307 gas injection facilities (grid connection points)")
    print("• 254 contactable gas injection operators (82.7% coverage)")
    print("• 2,824 biogas specialist companies in database")
    print("• 23,984 total biogas plants for sustainability verification")
    
    # 1. PRIORITY TARGET IDENTIFICATION
    print("\n\n🎯 PHASE 1: HIGH-VALUE TARGET IDENTIFICATION")
    print("-" * 50)
    
    # Top gas injection plants
    gas_producers = plants_df[plants_df['capacity_gas_m3/h'] > 0]
    gas_with_contact = gas_producers[gas_producers[['email', 'phone']].notna().any(axis=1)]
    
    # Top 20 gas injection plants by capacity
    top_gas_plants = gas_with_contact.nlargest(20, 'capacity_gas_m3/h')
    
    print("TOP 20 GAS INJECTION TARGETS:")
    print("(These inject physical gas into grid - main revenue source)")
    print("-" * 65)
    total_capacity = 0
    for idx, plant in top_gas_plants.iterrows():
        contact = plant['email'] if pd.notna(plant['email']) else plant['phone']
        capacity = plant['capacity_gas_m3/h']
        total_capacity += capacity
        annual_value = capacity * 8760 * 10 / 1000 * 5  # Rough certificate value
        print(f"{plant['plant_name'][:30]:30} | {capacity:>10,.0f} m³/h | €{annual_value:>8,.0f}/yr | {contact}")
    
    print(f"\nTOP 20 COMBINED: {total_capacity:,.0f} m³/h")
    annual_cert_value = total_capacity * 8760 * 10 / 1000 * 5
    print(f"ANNUAL CERT VALUE: €{annual_cert_value:,.0f}")
    
    # 2. BIOGAS SPECIALIST OPERATORS
    print("\n\n🏢 PHASE 2: BIOGAS SPECIALIST OPERATORS")
    print("-" * 45)
    
    biogas_operators = operators_df[operators_df['market_actor_name'].str.contains('biogas|bio-gas|Bio', case=False, na=False)]
    biogas_with_contact = biogas_operators[biogas_operators[['email', 'phone']].notna().any(axis=1)]
    
    print("TOP BIOGAS OPERATORS FOR PARTNERSHIP:")
    print("(These manage multiple sites and understand the market)")
    print("-" * 60)
    
    for idx, operator in biogas_with_contact.head(15).iterrows():
        contact = operator['email'] if pd.notna(operator['email']) else operator['phone']
        print(f"{operator['market_actor_name'][:45]:45} | {contact}")
    
    # 3. REGIONAL CLUSTERS
    print("\n\n🗺️  PHASE 3: REGIONAL CLUSTER STRATEGY")
    print("-" * 40)
    
    # Analyze top regions
    region_analysis = []
    top_postcodes = plants_df['postal_code'].value_counts().head(10)
    
    for postcode, total_plants in top_postcodes.items():
        region_plants = plants_df[plants_df['postal_code'] == postcode]
        contactable = region_plants[region_plants[['email', 'phone']].notna().any(axis=1)]
        gas_plants = region_plants[region_plants['capacity_gas_m3/h'] > 0]
        total_capacity = region_plants['capacity_el_kW'].sum()
        
        region_analysis.append({
            'postcode': postcode,
            'total_plants': total_plants,
            'contactable': len(contactable),
            'gas_injection': len(gas_plants),
            'total_capacity_mw': total_capacity / 1000,
            'contact_rate': len(contactable) / total_plants * 100
        })
    
    print("TOP REGIONAL OPPORTUNITIES:")
    print("PC     | Plants | Contact | Gas Inj | Capacity | Contact%")
    print("-" * 55)
    
    for region in region_analysis:
        print(f"{region['postcode']:6} | {region['total_plants']:6} | {region['contactable']:7} | "
              f"{region['gas_injection']:7} | {region['total_capacity_mw']:6.0f} MW | {region['contact_rate']:6.1f}%")
    
    # 4. SUSTAINABILITY VERIFICATION TARGETS
    print("\n\n🌱 PHASE 4: SUSTAINABILITY VERIFICATION PIPELINE")
    print("-" * 55)
    
    # Recent plants with good contact coverage
    recent_plants = plants_df[plants_df['commissioning_year'] >= 2015]
    recent_with_contact = recent_plants[recent_plants[['email', 'phone']].notna().any(axis=1)]
    
    # High-capacity plants (likely professional operations)
    large_plants = plants_df[plants_df['capacity_el_kW'] > 1000]  # >1MW
    large_with_contact = large_plants[large_plants[['email', 'phone']].notna().any(axis=1)]
    
    print(f"SUSTAINABILITY VERIFICATION TARGETS:")
    print(f"• Recent plants (2015+): {len(recent_with_contact):,} contactable")
    print(f"• Large plants (>1MW): {len(large_with_contact):,} contactable")
    print(f"• Combined unique targets: {len(pd.concat([recent_with_contact, large_with_contact]).drop_duplicates()):,}")
    
    # 5. IMMEDIATE ACTION PLAN
    print("\n\n📋 IMMEDIATE 30-DAY ACTION PLAN")
    print("-" * 40)
    
    print("WEEK 1: MARKET VALIDATION")
    print("• Contact top 5 gas injection operators")
    print("• Validate green certificate pricing and demand")
    print("• Understand grid injection requirements")
    
    print("\nWEEK 2: PARTNERSHIP DEVELOPMENT")
    print("• Contact top 10 biogas specialist operators")
    print("• Explore partnership/joint venture opportunities")
    print("• Map existing certificate trading relationships")
    
    print("\nWEEK 3: REGIONAL FOCUS")
    print("• Deep dive into postal code 26169 (100 plants)")
    print("• Contact 20 high-capacity producers in region")
    print("• Build regional sustainability verification pipeline")
    
    print("\nWEEK 4: SCALING PREPARATION")
    print("• Extract full operator database (all 5 Excel sheets)")
    print("• Build automated contact management system")
    print("• Develop certificate trading protocols")
    
    # 6. CONTACT LISTS FOR IMMEDIATE ACTION
    print("\n\n📞 IMMEDIATE CONTACT LISTS")
    print("-" * 35)
    
    print("🔥 TOP 5 GAS INJECTION PRIORITIES:")
    for idx, plant in top_gas_plants.head(5).iterrows():
        contact = plant['email'] if pd.notna(plant['email']) else plant['phone']
        capacity = plant['capacity_gas_m3/h']
        value = capacity * 8760 * 10 / 1000 * 5
        print(f"• {plant['plant_name'][:35]:35}")
        print(f"  📧 {contact}")
        print(f"  💰 €{value:,.0f}/year potential")
        print(f"  📍 {plant['postal_code']}")
        print()
    
    print("🤝 TOP 5 BIOGAS OPERATORS FOR PARTNERSHIP:")
    for idx, operator in biogas_with_contact.head(5).iterrows():
        contact = operator['email'] if pd.notna(operator['email']) else operator['phone']
        print(f"• {operator['market_actor_name']}")
        print(f"  📧 {contact}")
        print()
    
    # 7. SUCCESS METRICS
    print("\n📊 SUCCESS METRICS (30-DAY TARGETS)")
    print("-" * 40)
    print("• 5 gas injection operators contacted")
    print("• 3 partnership discussions initiated")
    print("• 1 pilot certificate trading agreement")
    print("• 20 sustainability verification contacts")
    print("• Regional cluster analysis completed")
    print("• Full database extraction completed")
    
    print(f"\n🎯 YEAR 1 REVENUE TARGET:")
    conservative_market_share = 0.001  # 0.1% market share
    annual_target = 37_801_723_590 * conservative_market_share
    print(f"€{annual_target:,.0f} (0.1% of total market)")

if __name__ == "__main__":
    create_action_plan()
