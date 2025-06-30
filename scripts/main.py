# ──────────────────────────────────────────────────────────────────────────────
# FILE: biogas_db/main.py
# -----------------------------------------------------------------------------
import argparse
import sys
import os
from pathlib import Path

# Add src directory to Python path for imports (go up one level to project root)
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'src'))

from src.core.mastr_fetcher import MaStrFetcher
from src.core.contact_builder import ContactBuilder
from src.core.plant_builder import PlantBuilder

# Define output paths for organized structure
DATA_DIR = Path("data/processed")
OUT_CONTACTS_XLSX = DATA_DIR / "german_biogas_operator_contacts.xlsx"
OUT_PLANTS_CSV = DATA_DIR / "german_biogas_plants_2025.csv"
OUT_JOIN_CSV = DATA_DIR / "german_biogas_plants_with_contacts.csv"

def build_database(scrape: bool, selenium: bool):
    """
    Builds the biogas database by fetching live data, processing it, and saving the results.

    Args:
        scrape (bool): Whether to perform web scraping for additional contact data.
        selenium (bool): Whether to use Selenium for scraping dynamic web content.

    Process:
        1. Fetches data from the MaStR SOAP service.
        2. Builds contact and plant datasets from the fetched data.
        3. Merges plant and contact data on operator/market actor IDs.
        4. Saves the merged and individual datasets to CSV/XLSX files.
        5. Prints the paths to the generated files.

    Returns:
        None
    """
    print("◆ Fetching live data from MaStR using open-mastr library …")
    fetcher = MaStrFetcher()
    paths = fetcher.fetch_all(method="bulk")  # Use bulk download (no auth needed)

    contacts = ContactBuilder(paths["market_actors"], scrape=scrape, selenium=selenium).build()
    plants   = PlantBuilder(paths["biomass"], paths["gas_producer"], paths["locations"]).build()

    merged = plants.merge(contacts, left_on="operator_id", right_on="market_actor_id", how="left")
    merged.to_csv(OUT_JOIN_CSV, index=False)
    print("\n✓ Done – files ready:")
    for p in (OUT_CONTACTS_XLSX, OUT_PLANTS_CSV, OUT_JOIN_CSV):
        print("   ", p.resolve())

# CLI -------------------------------------------------------------------
if __name__ == "__main__":
    ap = argparse.ArgumentParser(description="Build German biogas DB from live MaStR")
    ap.add_argument("--scrape", action="store_true", help="crawl websites for missing email/phone")
    ap.add_argument("--selenium", action="store_true", help="enable JS fallback via headless Chrome")
    args = ap.parse_args()
    build_database(scrape=args.scrape, selenium=args.selenium)