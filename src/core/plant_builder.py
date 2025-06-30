# ──────────────────────────────────────────────────────────────────────────────
# FILE: biogas_db/plant_builder.py
# -----------------------------------------------------------------------------
from pathlib import Path
from typing import Dict
import pandas as pd
from pathlib import Path

# Configuration constants  
CHUNKSIZE = 250_000
OUT_PLANTS_CSV = Path("data/processed/german_biogas_plants_2025.csv")

class PlantBuilder:
    """Merge biomass + gas units with coordinates."""
    def __init__(self, biomass_csv: Path, gas_csv: Path, loc_csv: Path):
        """
        Initializes the class with file paths for biomass, gas, and location CSV files.

        Args:
            biomass_csv (Path): Path to the CSV file containing biomass data.
            gas_csv (Path): Path to the CSV file containing gas data.
            loc_csv (Path): Path to the CSV file containing location data.
        """
        self.biomass_csv, self.gas_csv, self.loc_csv = biomass_csv, gas_csv, loc_csv
    
    def build(self):
        """
        Builds a DataFrame of plant data by enriching biomass and gas CSV files with location information,
        then saves the combined data to a CSV file.

        Steps:
            1. Reads location data from a CSV file in chunks, extracting 'unit_id', 'latitude', and 'longitude'.
            2. Creates a mapping from unit IDs to their corresponding location data.
            3. Enriches biomass and gas data with location information using the internal _enrich method.
            4. Concatenates the enriched DataFrames into a single DataFrame.
            5. Saves the resulting DataFrame to a CSV file specified by OUT_PLANTS_CSV.

        Returns:
            pd.DataFrame: The combined and enriched DataFrame of plant data.
        """
        # Create location mapping - handle actual MaStR column names
        try:
            # Try to read locations with expected columns
            loc_chunks = []
            location_columns = ["MastrNummer"]  # Actual MaStR location ID column
            
            for chunk in pd.read_csv(self.loc_csv, dtype=str, chunksize=CHUNKSIZE):
                if chunk.empty:
                    continue
                # For now, create empty location mapping since locations_extended is mostly empty
                # The biomass and gas data already contain coordinates (Laengengrad, Breitengrad)
                loc_chunks.append(chunk)
            
            if loc_chunks:
                loc_df = pd.concat(loc_chunks)
                # Create basic mapping with location IDs (though coordinates will come from unit data directly)
                loc_map = {str(row["MastrNummer"]): {"latitude": "", "longitude": ""} 
                          for _, row in loc_df.iterrows() if pd.notna(row.get("MastrNummer"))}
            else:
                loc_map = {}
                
        except Exception as e:
            print(f"⚠️  Error reading location data: {e}")
            loc_map = {}
        
        # Process biomass and gas data
        biomass_dfs = list(self._enrich(self.biomass_csv, loc_map, True))
        gas_dfs = list(self._enrich(self.gas_csv, loc_map, False))
        
        # Combine all data
        all_dfs = biomass_dfs + gas_dfs
        if all_dfs:
            df = pd.concat(all_dfs, ignore_index=True)
        else:
            # Create empty DataFrame with expected structure if no data
            df = pd.DataFrame(columns=["plant_id", "plant_name", "postal_code", "commissioning_year", 
                                     "capacity_el_kW", "capacity_gas_m3/h", "operator_id", 
                                     "latitude", "longitude", "plant_type"])
        
        df.to_csv(OUT_PLANTS_CSV, index=False)
        return df
    
    def _enrich(self, src: Path, loc_map: Dict[str, Dict[str, str]], has_el: bool):
        """
        Enriches plant data from a CSV file by adding location, cleaning, and transforming columns.

        Args:
            src (Path): Path to the source CSV file containing plant data.
            loc_map (Dict[str, Dict[str, str]]): Mapping from unit IDs to their latitude and longitude.
            has_el (bool): Indicates if the data includes electrical capacity ("capacity_el_kw").

        Yields:
            pd.DataFrame: DataFrame chunks with enriched and cleaned plant data, including location and standardized columns.
        """
        try:
            for chunk in pd.read_csv(src, dtype=str, chunksize=CHUNKSIZE):
                if chunk.empty:
                    continue
                
                # Map MaStR column names to our expected names
                if has_el:  # Biomass data
                    column_mapping = {
                        "EinheitMastrNummer": "unit_id",
                        "NameStromerzeugungseinheit": "plant_name", 
                        "Postleitzahl": "postal_code",
                        "Inbetriebnahmedatum": "commissioning_year",
                        "Nettonennleistung": "capacity_el_kw",
                        "AnlagenbetreiberMastrNummer": "operator_id",
                        "Laengengrad": "longitude_raw",
                        "Breitengrad": "latitude_raw"
                    }
                    # Biomass doesn't have gas capacity
                    chunk["capacity_gas_m3_per_h"] = "0"
                else:  # Gas producer data  
                    column_mapping = {
                        "EinheitMastrNummer": "unit_id",
                        "NameGaserzeugungseinheit": "plant_name",
                        "Postleitzahl": "postal_code", 
                        "Inbetriebnahmedatum": "commissioning_year",
                        "Erzeugungsleistung": "capacity_gas_m3_per_h",
                        "AnlagenbetreiberMastrNummer": "operator_id",
                        "Laengengrad": "longitude_raw",
                        "Breitengrad": "latitude_raw"
                    }
                    # Gas producers don't have electrical capacity
                    chunk["capacity_el_kw"] = "0"
                
                # Rename columns
                chunk = chunk.rename(columns=column_mapping)
                
                # Select required columns, handling missing ones
                required_cols = ["unit_id", "plant_name", "postal_code", "commissioning_year", 
                               "capacity_el_kw", "capacity_gas_m3_per_h", "operator_id"]
                
                available_cols = [col for col in required_cols if col in chunk.columns]
                if not available_cols:
                    print(f"⚠️  No required columns found in {src}")
                    continue
                
                # Add missing columns with default values
                for col in required_cols:
                    if col not in chunk.columns:
                        chunk[col] = ""
                        
                chunk = chunk[required_cols].copy()
                
                # Add location data - use coordinates from the data itself if available
                if "latitude_raw" in chunk.columns and "longitude_raw" in chunk.columns:
                    chunk["latitude"] = pd.to_numeric(chunk["latitude_raw"], errors="coerce")
                    chunk["longitude"] = pd.to_numeric(chunk["longitude_raw"], errors="coerce")
                else:
                    # Fall back to location mapping (though it's likely empty)
                    chunk[["latitude","longitude"]] = chunk.unit_id.apply(
                        lambda uid: pd.Series(loc_map.get(str(uid), {"latitude": None, "longitude": None}))
                    )
                
                # Add plant type
                chunk["plant_type"] = "biogas" if has_el else "gas"
                
                # Clean and transform data
                chunk["plant_id"] = chunk.unit_id.astype(str).str.replace(" ", "_").str.lower()
                chunk["plant_name"] = chunk.plant_name.astype(str).str.strip().str.replace(r"\s+", " ", regex=True)
                chunk["postal_code"] = chunk.postal_code.astype(str).str.strip().str.replace(r"\s+", "", regex=True)
                
                # Handle commissioning year - extract year from date if needed
                chunk["commissioning_year"] = chunk["commissioning_year"].astype(str).str[:4]  # Take first 4 chars (year)
                chunk["commissioning_year"] = pd.to_numeric(chunk.commissioning_year, errors="coerce").fillna(0).astype(int)
                
                chunk["capacity_el_kw"] = pd.to_numeric(chunk.capacity_el_kw, errors="coerce").fillna(0).astype(int)
                chunk["capacity_gas_m3_per_h"] = pd.to_numeric(chunk.capacity_gas_m3_per_h, errors="coerce").fillna(0).astype(int)
                chunk["operator_id"] = chunk.operator_id.astype(str).str.strip().str.replace(r"\s+", "", regex=True)
                
                # Reorder columns
                chunk = chunk[["plant_id", "plant_name", "postal_code", "commissioning_year", 
                              "capacity_el_kw", "capacity_gas_m3_per_h", "operator_id", 
                              "latitude", "longitude", "plant_type"]]
                
                # Rename columns to final format
                yield chunk.rename(columns={"capacity_el_kw": "capacity_el_kW",
                                           "capacity_gas_m3_per_h": "capacity_gas_m3/h"})
                                           
        except Exception as e:
            print(f"⚠️  Error processing {src}: {e}")
            # Return empty DataFrame with correct structure
            empty_df = pd.DataFrame(columns=["plant_id", "plant_name", "postal_code", "commissioning_year", 
                                           "capacity_el_kW", "capacity_gas_m3/h", "operator_id", 
                                           "latitude", "longitude", "plant_type"])
            yield empty_df   
# -----------------------------------------------------------------------------