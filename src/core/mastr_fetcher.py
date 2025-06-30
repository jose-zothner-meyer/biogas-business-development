# ──────────────────────────────────────────────────────────────────────────────
# FILE: biogas_db/mastr_fetcher.py
# -----------------------------------------------------------------------------
"""Modern MaStR data fetcher using the open-mastr library."""
from __future__ import annotations
from pathlib import Path
from typing import Dict, Optional, List
import os
import shutil
import pandas as pd
from open_mastr import Mastr
from open_mastr.soap_api.download import MaStRAPI

# Configuration constants
CHUNKSIZE = 250_000

class MaStrFetcher:
    """
    Modern MaStR data fetcher using the open-mastr library.
    
    This replaces the old SOAP-based approach with a more robust solution
    that handles authentication, bulk downloads, and data processing automatically.
    
    Supports both bulk download (no authentication) and API download (requires token).
    """
    
    def __init__(self, tmp_dir: str = "mastr_live", user: Optional[str] = None, token: Optional[str] = None):
        """
        Initialize the MaStR fetcher.
        
        Args:
            tmp_dir (str): Directory for storing temporary CSV files.
            user (str, optional): MaStR user ID (e.g., SOM956207857483). If not provided, 
                                   will be read from credentials file.
            token (str, optional): MaStR API token. If not provided, will be read from 
                                    credentials file or keyring.
        """
        self.tmp = Path(tmp_dir)
        self.tmp.mkdir(exist_ok=True)
        self.user = user or os.environ.get('MASTR_USER')
        self.token = token or os.environ.get('MASTR_TOKEN')
        
        # Initialize the main Mastr interface
        self.db = Mastr()
        
        # Initialize API interface for authenticated calls (if credentials available)
        self.api = None
        if self.user and self.token:
            try:
                self.api = MaStRAPI(user=self.user, key=self.token)
                print(f"◆ Initialized with API credentials for user: {self.user}")
            except Exception as e:
                print(f"⚠️  Could not initialize API with provided credentials: {e}")
        elif self.user:
            try:
                # Try to initialize with just user (token from credentials file)
                self.api = MaStRAPI()
                print(f"◆ Initialized API using credentials file for user: {self.user}")
            except Exception as e:
                print(f"⚠️  Could not initialize API from credentials file: {e}")
                print("   Will use bulk download method instead.")
    
    def fetch_all(self, method: str = "bulk", export_csv: bool = True) -> Dict[str, Path]:
        """
        Fetch all relevant datasets from MaStR and export them as CSV files.
        
        Args:
            method (str): Download method - "bulk" (no auth needed) or "API" (needs credentials)
            export_csv (bool): Whether to export data to CSV files after download
            
        Returns:
            Dict[str, Path]: Dictionary mapping dataset names to CSV file paths.
        """
        print(f"◆ Downloading MaStR data using {method} method...")
        
        # Choose appropriate download method
        if method.lower() == "api" and not self.api:
            print("⚠️  API method requested but no valid credentials available. Falling back to bulk method.")
            method = "bulk"
        
        # Data types we're interested in for biogas analysis
        # Note: Only downloading plant data since:
        # - Coordinates are already included in plant data (Laengengrad, Breitengrad) 
        # - Market actors table is mostly empty and downloading 4.7M records takes too long
        if method.lower() == "bulk":
            # For bulk download - only essential plant data
            data_types = ["biomass", "gas"]
        else:
            # For API download - only essential plant data
            data_types = ["biomass", "gas"]
        
        # Download the data
        try:
            if method.lower() == "bulk":
                # Bulk download - downloads the entire dataset 
                self.db.download(method="bulk", data=data_types)
            else:
                # API download - requires authentication but allows incremental updates
                self.db.download(method="API", data=data_types)
                
            print("✅ Download completed successfully!")
            
        except Exception as e:
            print(f"❌ Error during download: {e}")
            raise
        
        if not export_csv:
            return {}
        
        print("◆ Exporting data to CSV files...")
        
        # Export the relevant tables to CSV
        # Only export plant data for efficiency
        tables_to_export = [
            "biomass",         # Biomass plants  
            "gas_producer"     # Gas plants
        ]
        
        try:
            # Export to the standard open-mastr directory structure
            self.db.to_csv(tables=tables_to_export)
            print("✅ CSV export completed!")
            
        except Exception as e:
            print(f"⚠️  Error during CSV export: {e}")
            print("   Attempting manual table export...")
            return self._manual_export()
        
        # Find the exported CSV files and copy them to our working directory
        return self._copy_exported_files(tables_to_export)
    
    def get_available_tables(self) -> list[str]:
        """
        Get list of available tables in the MaStR database.
        
        Returns:
            list[str]: List of table names.
        """
        query = "SELECT name FROM sqlite_master WHERE type='table';"
        df = pd.read_sql_query(query, con=self.db.engine)
        return df['name'].tolist()
    
    def export_table(self, table_name: str, output_path: Path) -> Path:
        """
        Export a specific table to CSV.
        
        Args:
            table_name (str): Name of the table to export.
            output_path (Path): Path where to save the CSV file.
            
        Returns:
            Path: Path to the exported CSV file.
        """
        print(f"⇣ Exporting {table_name} → {output_path.name}")
        
        # Read table in chunks to handle large datasets
        chunks = []
        for chunk in pd.read_sql(table_name, con=self.db.engine, chunksize=CHUNKSIZE):
            chunks.append(chunk)
        
        # Concatenate and save
        df = pd.concat(chunks, ignore_index=True)
        df.to_csv(output_path, index=False)
        
        return output_path
    
    def _copy_exported_files(self, tables_to_export: List[str]) -> Dict[str, Path]:
        """
        Copy exported CSV files from open-mastr directory to our working directory.
        
        Args:
            tables_to_export: List of table names that were exported
            
        Returns:
            Dictionary mapping dataset names to local file paths
        """
        # Find the most recent export directory
        export_dir = Path.home() / ".open-MaStR" / "data"
        dataversion_dirs = list(export_dir.glob("dataversion-*"))
        
        if not dataversion_dirs:
            print("⚠️  No dataversion directories found. Creating empty files.")
            return self._create_empty_files()
        
        latest_dir = max(dataversion_dirs, key=lambda p: p.stat().st_mtime)
        print(f"📁 Using export directory: {latest_dir}")
        
        # Map open-mastr table names to our expected filenames
        file_mapping = {
            "market_actors": "bnetza_mastr_market_actors_raw.csv",
            "biomass": "bnetza_mastr_biomass_raw.csv", 
            "gas_producer": "bnetza_mastr_gas_producer_raw.csv",
            "locations": "bnetza_mastr_locations_extended_raw.csv"
        }
        
        result_paths = {}
        for key, filename in file_mapping.items():
            source = latest_dir / filename
            dest = self.tmp / f"{key}.csv"  # Use simple names for destination
            
            if source.exists():
                shutil.copy2(source, dest)
                print(f"📄 Copied {filename} → {dest.name} ({source.stat().st_size:,} bytes)")
                result_paths[key] = dest
            else:
                print(f"⚠️  Warning: {filename} not found, creating empty file")
                dest.touch()
                result_paths[key] = dest
        
        return result_paths
    
    def _manual_export(self) -> Dict[str, Path]:
        """
        Manually export tables when the standard to_csv method fails.
        
        Returns:
            Dictionary mapping dataset names to local file paths
        """
        print("📊 Attempting manual table export...")
        
        # Get list of available tables
        available_tables = self.get_available_tables()
        print(f"📋 Available tables: {', '.join(available_tables)}")
        
        # Map table names to expected output files
        table_mapping = {
            "biomass": "biomass.csv",
            "biomass_extended": "biomass.csv",
            "biomass_eeg": "biomass.csv",
            "gas": "gas.csv",
            "gas_producer": "gas.csv",
            "market": "market.csv",
            "market_actors": "market.csv",
            "location": "location.csv",
            "locations_basic": "location.csv",
            "locations_extended": "location.csv",
            # Alternative table names that might exist
            "Biomasse": "biomass.csv",
            "Gas": "gas.csv",  
            "Marktakteure": "market.csv",
            "Lokationen": "location.csv"
        }
        
        result_paths = {}
        file_to_dataset = {
            "biomass.csv": "biomass",
            "gas.csv": "gas_producer", 
            "market.csv": "market_actors",
            "location.csv": "locations"
        }
        
        # Priority order for each dataset (prefer tables with more data)
        dataset_priorities = {
            "biomass": ["biomass_extended", "biomass_eeg", "biomass", "Biomasse"],
            "gas_producer": ["gas_producer", "gas", "Gas"],
            "market_actors": ["market_actors", "market", "Marktakteure"],
            "locations": ["locations_extended", "locations_basic", "location", "Lokationen"]
        }
        
        # Export each dataset using the first available table
        for dataset_key, table_priority in dataset_priorities.items():
            exported = False
            for table_name in table_priority:
                if table_name in available_tables:
                    filename = f"{dataset_key}.csv"
                    output_path = self.tmp / filename
                    
                    try:
                        self.export_table(table_name, output_path)
                        result_paths[dataset_key] = output_path
                        exported = True
                        break
                    except Exception as e:
                        print(f"⚠️  Failed to export {table_name}: {e}")
                        continue
            
            if not exported:
                # Create empty file as fallback
                filename = f"{dataset_key}.csv"
                output_path = self.tmp / filename
                output_path.touch()
                result_paths[dataset_key] = output_path
                print(f"📄 Created empty file: {filename}")
        
        # Legacy approach - keep for backwards compatibility
        for table_name in available_tables:
            if table_name in table_mapping:
                filename = table_mapping[table_name]
                dataset_key = file_to_dataset.get(filename)
                
                # Only export if we haven't already handled this dataset
                if dataset_key and dataset_key not in result_paths:
                    output_path = self.tmp / filename
                    
                    try:
                        self.export_table(table_name, output_path)
                        result_paths[dataset_key] = output_path
                    except Exception as e:
                        print(f"⚠️  Failed to export {table_name}: {e}")
         # Create empty files for any missing datasets with proper structure
        for dataset_key in ["market_actors", "biomass", "gas_producer", "locations"]:
            if dataset_key not in result_paths:
                if dataset_key == "market_actors":
                    # Create structured market_actors file
                    market_headers = ["MastrNummer", "Firmenname", "Email", "Telefon", "Webseite"]
                    output_path = self.tmp / f"{dataset_key}.csv"
                    pd.DataFrame(columns=market_headers).to_csv(output_path, index=False)
                    result_paths[dataset_key] = output_path
                    print(f"📄 Created empty {dataset_key}.csv with proper headers")
                elif dataset_key == "locations":
                    # Create structured locations file
                    location_headers = ["MastrNummer", "NameDerTechnischenLokation", "Laengengrad", "Breitengrad"]
                    output_path = self.tmp / f"{dataset_key}.csv"
                    pd.DataFrame(columns=location_headers).to_csv(output_path, index=False)
                    result_paths[dataset_key] = output_path
                    print(f"📄 Created empty {dataset_key}.csv with proper headers")
                else:
                    # Create basic empty file for plant data
                    output_path = self.tmp / f"{dataset_key}.csv"
                    output_path.touch()
                    result_paths[dataset_key] = output_path
                    print(f"📄 Created empty file: {dataset_key}.csv")

        return result_paths
    
    def _create_empty_files(self) -> Dict[str, Path]:
        """Create CSV files with proper headers when no data is available."""
        result_paths = {}
        
        # Create empty biomass and gas files (should not happen with valid download)
        for dataset_key in ["biomass", "gas_producer"]:
            dest = self.tmp / f"{dataset_key}.csv"
            dest.touch()
            result_paths[dataset_key] = dest
            print(f"📄 Created empty file: {dataset_key}.csv")
        
        # Create structured empty files for market_actors and locations
        # Market actors with proper MaStR structure
        market_headers = ["MastrNummer", "Firmenname", "Email", "Telefon", "Webseite", 
                         "Personenart", "Rechtsform", "Land", "Postleitzahl", "Ort"]
        market_path = self.tmp / "market_actors.csv"
        pd.DataFrame(columns=market_headers).to_csv(market_path, index=False)
        result_paths["market_actors"] = market_path
        print(f"📄 Created empty market_actors.csv with proper headers")
        
        # Locations with proper MaStR structure  
        location_headers = ["MastrNummer", "NameDerTechnischenLokation", "Laengengrad", "Breitengrad"]
        location_path = self.tmp / "locations.csv"
        pd.DataFrame(columns=location_headers).to_csv(location_path, index=False)
        result_paths["locations"] = location_path
        print(f"📄 Created empty locations.csv with proper headers")
        
        return result_paths
    
    def test_api_connection(self) -> bool:
        """
        Test if the API connection is working properly.
        
        Returns:
            bool: True if API is accessible, False otherwise
        """
        if not self.api:
            print("❌ No API credentials configured")
            return False
        
        try:
            # Test with a simple API call
            result = self.api.GetLokaleUhrzeit()
            if result and result.get('Ergebniscode') == 'OK':
                local_time = result.get('LokaleUhrzeit')
                print(f"✅ API connection successful! Server time: {local_time}")
                return True
            else:
                print(f"❌ API test failed: {result}")
                return False
        except Exception as e:
            print(f"❌ API connection error: {e}")
            return False
    
    def get_database_info(self) -> Dict:
        """
        Get information about the local MaStR database.
        
        Returns:
            Dictionary with database statistics and table information
        """
        info = {
            "database_path": str(self.db.engine.url),
            "tables": {},
            "total_size_mb": 0
        }
        
        try:
            # Get table information
            tables = self.get_available_tables()
            for table in tables:
                try:
                    count_query = f"SELECT COUNT(*) as count FROM `{table}`"
                    result = pd.read_sql_query(count_query, con=self.db.engine)
                    row_count = result['count'].iloc[0] if not result.empty else 0
                    info["tables"][table] = {"rows": row_count}
                except Exception as e:
                    info["tables"][table] = {"rows": "error", "error": str(e)}
            
            # Get database file size if it's SQLite
            db_path = Path.home() / ".open-MaStR" / "data" / "sqlite" / "open-mastr.db"
            if db_path.exists():
                info["total_size_mb"] = round(db_path.stat().st_size / (1024 * 1024), 2)
                
        except Exception as e:
            info["error"] = str(e)
        
        return info
    
    def print_database_summary(self):
        """Print a summary of the database contents."""
        info = self.get_database_info()
        
        print("\n" + "="*60)
        print("🗄️  MaStR DATABASE SUMMARY")
        print("="*60)
        print(f"📁 Database: {info.get('database_path', 'Unknown')}")
        print(f"💾 Size: {info.get('total_size_mb', 0)} MB")
        print(f"📊 Tables: {len(info.get('tables', {}))}")
        
        if info.get('tables'):
            print("\n📋 Table Details:")
            for table_name, table_info in info['tables'].items():
                if isinstance(table_info.get('rows'), int):
                    print(f"   • {table_name}: {table_info['rows']:,} rows")
                else:
                    print(f"   • {table_name}: {table_info.get('rows', 'unknown')}")
        
        if info.get('error'):
            print(f"\n⚠️  Warning: {info['error']}")
        
        print("="*60)
