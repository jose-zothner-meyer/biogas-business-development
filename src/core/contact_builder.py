# ──────────────────────────────────────────────────────────────────────────────
# FILE: biogas_db/contact_builder.py
# -----------------------------------------------------------------------------
from __future__ import annotations
from pathlib import Path
from typing import Tuple, Optional, Dict
import time
import pandas as pd
from tqdm import tqdm
import requests
from bs4 import BeautifulSoup
import re
from pathlib import Path

# Configuration constants
EMAIL_RE = re.compile(r"[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,}")
PHONE_RE = re.compile(r"(?:\+?\d[\d\s()/.-]{6,})")
CHUNKSIZE = 250_000
MAXROWS_XLSX = 1_000_000
OUT_CONTACTS_XLSX = Path("data/processed/german_biogas_operator_contacts.xlsx")

class ContactBuilder:
    """Create one-row-per-operator XLSX; optional HTTP + Selenium enrichment."""

    def __init__(self, market_csv: Path, *, scrape: bool = False, selenium: bool = False):
        self.market_csv = market_csv
        self.scrape, self.selenium = scrape, selenium

    def build(self) -> pd.DataFrame:
        """
        Builds and processes the contact DataFrame.

        This method performs the following steps:
        1. Deduplicates the contact data.
        2. Optionally scrapes missing information if the `scrape` attribute is True.
        3. Saves the resulting DataFrame to an Excel file.
        4. Returns the DataFrame with the 'website' column removed, if present.

        Returns:
            pd.DataFrame: The processed contact DataFrame without the 'website' column.
        """
        df = self._deduplicate()
        if self.scrape:
            df = self._scrape_missing(df)
        self._save_xlsx(df)
        return df.drop(columns="website", errors="ignore")

    # ------------------------------------------------------------------
    def _deduplicate(self) -> pd.DataFrame:
        # Map MaStR column names to our expected column names
        column_mapping = {
            "MastrNummer": "market_actor_id",
            "Firmenname": "market_actor_name", 
            "Email": "email",
            "Telefon": "phone",
            "Webseite": "website"
        }
        
        keep = ["market_actor_id", "market_actor_name", "email", "phone", "website"]
        store: Dict[str, pd.Series] = {}
        
        try:
            for chunk in pd.read_csv(self.market_csv, dtype=str, chunksize=CHUNKSIZE):
                # Check if file is empty (only headers or completely empty)
                if chunk.empty:
                    continue
                    
                # Rename columns from MaStR format to our expected format
                chunk = chunk.rename(columns=column_mapping)
                
                # Check if we have the required columns after mapping
                available_cols = [col for col in keep if col in chunk.columns]
                if not available_cols:
                    print(f"⚠️  No required columns found in {self.market_csv}")
                    print(f"   Available columns: {list(chunk.columns)}")
                    continue
                    
                # Use only available columns
                chunk = chunk[available_cols].copy()
                
                # Fill missing columns with empty strings
                for col in keep:
                    if col not in chunk.columns:
                        chunk[col] = ""
                
                for _, row in chunk.iterrows():
                    if not (row.get("email") or row.get("phone") or str(row.get("website", ""))):
                        continue
                    store.setdefault(row.get("market_actor_id", ""), row)
                    
        except pd.errors.EmptyDataError:
            print(f"⚠️  Empty CSV file: {self.market_csv}")
        except Exception as e:
            print(f"⚠️  Error reading {self.market_csv}: {e}")
            
        # Return empty DataFrame with correct structure if no data found
        if not store:
            print(f"⚠️  No market actor data found, creating empty DataFrame")
            return pd.DataFrame(columns=keep)
            
        return pd.DataFrame.from_dict(store, orient="index")

    # ------------------------------------------------------------------
    def _scrape_missing(self, df: pd.DataFrame) -> pd.DataFrame:
        hdrs = {"User-Agent": "ContactCrawler/1.0 (+https://example.tld)"}
        mask = (df.email.isna() | (df.email == "")) & (df.phone.isna() | (df.phone == "")) & df.website.notna()
        subset = df[mask].copy()

        # optional Selenium
        driver = None
        if self.selenium:
            try:
                from selenium import webdriver
                from selenium.webdriver.chrome.options import Options
                opts = Options(); opts.add_argument("--headless=new")
                driver = webdriver.Chrome(options=opts)
            except Exception:
                print("!  Selenium not available – continuing without JS fallback")
                driver = None

        for idx, row in tqdm(subset.iterrows(), total=len(subset), desc="scraping"):
            base = str(row.website).rstrip("/")
            if not base.startswith("http"):
                base = "https://" + base
            if self._try_plain(df, idx, base, hdrs):
                continue
            if driver:
                self._try_selenium(df, idx, driver, base)
            time.sleep(1.5)

        if driver:
            driver.quit()
        return df

    # ---------------- helpers ----------------
    def _try_plain(self, df, idx, base, hdrs) -> bool:
        for suf in ("", "/impressum", "/kontakt"):
            try:
                r = requests.get(base + suf, headers=hdrs, timeout=10)
                if r.ok and "text/html" in r.headers.get("content-type", ""):
                    email, phone = self._extract(r.text)
                    if email or phone:
                        if email: df.at[idx, "email"] = email
                        if phone: df.at[idx, "phone"] = phone
                        return True
            except requests.RequestException:
                pass
        return False

    def _try_selenium(self, df, idx, driver, base):
        try:
            driver.get(base); time.sleep(2)
            email, phone = self._extract(driver.page_source)
            if email:
                df.at[idx, "email"] = email
            if phone:
                df.at[idx, "phone"] = phone
        except Exception:
            pass

    @staticmethod
    def _extract(html: str) -> Tuple[Optional[str], Optional[str]]:
        email = next(iter(EMAIL_RE.findall(html)), None)
        phone = next(iter(PHONE_RE.findall(html)), None)
        return email, phone

    # ------------------------------------------------------------------
    def _save_xlsx(self, df: pd.DataFrame):
        print("⟳  writing contacts workbook …")
        with pd.ExcelWriter(OUT_CONTACTS_XLSX, engine="xlsxwriter") as xw:
            for i in range(0, len(df), MAXROWS_XLSX):
                df.iloc[i:i+MAXROWS_XLSX].to_excel(xw, sheet_name=f"contacts_{i//MAXROWS_XLSX+1}", index=False)

        print(f"✓  contacts workbook saved to {OUT_CONTACTS_XLSX}")
        if len(df) > MAXROWS_XLSX:
            print(f"⚠️  {len(df)} rows, split into multiple sheets (max {MAXROWS_XLSX} rows per sheet).")
        else:
            print(f"✓  {len(df)} rows, single sheet created.")
        print("✓  contact building complete.")
        print(f"✓  contact data saved to {OUT_CONTACTS_XLSX.resolve()}")
        return OUT_CONTACTS_XLSX
    # -----------------------------------------------------------------------------