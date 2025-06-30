# FILE: biogas_db/config.py
# -----------------------------------------------------------------------------
from pathlib import Path
import re

EMAIL_RE  = re.compile(r"[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,}")
PHONE_RE  = re.compile(r"(?:\+?\d[\d\s()/.-]{6,})")
CHUNKSIZE = 250_000
MAXROWS_XLSX = 1_000_000

SOAP_WSDL = "https://www.marktstammdatenregister.de/MaStRAPI/wsdl/mastr.wsdl"

# output files (created in cwd)
OUT_CONTACTS_XLSX = Path("german_biogas_operator_contacts.xlsx")
OUT_PLANTS_CSV    = Path("german_biogas_plants_2025.csv")
OUT_JOIN_CSV      = Path("german_biogas_plants_with_contacts.csv")

