"""Stream tables from the MaStR SOAP service into local CSVs."""
from __future__ import annotations
from pathlib import Path
import pandas as pd
from zeep import Client
from tqdm import tqdm
from config import SOAP_WSDL

class SOAPStreamer:
    """
    SOAPStreamer is a utility class for fetching and storing paginated data from a SOAP web service.

    Attributes:
        client: The SOAP client service interface.
        tmp (Path): Directory for storing temporary CSV files.
        page (int): Number of records to fetch per page.

    Methods:
        fetch_all() -> dict[str, Path]:
            Fetches all relevant datasets from the SOAP service and saves them as CSV files in the temporary directory.
            Returns a dictionary mapping dataset names to their corresponding file paths.

        _paged(fn, **kw):
            Internal generator that yields pandas DataFrames for each page of results from the given SOAP method.

        _dump(method: str, fname: str, **params) -> Path:
            Internal method that fetches all pages for a given SOAP method, concatenates the results, and saves them as a CSV file.
            Returns the path to the saved CSV file.
    """
    def __init__(self, tmp_dir: str = "mast_live"):
        self.client = Client(SOAP_WSDL).service
        self.tmp    = Path(tmp_dir)
        self.tmp.mkdir(exist_ok=True)
        self.page   = 1000

    # public wrappers -----------------------------------------------------
    def fetch_all(self) -> dict[str, Path]:
        """
        Fetches and dumps multiple datasets from the SOAP API to CSV files.

        Returns:
            dict[str, Path]: A dictionary mapping dataset names to the file paths of the corresponding dumped CSV files. The keys are:
                - "market_actors": Market actors data.
                - "biomass": Biomass units data.
                - "gas_producer": Gas generator units data.
                - "locations": Location data for power generation.
        """
        return {
            "market_actors": self._dump("GetGefilterteListeMarktakteure", "market_actors.csv"),
            "biomass":       self._dump("GetEinheitBiomasse",       "biomass.csv"),
            "gas_producer":  self._dump("GetEinheitGasErzeuger",  "gas_producer.csv"),
            "locations":     self._dump("GetLokationStromErzeuger", "locations.csv"),
        }

    # internals -----------------------------------------------------------
    def _paged(self, fn, **kw):
        page = 0
        while True:
            chunk = fn(PageSize=self.page, PageNumber=page, **kw)
            if not chunk:
                break
            yield pd.DataFrame(chunk)
            if len(chunk) < self.page:
                break
            page += 1

    def _dump(self, method: str, fname: str, **params) -> Path:
        path = self.tmp / fname
        if path.exists():
            return path
        print(f"⇣  SOAP → {fname}")
        fn = getattr(self.client, method)
        parts = []
        for df in tqdm(self._paged(fn, **params), desc=f"{method}"):
            parts.append(df)
        pd.concat(parts).to_csv(path, index=False)
        return path
