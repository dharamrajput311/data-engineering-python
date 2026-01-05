import csv
from typing import List, Dict, Any
from pathlib import Path
from etl.base_extractor import BaseExtractor


class CSVExtractor(BaseExtractor):
    """
    Extracts data from a CSV file.
    """

    def __init__(self, file_path: Path):
        self.file_path = file_path

    def extract(self) -> List[Dict[str, Any]]:
        with open(self.file_path, mode="r", newline="") as f:
            return list(csv.DictReader(f))
     
