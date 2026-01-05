import json
from typing import List, Dict, Any
from etl.base_extractor import BaseExtractor


class JSONExtractor(BaseExtractor):
    """
    Extracts data from a JSON file.
    """

    def __init__(self, file_path: str):
        self.file_path = file_path

    def extract(self) -> List[Dict[str, Any]]:
        with open(self.file_path, mode="r") as f:
            return json.load(f)
     
