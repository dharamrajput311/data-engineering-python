import csv
from pathlib import Path
from datetime import datetime
from typing import List, Dict, Any, Optional
from etl.base_loader import BaseLoader

class BadCSVLoader(BaseLoader):
    """
    Loads invalid records into a CSV file.
    Appends data during a single pipeline run.
    Creates a new file for each pipeline execution. 
    """

    def __init__(self, folder_path: str, file_prefix: str, date_format: str) -> None:
        self.folder_path = Path(folder_path)
        self.folder_path.mkdir(exist_ok = True)

        timestamp = datetime.now().strftime(date_format)
        self.file_path = (
            self.folder_path / f"{file_prefix}_{timestamp}.csv"
        )

    def load(self, records: List[Dict[str, Any]], context: Optional[Dict] = None) -> None:
        """
        Append invalid records to CSV file.

        Args:
            records (List[Dict]): Invalid records
            context (Dict | None): Must contain 'source_file'
        """

        if not records:
            return
        
        source_file = context.get('source_file') if context else 'UNKNOWN'
        
        # Add source file name to each record
        for record in records:
            record['source_file_name'] = source_file

        file_exists = self.file_path.exists()

        with open(self.file_path, mode="a", newline="", encoding="utf-8") as f:
            writer = csv.DictWriter(f,fieldnames=records[0].keys())
            
            if not file_exists:
                writer.writeheader()
            
            writer.writerows(records)


