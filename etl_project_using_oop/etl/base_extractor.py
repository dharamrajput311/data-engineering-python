from abc import ABC,abstractmethod
from typing import List, Dict, Any
from pathlib import Path


class BaseExtractor(ABC):
    """
    Abstract base class of all extractors.
    Define the contract for data extraction.
    """

    def __init__(self, file_path: Path) -> None:
        self.file_path = file_path
        

    @abstractmethod
    def extract(self) -> List[Dict[str,Any]]:
        """
        Extract data from source.

        Returns:
            List of records as dictionaries.
        """
        pass
