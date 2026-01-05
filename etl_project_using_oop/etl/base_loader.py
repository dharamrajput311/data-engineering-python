from abc import ABC, abstractmethod
from typing import List, Dict, Any, Optional

class BaseLoader(ABC):
    """
    Abstract base class for data loading.
    Define the contract for data loading.
    """

    @abstractmethod
    def load(self, records: List[Dict[str, Any]], context: Optional[Dict] = None) -> None:
        """
        Load transformed records to target.

        Args:
            records (List[Dict]): Cleaned data
            context (Dict | None): Optional metadata(e.g., source file name)
        """
        pass
