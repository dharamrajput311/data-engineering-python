from abc import ABC, abstractmethod
from typing import List, Dict, Any

class BaseTransformer(ABC):
    """
    Abstract base class for data transformation.
    Define the contract for data transformation.
    """

    @abstractmethod
    def transform(self, records: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """
        Transform input records.

        Args:
            records: Raw extracted data

        Returns:
            Cleaned and transformed data
        """
        pass
