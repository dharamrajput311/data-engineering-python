from typing import List, Dict, Any, Set, Tuple
from etl.base_transformer import BaseTransformer


class GenericTransformer(BaseTransformer):
    """
    Generic transformer to handle:
    - Missing values
    - Duplicates records
    - Splits records into valid and invalid datasets
    """

    def transform(self, records: List[Dict[str, Any]]
                  ) -> Tuple[List[Dict[str, Any]], List[Dict[str, Any]]]:
        
        valid_records: List[Dict[str, Any]] = []
        invalid_records: List[Dict[str, Any]] = []
        seen: Set[Tuple] = set()

        for row in records:
            # Remove records with missing or garbage values
            if any(value in ("", "NA", None, "NULL") for value in row.values()):
                invalid_records.append(row)
                continue

            # Remove duplicate records
            row_key = tuple(row.items())
            if row_key in seen:
                invalid_records.append(row)
                continue

            seen.add(row_key)
            valid_records.append(row)

        return valid_records, invalid_records    



