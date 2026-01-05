import os
from etl.csv_extractor import CSVExtractor
from etl.json_extractor import JSONExtractor
from etl.base_extractor import BaseExtractor


def get_extractor(file_path: str) -> BaseExtractor:
    """
    Factory function that returns an appropriate extractor based on the file extension. 

    Args:
        file_path (str): The path to the source file to be processed.

    Return: 
        BaseExtractor: An instance of CSVExtractor or JSONExtractor         
    """

    extension = os.path.splitext(file_path)[1].lower()

    if extension == ".csv":
        return CSVExtractor(file_path)
    elif extension == ".json":
        return JSONExtractor(file_path)
    else:
        raise ValueError(f"Unsupported file type: {extension}")