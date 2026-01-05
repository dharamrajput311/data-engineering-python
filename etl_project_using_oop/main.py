"""
Entry point for the ETL pipeline.

Responsibilities:
- Read pipeline configuration from YAML
- Initialize extractor, transformer, and loaders
- Wire all components together
- Execute the ETL pipeline

Design Notes:
- No business logic is implemented here
- This file only orchestrates the pipeline
- Configuration is read once and passed explicitly to objects
"""

import yaml
from pathlib import Path
from datetime import datetime
import shutil
import os

from dotenv import load_dotenv

from etl.extractor_factory import get_extractor
from etl.generic_transformer import GenericTransformer
from etl.bad_csv_loader import BadCSVLoader
from etl.postgres_loader import PostgresLoader
from etl.pipeline import ETLPipeline


def load_config(config_path: str) -> dict:
    """
    Load YAML configuration file.

    Args:
        config_path (str): Path to config.yaml

    Returns:
        dict: Parsed configuration dictionary
    """  
    with open(config_path, mode="r") as file:  
            return yaml.safe_load(file)
    

        
def main() -> None:
    """
    Main function to run the ETL pipeline.

    Steps:
        1. Load configuration
        2. Load secure database connection string from environment variables
        3. Create extractor using factory method
        3. Execute ETL pipeline
        4. Load Invalid data into CSV
        5. Archive successfully processed files

    Args:
        None

    Returns:
        None
    """

    # ------------------------------------------------------------------
    # Load conenvironment variables
    # ------------------------------------------------------------------
    load_dotenv()

    postgres_connection_string = os.getenv("POSTGRES_CONN_STRING")
    if not postgres_connection_string:
         raise EnvironmentError(
              "POSTGRES_CONN_STRING is not set in environment variables"
         )

    # ------------------------------------------------------------------
    # Load configuration
    # ------------------------------------------------------------------
    config = load_config("config/config.yaml")

    pipeline_cfg = config["pipeline"]
    invalid_file_cfg = pipeline_cfg["invalid_file"]
    database_cfg = pipeline_cfg["database"]
    postgres_cfg = database_cfg["postgres"]

    input_folder = Path(pipeline_cfg["input_folder"])
    garbage_records_folder = pipeline_cfg["garbage_records_folder"]  

    archive_folder = Path(pipeline_cfg["archive_folder"])  
    archive_date_folder = archive_folder / datetime.now().strftime("%Y%m%d")
    archive_date_folder.mkdir(parents=True, exist_ok=True)    

    # ------------------------------------------------------------------
    # Validate input folder
    # ------------------------------------------------------------------
    if not input_folder.exists():
         raise FileNotFoundError(f"Input folder not found: {input_folder}")
    
    input_files = list(input_folder.iterdir())

    if not input_files:
         print("No input files found. Pipeline stopped.")
         return 

    # ------------------------------------------------------------------
    # Initialize Transformer
    # ------------------------------------------------------------------
    transformer = GenericTransformer()

    # ------------------------------------------------------------------
    # Initialize Loaders
    # ------------------------------------------------------------------ 

    # Valid records loader
    pg_loader = PostgresLoader(
         connection_string=postgres_connection_string, 
         table_name=postgres_cfg['table_name']
         )
    
    # Invalid records loader
    bad_csv_loader = BadCSVLoader(
         folder_path=garbage_records_folder,
         file_prefix=invalid_file_cfg['garbage_file_prefix'],
         date_format=invalid_file_cfg['date_format']
         )
    
    # ------------------------------------------------------------------
    # Process each file
    # ------------------------------------------------------------------ 
    for file_path in input_files:
        if not file_path.is_file():
             continue
        
        try:
             extractor = get_extractor(
                  file_path=file_path
             )

             pipeline = ETLPipeline(
                  extractor=extractor,
                  transformer=transformer,
                  valid_loader=pg_loader,
                  invalid_loader=bad_csv_loader
             )

             pipeline.run() 

             # ------------------------------------------------------------------
             # Archive processed files.
             # ------------------------------------------------------------------   
             shutil.move(
                  str(file_path),
                  str(archive_date_folder / file_path.name)
             )

        except ValueError as e :
             print(f"Skipping unsupported file {file_path.name}: e")


if __name__ == "__main__":
    main()
