import psycopg2
from typing import List, Dict, Any, Optional
from etl.base_loader import BaseLoader


class PostgresLoader(BaseLoader):
    """
    Loads records into a PostgreSQL table.
    """

    def __init__(self, connection_string: str, table_name: str):
        self.connection_string = connection_string
        self.table_name = table_name


    def load(self, records: List[Dict[str, Any]], context: Optional[Dict] = None):
        """
        Insert records into PostgreSQL table.

        Args:
            records (List[Dict]): Valid records
            context (Dict | None): Not used
        """
        if not records:
            return
        
        columns = records[0].keys()

        # PostgreSQL use %s as placeholders for parameters
        placeholders =  ",".join(["%s"] * len(columns))
        col_names = ",".join(columns)

        inser_query = f"INSERT INTO {self.table_name}({col_names}) VALUES ({placeholders})"

        #connect PostgreSQL using psycopg2
        conn = psycopg2.connect(self.connection_string)
        cursor = conn.cursor()

        try:
            for row in records:
                cursor.execute(inser_query, list(row.values()))
                conn.commit()
        except Exception as e :
            conn.rollback()
            raise e
        finally:
            cursor.close()
            conn.close        
                               
   



