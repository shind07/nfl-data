import logging
import os
import time

import pandas as pd

from app.config import DATA_DIRECTORY


def init_directory(path: str) -> None:
    """Create a directory if it doesn't exist."""
    if not os.path.exists(path):
        logging.info(f"Creating directory {path}...")
        os.mkdir(path)


def _atomic_rewrite(db_conn, df: pd.DataFrame, table_name: str) -> None:
    """Overwrite the table without dropping entirely.

    By default, pd.to_sql will drop the table and recreate its own schema,
    so by doing this we maintain the schema when overwriting a table.
    """
    db_conn.execute(f"""
        CREATE TABLE {table_name}_temp AS TABLE {table_name};
        TRUNCATE TABLE {table_name};
    """)
    df.to_sql(table_name, db_conn, index=False, if_exists='append')
    db_conn.execute(f"""DROP TABLE {table_name}_temp;""")


def load(db_conn, df: pd.DataFrame, table_name: str, overwrite: bool = True, backup: bool = False) -> None:
    """Write a pandas DataFrame to a database."""
    logging.info(f"Writing {len(df)} rows to {table_name}...")

    if overwrite:
        _atomic_rewrite(db_conn, df, table_name)
    else:
        df.to_sql(table_name, db_conn, index=False, if_exists='append')

    if backup:
        output_path = os.path.join(
            DATA_DIRECTORY,
            f"{table_name}_{time.strftime('%Y%m%d-%H%M%S')}.csv")
        logging.info(f"Saving a csv copy to {output_path}...")
        df.to_csv(output_path, index=False)

    logging.info(f"{table_name} successfully loaded to the database.")
