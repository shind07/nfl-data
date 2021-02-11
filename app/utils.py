import logging
import os
import time
import urllib.request

import pandas as pd

from app.config import (
    DATA_DIRECTORY,
    HEADSHOTS_DIRECTORY,
    TEAM_LOGOS_DIRECTORY,
    TEAM_LOGOS_REMOTE_PATH
)


def get_headshot_path(season: str, team: str, gsis_id: str) -> str:
    """Deterministically generate the local path to the headshot png."""
    return os.path.join(
        HEADSHOTS_DIRECTORY,
        f"{season}_{team}_{gsis_id}.png"
    )


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
        DROP TABLE IF EXISTS {table_name}_temp;
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


def download_image(remote_path: str, local_path: str) -> bool:
    """Download an image, return True if successful else False.
    
    Adding a sleep at the end to avoid spamming a remote path.
    """
    logging.info(f"Saving image for {row['full_name']}....")
    try:
        urllib.request.urlretrieve(remote_path, local_path)
        return True
    except Exception as e:
        logging.error(e)
        return False

    time.sleep(0.5)


def download_team_logos() -> None:
    df = pd.read_csv(TEAM_LOGOS_REMOTE_PATH)
