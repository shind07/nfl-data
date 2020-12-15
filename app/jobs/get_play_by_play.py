"""
https://gist.github.com/Deryck97/dff8d33e9f841568201a2a0d5519ac5e
https://github.com/guga31bb/nflfastR-data/tree/master/data
"""
import logging

import pandas as pd

from app.config import (
    configure_logging,
    CURRENT_YEAR,
)
from app.db import get_db_conn

REMOTE_PATH_TEMPLATE = 'https://github.com/guga31bb/nflfastR-data/blob/master/data/play_by_play_{year}.csv.gz?raw=True'
OUTPUT_TABLE_NAME = 'play_by_play'


def _extract(path: str) -> pd.DataFrame:
    """Download CSV from remote path."""
    logging.info(f"Downloading remote play by play CSV from {path}...")
    return pd.read_csv(
        path,
        compression='gzip' if "csv.gz" in path else "infer",
        low_memory=False
    )


def _load(db_conn, df: pd.DataFrame) -> None:
    """Write DF to database."""
    logging.info(f"Writing {len(df)} rows to {OUTPUT_TABLE_NAME}...")
    df.to_sql(OUTPUT_TABLE_NAME, db_conn, index=False, if_exists='replace')


def run(year: str = CURRENT_YEAR) -> None:
    logging.info("Getting play by play data...")
    remote_path = REMOTE_PATH_TEMPLATE.format(year=year)
    df = _extract(remote_path)

    db_conn = get_db_conn()
    _load(db_conn, df)
    logging.info("Play by play data loaded to db.")


if __name__ == "__main__":
    configure_logging()
    run()
