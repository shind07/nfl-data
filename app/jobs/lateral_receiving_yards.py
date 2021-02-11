import logging

import pandas as pd

from app.config import (
    configure_logging,
    CURRENT_YEAR,
)
from app.db import get_db_eng, load


REMOTE_PATH_TEMPLATE = 'https://raw.githubusercontent.com/mrcaseb/nfl-data/master/data/lateral_rec_yards/lateral_receiving_yards_{year}.csv'
OUTPUT_TABLE_NAME = 'lateral_receiving_yards'


def _extract(path: str) -> pd.DataFrame:
    """Download CSV from remote path."""
    logging.info(f"Downloading lateral receiving yards CSV from {path}...")
    return pd.read_csv(
        path,
        compression='gzip' if "csv.gz" in path else "infer",
        low_memory=False
    )


def run(year: int = CURRENT_YEAR) -> None:
    logging.info("Getting lateral receiving yards data...")
    remote_path = REMOTE_PATH_TEMPLATE.format(year=year)
    df = _extract(remote_path)
    df['year'] = year

    with get_db_eng().connect() as db_conn:
        load(db_conn, df, OUTPUT_TABLE_NAME, overwrite=False)


if __name__ == "__main__":
    configure_logging()
    run()
