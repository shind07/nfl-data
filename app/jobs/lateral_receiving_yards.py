import logging

import pandas as pd

from app.config import (
    configure_logging,
    CURRENT_YEAR,
)
from app.db import get_db_eng

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


def _load(db_conn, df: pd.DataFrame) -> None:
    """Write DF to database."""
    logging.info(f"Writing {len(df)} rows to {OUTPUT_TABLE_NAME}...")
    df.to_sql(OUTPUT_TABLE_NAME, db_conn, index=False, if_exists='replace')


def run(year: int = CURRENT_YEAR) -> None:
    logging.info("Getting lateral receiving yards data...")
    remote_path = REMOTE_PATH_TEMPLATE.format(year=year)
    df = _extract(remote_path)
    df['year'] = year

    with get_db_eng().connect() as db_conn:
        _load(db_conn, df)
        logging.info("Lateral receiving yards data loaded to db.")


if __name__ == "__main__":
    configure_logging()
    run()
