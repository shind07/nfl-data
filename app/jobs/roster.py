"""
Originally, this job used this link to download ALL rosters going back to 1999 at once:
REMOTE_PATH = 'https://github.com/mrcaseb/nflfastR-roster/blob/master/data/nflfastR-roster.csv.gz?raw=True'

Howver, this is the proper link as it has different fields.
Most important, the ID (GSIS_ID) is a different format than the previous roster and the
play by play, so we need to convert the play by play id to the same format for joins,
"""
import logging

import pandas as pd

from app.config import configure_logging
from app.db import get_db_eng

REMOTE_PATH = 'https://raw.githubusercontent.com/mrcaseb/nflfastR-roster/master/data/seasons/roster_2020.csv'
OUTPUT_TABLE_NAME = 'roster'


def _extract(path: str) -> pd.DataFrame:
    """Download CSV from remote path."""
    logging.info(f"Downloading remote roster CSV from {path}...")
    return pd.read_csv(
        path,
        compression='gzip' if "csv.gz" in path else "infer",
        low_memory=False
    )


def _load(db_conn, df: pd.DataFrame) -> None:
    """Write DF to database."""
    logging.info(f"Writing {len(df)} rows to {OUTPUT_TABLE_NAME}...")
    df.to_sql(OUTPUT_TABLE_NAME, db_conn, index=False, if_exists='replace')


def run() -> None:
    logging.info("Getting roster data...")
    df = _extract(REMOTE_PATH)

    with get_db_eng().connect() as db_conn:
        _load(db_conn, df)
        logging.info("Roster data loaded to db.")


if __name__ == "__main__":
    configure_logging()
    run()
