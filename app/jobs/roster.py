"""
Originally, this job used this link to download ALL rosters going back to 1999 at once:
REMOTE_PATH = 'https://github.com/mrcaseb/nflfastR-roster/blob/master/data/nflfastR-roster.csv.gz?raw=True'

Howver, this is the proper link as it has different fields.
Most important, the ID (GSIS_ID) is a different format than the previous roster and the
play by play, so we need to convert the play by play id to the same format for joins,
"""
import logging

import pandas as pd

from app.config import (
    configure_logging,
    CURRENT_YEAR,
)
from app.db import get_db_eng
from app.utils import load

REMOTE_PATH = 'https://raw.githubusercontent.com/mrcaseb/nflfastR-roster/master/data/seasons/roster_{year}.csv'
OUTPUT_TABLE_NAME = 'roster'


def _extract(path: str) -> pd.DataFrame:
    """Download CSV from remote path."""
    logging.info(f"Downloading remote roster CSV from {path}...")
    return pd.read_csv(
        path,
        compression='gzip' if "csv.gz" in path else "infer",
        low_memory=False
    )


def run(year: int = CURRENT_YEAR) -> None:
    logging.info("Getting roster data...")
    df = _extract(REMOTE_PATH.format(year=year))

    with get_db_eng().connect() as db_conn:
        load(db_conn, df, OUTPUT_TABLE_NAME, overwrite=False)
        logging.info("Roster data loaded to db.")


if __name__ == "__main__":
    configure_logging()
    run()
