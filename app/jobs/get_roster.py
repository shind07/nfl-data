"""
https://gist.github.com/Deryck97/dff8d33e9f841568201a2a0d5519ac5e
https://github.com/guga31bb/nflfastR-data/tree/master/data
"""
import logging
import os

import pandas as pd

from app.config import (
    configure_logging,
    ROSTER_DIRECTORY
)
from app.utils import (
    init_directory,
)

REMOTE_PATH = 'https://github.com/mrcaseb/nflfastR-roster/blob/master/data/nflfastR-roster.csv.gz?raw=True'


def _extract(path: str) -> pd.DataFrame:
    """Download CSV from remote path."""
    logging.info(f"Downloading remote roster CSV from {path}...")

    return pd.read_csv(
        path,
        compression='gzip' if "csv.gz" in path else "infer",
        low_memory=False
    )


def _load(df: pd.DataFrame, path: str) -> None:
    """Write DF to local CSV."""
    init_directory(ROSTER_DIRECTORY)
    logging.info(f"Writing roster CSV to {path}...")

    df.to_csv(path, index=False)


def run():
    logging.info("Getting roster data...")

    df = _extract(REMOTE_PATH)

    local_path = os.path.join(ROSTER_DIRECTORY, 'play_by_play.csv')
    _load(df, local_path)


if __name__ == "__main__":
    configure_logging()

    run()
