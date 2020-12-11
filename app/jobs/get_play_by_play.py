"""
https://gist.github.com/Deryck97/dff8d33e9f841568201a2a0d5519ac5e
https://github.com/guga31bb/nflfastR-data/tree/master/data
"""
import logging
import os

import pandas as pd

from app.config import (
    configure_logging,
    CURRENT_YEAR,
    PLAY_BY_PLAY_DIRECTORY
)
from app.utils import (
    init_directory,
)

REMOTE_PATH_TEMPLATE = 'https://github.com/guga31bb/nflfastR-data/blob/master/data/play_by_play_{year}.csv.gz?raw=True'


def _extract(path: str) -> pd.DataFrame:
    """Download CSV from remote path."""
    logging.info(f"Downloading remote play by play CSV from {path}...")

    return pd.read_csv(
        path,
        compression='gzip' if "csv.gz" in path else "infer",
        low_memory=False
    )


def _load(df: pd.DataFrame, path: str) -> None:
    """Write DF to local CSV."""
    init_directory(PLAY_BY_PLAY_DIRECTORY)
    logging.info(f"Writing play by play CSV to {path}...")

    df.to_csv(path, index=False)


def run():
    logging.info("Getting play by play data...")

    remote_path = REMOTE_PATH_TEMPLATE.format(year=CURRENT_YEAR)
    df = _extract(remote_path)

    local_path = os.path.join(PLAY_BY_PLAY_DIRECTORY, 'play_by_play.csv')
    _load(df, local_path)


if __name__ == "__main__":
    configure_logging()

    run()
