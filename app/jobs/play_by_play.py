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
from app.db import get_db_eng, load


REMOTE_PATH_TEMPLATE = 'https://github.com/guga31bb/nflfastR-data/blob/master/data/play_by_play_{year}.csv.gz?raw=True'
OUTPUT_TABLE_NAME = 'play_by_play'


class NoNewGamesException(Exception):
    pass


def _extract(path: str) -> pd.DataFrame:
    """Download CSV from remote path."""
    logging.info(f"Downloading remote play by play CSV from {path}...")
    return pd.read_csv(
        path,
        compression='gzip' if "csv.gz" in path else "infer",
        low_memory=False
    )


def _transform(db_conn, df: pd.DataFrame) -> pd.DataFrame:
    """Filter out plays from the pbp data so we only upload new data"""
    logging.info("Checking for new games in the play by play data...")
    query = """SELECT DISTINCT game_id from play_by_play"""
    df_existing_games = pd.read_sql(query, db_conn)
    df_new_games = df[~df['game_id'].isin(df_existing_games['game_id'])]
    return df_new_games


def run(year: int = CURRENT_YEAR) -> None:
    logging.info("Getting play by play data...")
    df = _extract(REMOTE_PATH_TEMPLATE.format(year=year))
    df['year'] = year

    with get_db_eng().connect() as db_conn:
        df = _transform(db_conn, df)
        if len(df) == 0:
            raise NoNewGamesException("No new games found in play by play!")
        load(db_conn, df, OUTPUT_TABLE_NAME, overwrite=False)


if __name__ == "__main__":
    configure_logging()
    run()
