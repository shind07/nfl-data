"""
Creates a table with one row per rushing type (designed, scramble, qb_kneel, total) per team, per year.

Upstream jobs: rushing_by_player_by_game
"""
import logging

import pandas as pd

from app.config import (
    configure_logging,
)
from app.db import get_db_eng, load

OUTPUT_TABLE_NAME = "rushing_by_team_by_year"


def _extract(db_conn) -> pd.DataFrame:
    """Getting the raw rushing_by_player_by_game stats."""
    logging.info("Extracting rushing stats by player by year from play by play...")
    query = """SELECT * FROM rushing_by_team_by_game"""
    df = pd.read_sql(query, db_conn)
    logging.info(f"Extracted {len(df)} rows of rushing by team by year stats.")
    return df


def _transform(df: pd.DataFrame) -> pd.DataFrame:
    """Aggregate the per player stats to get the per team stats."""
    logging.info("Aggregating the per game stats to the year level...")

    grouping_cols = ['year', 'season_type', 'team']
    return df.groupby(grouping_cols, as_index=False).sum()


def run() -> None:
    logging.info(f"Running job for {OUTPUT_TABLE_NAME}...")
    with get_db_eng().connect() as db_conn:
        df = _extract(db_conn)
        load(db_conn, df, OUTPUT_TABLE_NAME)
        logging.info(f"Job for {OUTPUT_TABLE_NAME} complete.")


if __name__ == "__main__":
    configure_logging()
    run()
