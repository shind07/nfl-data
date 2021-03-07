"""
Upstream jobs: play_by_play_enriched

Need the 2 pt convert stats to compute fantasy points.
"""
import logging

import numpy as np
import pandas as pd

from app.config import (
    configure_logging,
    PLAYER_YEAR_GROUPING_COLUMNS
)
from app.db import get_db_eng, load


OUTPUT_TABLE_NAME = "two_point_conversions_by_player_by_year"


def _extract(db_conn) -> pd.DataFrame:
    """Get the two point conversion stats by game."""
    logging.info("Extracting two point conversions by game...")
    query = """SELECT * FROM two_point_conversions_by_player_by_game"""
    return pd.read_sql(query, db_conn)


def _transform(df: pd.DataFrame) -> pd.DataFrame:
    """Aggregate the per game stats to the year level."""
    return df.groupby(PLAYER_YEAR_GROUPING_COLUMNS, as_index=False).sum().drop("week", axis=1)


def run() -> None:
    logging.info(f"Running job for {OUTPUT_TABLE_NAME}...")
    with get_db_eng().connect() as db_conn:
        df = _extract(db_conn)
        df = _transform(df)
        load(db_conn, df, OUTPUT_TABLE_NAME)
        logging.info(f"Job for {OUTPUT_TABLE_NAME} complete.")


if __name__ == "__main__":
    configure_logging()
    run()
