"""
Creates a table with one row per rushing type (designed, scramble, qb_kneel, total) per player, per year.

Upstream jobs: rushing_by_player_by_game
"""
import logging

import pandas as pd

from app.config import (
    configure_logging,
)
from app.db import get_db_eng

OUTPUT_TABLE_NAME = "rushing_by_player_by_year"


def _extract(db_conn) -> pd.DataFrame:
    """Getting designed rushing stats, per player per year, from the play by play."""
    logging.info("Extracting rushing stats by player by year from play by play...")
    query = """
        SELECT
            year,
            season_type,
            team,
            gsis_id,
            pos,
            rusher,
            rush_type,
            COUNT(DISTINCT game_id) as games,
            SUM(attempts) AS attempts,
            SUM(yards) AS yards,
            SUM(td) AS td,
            SUM(fumbles) AS fumbles,
            SUM(fumbles_lost) AS fumbles_lost,
            SUM(fumbles_out_of_bounds) AS fumbles_out_of_bounds,
            SUM(epa) AS epa
        FROM
            rushing_by_player_by_game
        GROUP BY
            year,
            season_type,
            team,
            gsis_id,
            pos,
            rusher,
            rush_type
        ORDER BY
            SUM(yards) DESC
    """
    df = pd.read_sql(query, db_conn)
    logging.info(f"Extracted {len(df)} rows of rushing stats.")
    return df


def _load(db_conn, df: pd.DataFrame) -> None:
    """Write DF to database."""
    logging.info(f"Writing {len(df)} rows to {OUTPUT_TABLE_NAME}...")
    df.to_sql(OUTPUT_TABLE_NAME, db_conn, index=False, if_exists='replace')


def run() -> None:
    logging.info(f"Running job for {OUTPUT_TABLE_NAME}...")
    with get_db_eng().connect() as db_conn:
        df = _extract(db_conn)
        _load(db_conn, df)
        logging.info(f"Job for {OUTPUT_TABLE_NAME} complete.")


if __name__ == "__main__":
    configure_logging()
    run()
