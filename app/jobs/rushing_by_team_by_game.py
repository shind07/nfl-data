"""
Creates a table with one row per rushing type (designed, scramble, qb_kneel, total) per team, per game.

Upstream jobs: rushing_by_player_by_game
"""
import logging

import pandas as pd

from app.config import (
    configure_logging,
)
from app.db import get_db_eng, load

OUTPUT_TABLE_NAME = "rushing_by_team_by_game"


def _extract(db_conn) -> pd.DataFrame:
    """Getting rushing stats, per team per game, from rushing_by_player_by_game."""
    logging.info("Extracting rushing stats by team by game from play by play...")
    query = """
        SELECT
            year,
            season_type,
            game_id,
            team,
            opp,
            week,
            rush_type,
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
            game_id,
            week,
            team,
            opp,
            rush_type
        ORDER BY
            SUM(yards) DESC
    """
    df = pd.read_sql(query, db_conn)
    logging.info(f"Extracted {len(df)} rows of rushing stats.")
    return df


def run() -> None:
    logging.info(f"Running job for {OUTPUT_TABLE_NAME}...")
    with get_db_eng().connect() as db_conn:
        df = _extract(db_conn)
        load(db_conn, df, OUTPUT_TABLE_NAME)
        logging.info(f"Job for {OUTPUT_TABLE_NAME} complete.")


if __name__ == "__main__":
    configure_logging()
    run()
