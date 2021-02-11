"""
Upstream jobs: receiving_by_player_by_game

NOTE: We get two tables - receiving by position, and total receiving, and stack them
like we do with the rushing tables.
"""
import logging

import pandas as pd

from app.config import (
    configure_logging,
)
from app.db import get_db_eng, load


OUTPUT_TABLE_NAME = "receiving_by_team_by_game"


def _extract_by_position(db_conn) -> pd.DataFrame:
    """Getting passing stats, per team per game per position"""
    logging.info("Extracting receving stats by player by game from play by play...")
    query = """
        SELECT
            year,
            season_type,
            game_id,
            team,
            opp,
            week,
            pos,
            SUM(receptions) AS receptions,
            SUM(targets) AS targets,
            SUM(yards) AS yards,
            SUM(air_yards_intended) AS air_yards_intended,
            SUM(air_yards_completed) AS air_yards_completed,
            SUM(td) AS td,
            SUM(int) AS int,
            SUM(fumbles) as fumbles,
            SUM(epa) AS epa,
            SUM(cpoe) AS cpoe
        FROM
            receiving_by_player_by_game
        GROUP BY
            year, game_id, pos,
            week, team, opp, season_type
        ORDER BY epa DESC
    """
    df = pd.read_sql(query, db_conn)
    logging.info(f"Extracted {len(df)} rows of receving stats.")
    return df


def _extract_all(db_conn) -> pd.DataFrame:
    """Getting passing stats, per team per game for all positions"""
    logging.info("Extracting receving stats by player by game from play by play...")
    query = """
        SELECT
            year,
            season_type,
            game_id,
            team,
            opp,
            week,
            'all' AS pos,
            SUM(receptions) AS receptions,
            SUM(targets) AS targets,
            SUM(yards) AS yards,
            SUM(air_yards_intended) AS air_yards_intended,
            SUM(air_yards_completed) AS air_yards_completed,
            SUM(td) AS td,
            SUM(int) AS int,
            SUM(fumbles) as fumbles,
            SUM(epa) AS epa,
            SUM(cpoe) AS cpoe
        FROM
            receiving_by_player_by_game
        GROUP BY
            year, game_id, week, team, opp, season_type
        ORDER BY epa DESC
    """
    df = pd.read_sql(query, db_conn)
    logging.info(f"Extracted {len(df)} rows of receving stats.")
    return df


def _transform(df_by_position: pd.DataFrame, df_all: pd.DataFrame) -> pd.DataFrame:
    """Combine the yards by position and total yards."""
    return pd.concat([df_by_position, df_all])


def run() -> None:
    logging.info(f"Running job for {OUTPUT_TABLE_NAME}...")
    with get_db_eng().connect() as db_conn:
        df_by_position = _extract_by_position(db_conn)
        df_all = _extract_all(db_conn)
        df = _transform(df_by_position, df_all)
        load(db_conn, df, OUTPUT_TABLE_NAME)
        logging.info(f"Job for {OUTPUT_TABLE_NAME} complete.")


if __name__ == "__main__":
    configure_logging()
    run()
