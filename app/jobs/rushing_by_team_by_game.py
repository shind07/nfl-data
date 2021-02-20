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


def _extract_all(db_conn) -> pd.DataFrame:
    """Getting the raw rushing_by_player_by_game stats."""
    logging.info("Extracting rushing stats by player by year from play by play...")
    query = """SELECT * FROM rushing_by_player_by_game"""
    df = pd.read_sql(query, db_conn)
    logging.info(f"Extracted {len(df)} rows of rushing by team by game stats.")
    return df


def _extract_position(db_conn) -> pd.DataFrame:
    """Getting the rushing stats by position"""
    query = """
        SELECT
            year,
            season_type,
            game_id,
            team,
            opp,
            week,
            SUM(CASE WHEN pos = 'RB' THEN attempts ELSE 0 END) AS attempts_rb,
            SUM(CASE WHEN pos = 'RB' THEN yards ELSE 0 END) AS yards_rb,
            SUM(CASE WHEN pos = 'RB' THEN td ELSE 0 END) AS td_rb,
            SUM(CASE WHEN pos = 'RB' THEN epa ELSE 0 END) AS epa_rb,
            
            SUM(CASE WHEN pos = 'QB' THEN attempts_designed ELSE 0 END) AS attempts_qb_designed,
            SUM(CASE WHEN pos = 'QB' THEN yards_designed ELSE 0 END) AS yards_qb_designed,
            SUM(CASE WHEN pos = 'QB' THEN td_designed ELSE 0 END) AS td_qb_designed,
            SUM(CASE WHEN pos = 'QB' THEN epa_designed ELSE 0 END) AS epa_qb_designed,
            
            SUM(CASE WHEN pos = 'QB' THEN attempts_scramble ELSE 0 END) AS attempts_qb_scramble,
            SUM(CASE WHEN pos = 'QB' THEN yards_scramble ELSE 0 END) AS yards_qb_scramble,
            SUM(CASE WHEN pos = 'QB' THEN td_scramble ELSE 0 END) AS td_qb_scramble,
            SUM(CASE WHEN pos = 'QB' THEN epa_scramble ELSE 0 END) AS epa_qb_scramble,

            SUM(CASE WHEN pos = 'QB' THEN attempts_kneel ELSE 0 END) AS attempts_qb_kneel,
            SUM(CASE WHEN pos = 'QB' THEN yards_kneel ELSE 0 END) AS yards_qb_kneel,
            SUM(CASE WHEN pos = 'QB' THEN epa_kneel ELSE 0 END) AS epa_qb_kneel,
            
            SUM(CASE WHEN pos = 'WR' THEN attempts ELSE 0 END) AS attempts_wr,
            SUM(CASE WHEN pos = 'WR' THEN yards ELSE 0 END) AS yards_wr,
            SUM(CASE WHEN pos = 'WR' THEN td ELSE 0 END) AS td_wr,
            SUM(CASE WHEN pos = 'WR' THEN epa ELSE 0 END) AS epa_wr,
            
            SUM(CASE WHEN pos NOT IN ('RB', 'WR', 'QB') THEN attempts ELSE 0 END) as attempts_other,
            SUM(CASE WHEN pos NOT IN ('RB', 'WR', 'QB') THEN yards ELSE 0 END) as yards_other,
            SUM(CASE WHEN pos NOT IN ('RB', 'WR', 'QB') THEN td ELSE 0 END) as td_other,
            SUM(CASE WHEN pos NOT IN ('RB', 'WR', 'QB') THEN epa ELSE 0 END) as epa_other

        FROM
            rushing_by_player_by_game
        GROUP BY
            year, season_type, game_id, team, opp, week
    """
    df = pd.read_sql(query, db_conn)
    logging.info(f"Extracted {len(df)} rows of rushing by team by game by position stats.")
    return df


def _transform(df_all: pd.DataFrame, df_position: pd.DataFrame) -> pd.DataFrame:
    """Aggregate the per player stats to get the per team stats."""
    logging.info("Joining the position stats to the team totals...")
    grouping_cols = ['year', 'season_type', 'game_id', 'team', 'opp', 'week']
    df = df_all.groupby(grouping_cols, as_index=False).sum()
    return df.merge(df_position, on=grouping_cols)


def run() -> None:
    logging.info(f"Running job for {OUTPUT_TABLE_NAME}...")
    with get_db_eng().connect() as db_conn:
        df_all = _extract_all(db_conn)
        df_position = _extract_position(db_conn)
        df = _transform(df_all, df_position)
        load(db_conn, df, OUTPUT_TABLE_NAME)
        logging.info(f"Job for {OUTPUT_TABLE_NAME} complete.")


if __name__ == "__main__":
    configure_logging()
    run()
