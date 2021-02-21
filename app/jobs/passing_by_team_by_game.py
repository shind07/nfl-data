"""
Upstream jobs: passing_by_player_by_game
"""
import logging

import numpy as np
import pandas as pd

from app.config import (
    configure_logging,
    TEAM_GAME_GROUPING_COLUMNS
)
from app.db import get_db_eng, load

OUTPUT_TABLE_NAME = "passing_by_team_by_game"


def _extract(db_conn) -> pd.DataFrame:
    """Getting passing stats, per team per game for all positions"""
    logging.info("Extracting receving stats by player by game from play by play...")
    query = "SELECT * FROM passing_by_player_by_game"
    df = pd.read_sql(query, db_conn)
    return df[[col for col in df.columns if "_share" not in col]]


def _transform(df: pd.DataFrame) -> pd.DataFrame:
    """Aggregate player stats to the team level, recalculate the rate stats."""
    logging.info("Aggregating the player per game stats to the team level...")
    df = df.groupby(TEAM_GAME_GROUPING_COLUMNS, as_index=False).sum()

    df['target_share_wr'] = df['attempts_wr'] / df['attempts']
    df['target_share_te'] = df['attempts_te'] / df['attempts']
    df['target_share_rb'] = df['attempts_rb'] / df['attempts']
    df['target_share_other'] = df['attempts_other'] / df['attempts']
  
    df['air_yards_intended_share_wr'] = df['air_yards_intended_wr'] / df['air_yards_intended']
    df['air_yards_intended_share_te'] = df['air_yards_intended_te'] / df['air_yards_intended']
    df['air_yards_intended_share_rb'] = df['air_yards_intended_rb'] / df['air_yards_intended']

    df['air_yards_completed_share_wr'] = df['air_yards_completed_wr'] / df['air_yards_completed']
    df['air_yards_completed_share_te'] = df['air_yards_completed_te'] / df['air_yards_completed']
    df['air_yards_completed_share_rb'] = df['air_yards_completed_rb'] / df['air_yards_completed']  
    
    df = df.replace([np.inf, -np.inf], np.nan)
    return df.fillna(0)


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
