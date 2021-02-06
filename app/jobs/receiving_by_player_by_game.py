"""
Upstream jobs: play_by_play_enriched

NOTE: entries where gsis_id are null are spikes.
"""
import logging

import pandas as pd

from app.config import (
    configure_logging,
)
from app.db import get_db_eng

OUTPUT_TABLE_NAME = "receiving_by_player_by_game"


def _extract(db_conn) -> pd.DataFrame:
    """Getting passing stats, per player per game"""
    logging.info("Extracting receving stats by player by game from play by play...")
    query = """
        SELECT
            year,
            season_type,
            game_id,
            posteam AS team,
            defteam as opp,
            week,
            receiver_gsis_id AS gsis_id,
            receiver_position AS pos,
            receiver,
            SUM(complete_pass) AS receptions,
            SUM(pass_attempt) AS targets,
            SUM(yards_gained) AS yards,
            SUM(air_yards) AS air_yards_intended,
            SUM(CASE WHEN complete_pass = 1 THEN air_yards ELSE 0 END) AS air_yards_completed,
            SUM(pass_touchdown) AS td,
            SUM(interception) as int,
            SUM(fumble) as fumbles,
            SUM(epa) AS epa,
            SUM(cpoe) AS cpoe
        FROM
            play_by_play_enriched
        WHERE
            (play_type = 'pass' or play_type = 'qb_spike')
            AND two_point_attempt = 0
            AND sack = 0
        GROUP BY
            year, game_id, receiver_gsis_id, receiver_position, 
            week, defteam, posteam, receiver, season_type
        ORDER BY
            epa DESC
    """
    df = pd.read_sql(query, db_conn)
    logging.info(f"Extracted {len(df)} rows of receving stats.")
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
