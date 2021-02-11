"""
Upstream jobs: play_by_play_enriched
"""
import logging

import numpy as np
import pandas as pd

from app.config import (
    configure_logging,
)
from app.db import get_db_eng, load


OUTPUT_TABLE_NAME = "receiving_by_player_by_year"


def _extract(db_conn) -> pd.DataFrame:
    """Getting passing stats, per player per game"""
    logging.info("Extracting receving stats by player by year from play by play...")
    query = """
        SELECT
            receiving.*, laterals.lateral_rec_yards
        FROM
            (SELECT
                year,
                season_type,
                posteam AS team,
                receiver_gsis_id AS gsis_id,
                receiver_position AS pos,
                receiver,
                COUNT(DISTINCT game_id) as games,
                SUM(complete_pass) AS receptions,
                SUM(pass_attempt) AS targets,
                SUM(yards_gained) AS yards,
                SUM(air_yards) AS air_yards_intended,
                SUM(CASE WHEN complete_pass = 1 THEN air_yards ELSE 0 END) AS air_yards_completed,
                SUM(pass_touchdown) AS td,
                SUM(interception) AS int,
                SUM(fumble) AS fumbles,
                SUM(epa) AS epa,
                SUM(cpoe) AS cpoe
            FROM
                play_by_play_enriched
            WHERE
                play_type = 'pass'
                AND two_point_attempt = 0
                AND sack = 0
                AND receiver IS NOT NULL
            GROUP BY
                year, posteam, receiver_gsis_id, receiver_position, receiver, season_type) receiving
        LEFT JOIN
            (SELECT
                year,
                gsis_player_id,
                lateral_rec_yards
            FROM
                lateral_receiving_yards
            ) laterals
        ON
            receiving.year = laterals.year
            AND receiving.gsis_id = laterals.gsis_player_id
    """
    df = pd.read_sql(query, db_conn)
    logging.info(f"Extracted {len(df)} rows of receving stats.")
    return df


def _transform(df: pd.DataFrame) -> pd.DataFrame:
    """Add lateral receiving yards to total yards"""
    logging.info("Adding lateral receiving yards...")
    df['yards'] = np.where(
        ~df['lateral_rec_yards'].isna(),
        df['yards'] + df['lateral_rec_yards'],
        df['yards']
    )
    return df.drop('lateral_rec_yards', axis=1)


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
