"""
Upstream jobs: play_by_play_enriched

Need the 2 pt convert stats to compute fantasy points.
"""
import logging

import numpy as np
import pandas as pd

from app.config import (
    configure_logging,
)
from app.db import get_db_eng, load


OUTPUT_TABLE_NAME = "two_point_conversions_by_player_by_game"


def _extract_qb_stats(db_conn) -> pd.DataFrame:
    """Get two point conversions for QBs."""
    logging.info("Extracting two point conversions for QBs...")
    query = """
        SELECT 
            year, season_type, game_id, posteam AS team, defteam AS opp, week,
            passer_id AS gsis_id, passer_position AS pos, passer_player_name AS player,
            COUNT(*) AS two_point_conversions
        FROM
            play_by_play_enriched
        WHERE
            two_point_attempt = 1 and two_point_conv_result = 'success'
            AND passer_id IS NOT NULL and passer_position IS NOT NULL
        GROUP BY
            year, season_type, game_id, posteam, defteam, week, passer_id, passer_position, passer_player_name
    """
    return pd.read_sql(query, db_conn)


def _extract_skill_position_stats(db_conn) -> pd.DataFrame:
    """Get two point conversion stats for non QBs."""
    logging.info("Extracting two point conversions for skill position players...")
    query = """
        SELECT 
            year, season_type, game_id, posteam AS team, defteam AS opp, week, fantasy_player_id AS gsis_id,
            CASE WHEN receiver_position IS NULL THEN rusher_position ELSE receiver_position END AS pos,
            fantasy_player_name AS player,
            COUNT(*) AS two_point_conversions
        FROM
            play_by_play_enriched
        WHERE
            two_point_attempt = 1 and two_point_conv_result = 'success'
            AND passer_id IS NOT NULL and passer_position IS NOT NULL
        GROUP BY
            year, season_type, game_id, posteam, defteam, week, fantasy_player_id, fantasy_player_name,
            (CASE WHEN receiver_position IS NULL THEN rusher_position ELSE receiver_position END)
    """
    return pd.read_sql(query, db_conn)


def _transform(df_qb: pd.DataFrame, df_skill: pd.DataFrame) -> pd.DataFrame:
    """Concatenate the QB stats with the skill position stats"""
    return pd.concat([df_qb, df_skill])


def run() -> None:
    logging.info(f"Running job for {OUTPUT_TABLE_NAME}...")
    with get_db_eng().connect() as db_conn:
        df_qb = _extract_qb_stats(db_conn)
        df_skill = _extract_skill_position_stats(db_conn)
        df = _transform(df_qb, df_skill)
        load(db_conn, df, OUTPUT_TABLE_NAME)
        logging.info(f"Job for {OUTPUT_TABLE_NAME} complete.")


if __name__ == "__main__":
    configure_logging()
    run()
