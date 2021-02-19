"""
Upstream jobs: play_by_play_enriched
"""
import logging

import pandas as pd

from app.config import (
    configure_logging,
)
from app.db import get_db_eng, load


OUTPUT_TABLE_NAME = "passing_by_player_by_year"


def _extract(db_conn) -> pd.DataFrame:
    """Getting passing stats, per player per year"""
    logging.info("Extracting passing stats by player by game from play by play...")
    query = """
        SELECT
            year,
            season_type,
            posteam AS team,
            passer_gsis_id AS gsis_id,
            passer_position AS pos,
            passer as player,
            COUNT(DISTINCT p.game_id) AS games,
            SUM(complete_pass) AS completions,
            SUM(pass_attempt) AS attempts,
            SUM(CASE
                WHEN lateral_rec_yards IS NOT NULL AND sack = 0 THEN yards_gained + lateral_rec_yards
                WHEN sack = 0 THEN yards_gained
                ELSE 0 END
            ) AS yards,
            SUM(CASE WHEN sack = 0 THEN air_yards ELSE 0 END) AS air_yards_intended,
            SUM(CASE WHEN complete_pass = 1 THEN air_yards ELSE 0 END) AS air_yards_completed,
            SUM(pass_touchdown) AS td,
            SUM(interception) AS int,
            SUM(sack) as sacks,
            SUM(CASE WHEN sack = 1 THEN yards_gained ELSE 0 END) AS sack_yards,
            SUM(fumble) AS fumbles,
            SUM(fumble_lost) AS fumbles_lost,
            SUM(CASE WHEN play_type = 'qb_spike' THEN 1 ELSE 0 END) AS spikes,
            SUM(CASE WHEN play_type != 'qb_spike' THEN epa ELSE 0 END) AS pass_epa,
            SUM(epa) AS epa,
            SUM(CASE WHEN play_type = 'qb_spike' THEN epa ELSE 0 END) AS spike_epa,
            CASE WHEN
                SUM(cpoe) IS NULL OR SUM(pass_attempt) = 0 THEN 0
                ELSE SUM(cpoe) / SUM(pass_attempt) END
            AS cpoe
        FROM
            play_by_play_enriched AS p
        LEFT JOIN
            (SELECT
                game_id,
                play_id,
                SUM(lateral_rec_yards) as lateral_rec_yards
            FROM
                lateral_receiving_yards
            GROUP BY
                game_id, play_id
            ) AS l
        ON
            p.game_id = l.game_id AND p.play_id = l.play_id
        WHERE
            (play_type = 'pass' or play_type = 'qb_spike')
            AND two_point_attempt = 0
        GROUP BY
            year, posteam, passer_gsis_id, p.passer_position, passer, season_type
        ORDER BY
            epa DESC
    """
    df = pd.read_sql(query, db_conn)
    logging.info(f"Extracted {len(df)} rows of passing stats.")
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
