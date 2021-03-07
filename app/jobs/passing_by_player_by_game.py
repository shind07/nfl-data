"""
Upstream jobs: play_by_play_enriched
"""
import logging

import numpy as np
import pandas as pd

from app.config import (
    configure_logging,
    PLAYER_GAME_GROUPING_COLUMNS
)
from app.db import get_db_eng, load

OUTPUT_TABLE_NAME = "passing_by_player_by_game"


def _extract_all(db_conn) -> pd.DataFrame:
    """Getting passing stats, per player per game"""
    logging.info("Extracting passing stats by player by game from play by play...")
    query = """
        SELECT
            year,
            season_type,
            p.game_id AS game_id,
            posteam AS team,
            defteam AS opp,
            week,
            passer_id AS gsis_id,
            passer_position AS pos,
            passer as player,
            SUM(complete_pass) AS completions,
            SUM(pass_attempt) - SUM(sack) AS attempts, 
            SUM(CASE
                WHEN lateral_rec_yards IS NOT NULL AND sack = 0 THEN yards_gained + lateral_rec_yards
                WHEN sack = 0 THEN yards_gained
                ELSE 0 END
            ) AS yards,
            SUM(CASE WHEN sack = 0 THEN air_yards ELSE 0 END) AS air_yards_intended,
            SUM(CASE WHEN complete_pass = 1 THEN air_yards ELSE 0 END) AS air_yards_completed,
            SUM(pass_touchdown) AS td,
            SUM(interception) as int,
            SUM(sack) as sacks,
            SUM(CASE WHEN sack = 1 THEN yards_gained ELSE 0 END) AS sack_yards,
            SUM(CASE WHEN sack = 1 THEN fumble ELSE 0 END) as fumbles,
            SUM(CASE WHEN sack = 1 THEN fumble_lost ELSE 0 END) AS fumbles_lost,
            SUM(CASE WHEN play_type = 'qb_spike' THEN 1 ELSE 0 END) AS spikes,
            SUM(epa) AS epa,
            SUM(CASE WHEN play_type != 'qb_spike' AND sack = 0 THEN epa ELSE 0 END) AS epa_pass,
            SUM(CASE WHEN interception = 1 THEN epa ELSE 0 END) as epa_int,
            SUM(CASE WHEN sack = 1 THEN epa ELSE 0 END) AS epa_sack,
            SUM(CASE WHEN play_type = 'qb_spike' THEN epa ELSE 0 END) AS epa_spike,
            SUM(cpoe) AS cpoe
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
            ) as l
        ON
            p.game_id = l.game_id AND p.play_id = l.play_id
        WHERE
            (play_type = 'pass' or play_type = 'qb_spike')
            AND two_point_attempt = 0
        GROUP BY
            year, week, passer_id, passer_position,
            p.game_id, defteam, posteam, passer, season_type
        ORDER BY
            epa DESC
    """
    df = pd.read_sql(query, db_conn)
    logging.info(f"Extracted {len(df)} rows of passing stats.")
    return df


def _extract_position(db_conn) -> pd.DataFrame:
    """Getting passing stats, per player per game by targeted position"""
    logging.info("Extracting passing stats by player by game by position from play by play...")
    query = """
        SELECT
            year,
            season_type,
            p.game_id AS game_id,
            posteam AS team,
            defteam AS opp,
            week,
            passer_id AS gsis_id,
            passer_position AS pos,
            passer as player,

            -- WR stats
            SUM(complete_pass) FILTER (WHERE receiver_position = 'WR') AS completions_wr,
            SUM(pass_attempt) FILTER (WHERE receiver_position = 'WR') AS attempts_wr,
            SUM(CASE
                WHEN lateral_rec_yards IS NOT NULL AND sack = 0 THEN yards_gained + lateral_rec_yards
                WHEN sack = 0 THEN yards_gained
                ELSE 0 END
            ) FILTER (WHERE receiver_position = 'WR') AS yards_wr,
            SUM(CASE WHEN sack = 0 THEN air_yards ELSE 0 END)
                FILTER (WHERE receiver_position = 'WR') AS air_yards_intended_wr,
            SUM(CASE WHEN complete_pass = 1 THEN air_yards ELSE 0 END)
                FILTER (WHERE receiver_position = 'WR') AS air_yards_completed_wr,
            SUM(pass_touchdown) FILTER (WHERE receiver_position = 'WR') AS td_wr,
            SUM(interception) FILTER (WHERE receiver_position = 'WR') as int_wr,
            SUM(epa) FILTER (WHERE receiver_position = 'WR') AS epa_wr,
            SUM(cpoe) FILTER (WHERE receiver_position = 'WR') AS cpoe_wr,

            -- TE stats
            SUM(complete_pass) FILTER (WHERE receiver_position = 'TE') AS completions_te,
            SUM(pass_attempt) FILTER (WHERE receiver_position = 'TE') AS attempts_te,
            SUM(CASE
                WHEN lateral_rec_yards IS NOT NULL AND sack = 0 THEN yards_gained + lateral_rec_yards
                WHEN sack = 0 THEN yards_gained
                ELSE 0 END
            ) FILTER (WHERE receiver_position = 'TE') AS yards_te,
            SUM(CASE WHEN sack = 0 THEN air_yards ELSE 0 END)
                FILTER (WHERE receiver_position = 'TE') AS air_yards_intended_te,
            SUM(CASE WHEN complete_pass = 1 THEN air_yards ELSE 0 END)
                FILTER (WHERE receiver_position = 'TE') AS air_yards_completed_te,
            SUM(pass_touchdown) FILTER (WHERE receiver_position = 'TE') AS td_te,
            SUM(interception) FILTER (WHERE receiver_position = 'TE') as int_te,
            SUM(epa) FILTER (WHERE receiver_position = 'TE') AS epa_te,
            SUM(cpoe) FILTER (WHERE receiver_position = 'TE') AS cpoe_te,

            -- RB stats
            SUM(complete_pass) FILTER (WHERE receiver_position = 'RB') AS completions_rb,
            SUM(pass_attempt) FILTER (WHERE receiver_position = 'RB') AS attempts_rb,
            SUM(CASE
                WHEN lateral_rec_yards IS NOT NULL AND sack = 0 THEN yards_gained + lateral_rec_yards
                WHEN sack = 0 THEN yards_gained
                ELSE 0 END
            ) FILTER (WHERE receiver_position = 'RB') AS yards_rb,
            SUM(CASE WHEN sack = 0 THEN air_yards ELSE 0 END)
                FILTER (WHERE receiver_position = 'RB') AS air_yards_intended_rb,
            SUM(CASE WHEN complete_pass = 1 THEN air_yards ELSE 0 END)
                FILTER (WHERE receiver_position = 'RB') AS air_yards_completed_rb,
            SUM(pass_touchdown) FILTER (WHERE receiver_position = 'RB') AS td_rb,
            SUM(interception) FILTER (WHERE receiver_position = 'RB') as int_rb,
            SUM(epa) FILTER (WHERE receiver_position = 'RB') AS epa_rb,
            SUM(cpoe) FILTER (WHERE receiver_position = 'RB') AS cpoe_rb,

            -- NULL stats
            SUM(complete_pass) FILTER (WHERE receiver_position IS NULL) AS completions_null,
            SUM(pass_attempt) FILTER (WHERE receiver_position IS NULL) AS attempts_null,
            SUM(CASE
                WHEN lateral_rec_yards IS NOT NULL AND sack = 0 THEN yards_gained + lateral_rec_yards
                WHEN sack = 0 THEN yards_gained
                ELSE 0 END
            ) FILTER (WHERE receiver_position IS NULL) AS yards_null,
            SUM(CASE WHEN sack = 0 THEN air_yards ELSE 0 END)
                FILTER (WHERE receiver_position IS NULL) AS air_yards_intended_null,
            SUM(CASE WHEN complete_pass = 1 THEN air_yards ELSE 0 END)
                FILTER (WHERE receiver_position IS NULL) AS air_yards_completed_null,
            SUM(pass_touchdown) FILTER (WHERE receiver_position IS NULL) AS td_null,
            SUM(interception) FILTER (WHERE receiver_position IS NULL) as int_null,
            SUM(epa) FILTER (WHERE receiver_position IS NULL) AS epa_null,
            SUM(cpoe) FILTER (WHERE receiver_position IS NULL) AS cpoe_null,

            -- Other stats
            SUM(complete_pass) FILTER
                (WHERE receiver_position NOT IN ('WR', 'TE', 'RB')
                AND receiver_position IS NOT NULL
                AND interception = 0)  AS completions_other,
            SUM(pass_attempt) FILTER
                (WHERE receiver_position NOT IN ('WR', 'TE', 'RB')
                AND receiver_position IS NOT NULL
                AND interception = 0)  AS attempts_other,
            SUM(CASE
                WHEN lateral_rec_yards IS NOT NULL AND sack = 0 THEN yards_gained + lateral_rec_yards
                WHEN sack = 0 THEN yards_gained
                ELSE 0 END
            ) FILTER
                (WHERE receiver_position NOT IN ('WR', 'TE', 'RB')
                AND receiver_position IS NOT NULL
                AND interception = 0)
            AS yards_other,
            SUM(CASE WHEN sack = 0 THEN air_yards ELSE 0 END) FILTER
                (WHERE receiver_position NOT IN ('WR', 'TE', 'RB')
                AND receiver_position IS NOT NULL
                AND interception = 0)  AS air_yards_intended_other,
            SUM(CASE WHEN complete_pass = 1 THEN air_yards ELSE 0 END)
                FILTER
                (WHERE receiver_position NOT IN ('WR', 'TE', 'RB')
                AND receiver_position IS NOT NULL
                AND interception = 0) AS air_yards_completed_other,
            SUM(pass_touchdown) FILTER
                (WHERE receiver_position NOT IN ('WR', 'TE', 'RB')
                AND receiver_position IS NOT NULL
                AND interception = 0) AS td_other,
            SUM(interception) FILTER
                (WHERE receiver_position NOT IN ('WR', 'TE', 'RB')
                AND receiver_position IS NOT NULL
                AND interception = 0) as int_other,
            SUM(epa) FILTER
                (WHERE receiver_position NOT IN ('WR', 'TE', 'RB')
                AND receiver_position IS NOT NULL
                AND interception = 0) AS epa_other,
            SUM(cpoe) FILTER
                (WHERE receiver_position NOT IN ('WR', 'TE', 'RB')
                AND receiver_position IS NOT NULL
                AND interception = 0) AS cpoe_other

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
            ) as l
        ON
            p.game_id = l.game_id AND p.play_id = l.play_id
        WHERE
            play_type = 'pass' AND sack = 0 AND two_point_attempt = 0
        GROUP BY
            year, week, passer_id, passer_position,
            p.game_id, defteam, posteam, passer, season_type
    """
    df = pd.read_sql(query, db_conn)
    logging.info(f"Extracted {len(df)} rows of passing stats.")
    return df


def _transform(df_all: pd.DataFrame, df_position: pd.DataFrame) -> pd.DataFrame:
    """Join the totals to the positional data and compute rate stats."""
    df = df_all.merge(df_position, how='left', on=PLAYER_GAME_GROUPING_COLUMNS)

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
        df_all = _extract_all(db_conn)
        df_position = _extract_position(db_conn)
        df = _transform(df_all, df_position)
        load(db_conn, df, OUTPUT_TABLE_NAME)
        logging.info(f"Job for {OUTPUT_TABLE_NAME} complete.")


if __name__ == "__main__":
    configure_logging()
    run()
