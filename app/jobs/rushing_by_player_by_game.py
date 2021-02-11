"""
Creates a table with one row per rushing type (designed, scramble, qb_kneel, total) per player, per game.
"""
import logging

import pandas as pd

from app.config import (
    configure_logging,
)
from app.db import get_db_eng, load

OUTPUT_TABLE_NAME = "rushing_by_player_by_game"


def _extract_designed(db_conn) -> pd.DataFrame:
    """Getting designed rushing stats, per player per game, from the play by play."""
    logging.info("Extracting designed rushing stats from play by play...")
    query = """
        SELECT
            year,
            season_type,
            game_id,
            defteam AS opp,
            posteam AS team,
            week,
            rusher_gsis_id AS gsis_id,
            rusher_position AS pos,
            rusher,
            'designed' AS rush_type,
            SUM(rush) AS attempts,
            SUM(yards_gained) AS yards,
            SUM(rush_touchdown) AS td,
            SUM(fumble) AS fumbles,
            SUM(fumble_lost) AS fumbles_lost,
            SUM(fumble_out_of_bounds) AS fumbles_out_of_bounds,
            SUM(epa) AS epa
        FROM
            play_by_play_enriched
        WHERE
            play_type = 'run'
            AND two_point_attempt = 0
            AND rusher is not null
        GROUP BY
            year, season_type, game_id, rusher_gsis_id, rusher_position,
            week, posteam, defteam, rusher_id, rusher
        ORDER BY
            yards DESC
    """
    df = pd.read_sql(query, db_conn)
    logging.info(f"Extracted {len(df)} rows of designed rushing stats.")
    return df


def _extract_scrambles(db_conn) -> pd.DataFrame:
    """Getting scramble runs from the play by play.

    Scrambles 'play_type' of 'run', but the `rusher` value is null and the
    `passer` value is the scrambler.
    """
    logging.info("Extracting scramble rushing stats from play by play...")
    query = """
        SELECT
            year,
            season_type,
            game_id,
            defteam AS opp,
            posteam AS team,
            week,
            passer_gsis_id AS gsis_id,
            passer_position as pos,
            passer AS rusher,
            'scramble' AS rush_type,
            SUM(rush) AS attempts,
            SUM(yards_gained) AS yards,
            SUM(rush_touchdown) AS td,
            SUM(fumble) AS fumbles,
            SUM(fumble_lost) AS fumbles_lost,
            SUM(fumble_out_of_bounds) AS fumbles_out_of_bounds,
            SUM(epa) AS epa
        FROM
            play_by_play_enriched
        WHERE
            play_type = 'run'
            AND two_point_attempt = 0
            AND passer is not null
        GROUP BY
            year, season_type, game_id, week,
            posteam, defteam, passer_position, passer_gsis_id, passer
        ORDER BY
            yards DESC
    """
    df = pd.read_sql(query, db_conn)
    logging.info(f"Extracted {len(df)} rows of scramble rushing stats.")
    return df


def _extract_qb_kneels(db_conn) -> pd.DataFrame:
    """Getting qb kneels from the play by play since they are included in rushing stats."""
    logging.info("Extracting qb kneel stats from play by play...")
    query = """
        SELECT
            year,
            season_type,
            game_id,
            defteam AS opp,
            posteam AS team,
            week,
            rusher_position as pos,
            rusher_gsis_id as gsis_id,
            rusher,
            'qb_kneel' AS rush_type,
            SUM(rush) AS attempts,
            SUM(yards_gained) AS yards,
            SUM(rush_touchdown) AS td,
            SUM(fumble) AS fumbles,
            SUM(fumble_lost) AS fumbles_lost,
            SUM(fumble_out_of_bounds) AS fumbles_out_of_bounds,
            SUM(epa) AS epa
        FROM
            play_by_play_enriched
        WHERE
            play_type = 'qb_kneel'
        GROUP BY
            year, season_type, game_id, week,
            posteam, defteam, rusher_position, rusher_gsis_id, rusher
        ORDER BY
            yards DESC
    """
    df = pd.read_sql(query, db_conn)
    logging.info(f"Extracted {len(df)} rows of qb kneel stats.")
    return df


def _transform(df_designed, df_scrambles, df_qb_kneels) -> pd.DataFrame:
    """
    Concat all 3 rushing stats into a single DF.
    Then sum them to get the total rushing stats.
    Then add the total rushing stats to the DF with the original 3 rushing stats.
    The final DF has 4 possible rows distinguished by 'rush_type', which can either
    be designed, scramble, qb_kneel, or total.
    """
    logging.info("Calculating total rushing stats...")
    df_all = pd.concat([df_designed, df_scrambles, df_qb_kneels])

    grouping_cols = ['year', 'season_type', 'game_id', 'team', 'opp', 'week', 'gsis_id', 'pos', 'rusher']
    df_totals = df_all.groupby(grouping_cols, as_index=False).sum()
    df_totals['rush_type'] = 'total'
    df_final = pd.concat([df_all, df_totals])

    logging.info(f"Created {len(df_final)} rows of rushing stats.")
    return df_final


def run() -> None:
    logging.info(f"Running job for {OUTPUT_TABLE_NAME}...")
    with get_db_eng().connect() as db_conn:
        df_designed = _extract_designed(db_conn)
        df_scrambles = _extract_scrambles(db_conn)
        df_qb_kneels = _extract_qb_kneels(db_conn)

        df = _transform(df_designed, df_scrambles, df_qb_kneels)
        load(db_conn, df, OUTPUT_TABLE_NAME)
        logging.info(f"Job for {OUTPUT_TABLE_NAME} complete.")


if __name__ == "__main__":
    configure_logging()
    run()
