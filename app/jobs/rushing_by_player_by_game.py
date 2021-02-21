"""
Creates a table with one row per rushing type (designed, scramble, qb_kneel, total) per player, per game.
"""
import logging

import pandas as pd

from app.config import (
    configure_logging,
    PLAYER_GAME_GROUPING_COLUMNS
)
from app.db import get_db_eng, load

OUTPUT_TABLE_NAME = "rushing_by_player_by_game"


def _rename_cols(df: pd.DataFrame, suffix: str, exempt: list) -> pd.DataFrame:
    """Append a suffix to a subset of columns."""
    return df.rename(columns={
        col: f"{col}{suffix}" for col in df.columns if col not in exempt
    })


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
            rusher as player,
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
            passer AS player,
            SUM(pass) AS attempts,
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
            rusher as player,
            SUM(qb_kneel) AS attempts,
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
    Grab the rushing stats for the 3 rushing types (designed, scramble, kneel) and
    total them. Then join all 4 tables to create one table with all the rushing stats.

    Then, add designed and scramble stats to get basic rushing stats. These stats may
    differ slightly from official stats since qb kneels are excluded.
    """
    logging.info("Calculating total rushing stats...")
    df_all = pd.concat([df_designed, df_scrambles, df_qb_kneels])

    df_totals = df_all.groupby(PLAYER_GAME_GROUPING_COLUMNS, as_index=False).sum()

    df_totals = _rename_cols(df_totals, '_total', grouping_cols)
    df_designed = _rename_cols(df_designed, '_designed', grouping_cols)
    df_scrambles = _rename_cols(df_scrambles, '_scramble', grouping_cols)
    df_qb_kneels = _rename_cols(df_qb_kneels, '_kneel', grouping_cols)

    df_all = (
        df_totals
        .merge(
            df_designed,
            how='outer',
            on=grouping_cols,
        )
        .merge(
            df_scrambles,
            how='outer',
            on=grouping_cols,
        )
        .merge(
            df_qb_kneels,
            how='outer',
            on=grouping_cols,
        )
        .fillna(0)
    )

    df_all['attempts'] = df_all['attempts_designed'] + df_all['attempts_scramble']
    df_all['yards'] = df_all['yards_designed'] + df_all['yards_scramble']
    df_all['td'] = df_all['td_designed'] + df_all['td_scramble']
    df_all['fumbles'] = df_all['fumbles_designed'] + df_all['fumbles_scramble']
    df_all['fumbles_lost'] = df_all['fumbles_lost_designed'] + df_all['fumbles_lost_scramble']
    df_all['fumbles_out_of_bounds'] = df_all['fumbles_out_of_bounds_designed'] + \
        df_all['fumbles_out_of_bounds_scramble']
    df_all['epa'] = df_all['epa_designed'] + df_all['epa_scramble']

    logging.info(f"Created {len(df_all)} rows of rushing stats.")
    return df_all


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
