"""
We need to add some info to the play by play data:
1. For play_type == `qb_spike`, the passer is None
but passer_player_name is not. Since we eventually want to group on passer,
we need to replace these None values so qb spikes are included in the passing stats.

2. We add the receiver position from the roster data so we can
break down passing stats by receiver position. To do this, we need
to convert the play by play id to a gsis id so it can be joined to the
roster data.

Upstream jobs: play_by_play, roster
"""
import logging

import codecs
import numpy as np
import pandas as pd

from app.config import (
    configure_logging,
    CURRENT_YEAR
)
from app.db import get_db_eng

OUTPUT_TABLE_NAME = 'play_by_play_enriched'


def _convert_to_gsis_id(pbp_id):
    """Convert the play by play id to gsis id"""
    if pbp_id is None:
        return None

    if type(pbp_id) == float:
        return pbp_id

    return codecs.decode(pbp_id[4:-8].replace('-', ''), "hex").decode('utf-8')


def _extract(db_conn) -> pd.DataFrame:
    """Get the upstream tables."""
    logging.info("Extracting play by play data...")
    query = """SELECT * FROM play_by_play"""
    df_play_by_play = pd.read_sql(query, db_conn)
    logging.info(f"Extracted {len(df_play_by_play)} rows of play by play data.")

    logging.info("Extracting roster data...")
    query = """SELECT * FROM roster"""
    df_roster = pd.read_sql(query, db_conn)
    logging.info(f"Extracted {len(df_roster)} rows of roster data.")

    return df_play_by_play, df_roster


def _transform(df_play_by_play, df_roster) -> pd.DataFrame:
    """Fill in the empty passer values, add receiver position."""
    logging.info("Filling in passer values and adding GSIS ID....")
    df_play_by_play['passer'] = np.where(
        df_play_by_play['passer'].isna() & ~df_play_by_play['passer_player_name'].isna(),
        df_play_by_play['passer_player_name'],
        df_play_by_play['passer']
    )
    df_play_by_play['gsis_id'] = df_play_by_play['receiver_id'].apply(_convert_to_gsis_id)

    logging.info("Adding receiver position data from roster table...")
    df_roster_slim = df_roster[['season', 'position', 'gsis_id']]
    df_roster_slim = df_roster_slim.drop_duplicates()
    df_roster_slim = df_roster_slim[df_roster_slim['position'].notnull()]
    df_roster_slim = df_roster_slim[df_roster_slim['gsis_id'].notnull()]
    df_enriched = df_play_by_play.merge(
        df_roster_slim,
        how='left',
        left_on=['season', 'gsis_id'],
        right_on=['season', 'gsis_id']
    )
    logging.info(f"Enriched {len(df_enriched)} rows of play by play data.")

    return df_enriched


def _load(db_conn, df: pd.DataFrame) -> None:
    """Write DF to database."""
    logging.info(f"Writing {len(df)} rows to {OUTPUT_TABLE_NAME}...")
    df.to_sql(OUTPUT_TABLE_NAME, db_conn, index=False, if_exists='replace')


def run(year: int = CURRENT_YEAR) -> None:
    logging.info("Enriching play by play data with roster data...")
    with get_db_eng().connect() as db_conn:
        df_play_by_play, df_roster = _extract(db_conn)
        df_play_by_play_enriched = _transform(df_play_by_play, df_roster)
        _load(db_conn, df_play_by_play_enriched)


if __name__ == "__main__":
    configure_logging()
    run()
