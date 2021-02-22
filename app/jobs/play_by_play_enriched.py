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
from app.db import get_db_eng, load


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
    df_play_by_play['passer_gsis_id'] = df_play_by_play['passer_id'] #.apply(_convert_to_gsis_id)
    df_play_by_play['receiver_gsis_id'] = df_play_by_play['receiver_id'] #.apply(_convert_to_gsis_id)
    df_play_by_play['rusher_gsis_id'] = df_play_by_play['rusher_id'] #.apply(_convert_to_gsis_id)

    logging.info("Adding position data from roster table...")
    df_roster_slim = df_roster[['season', 'position', 'gsis_id']]
    df_roster_slim = df_roster_slim.drop_duplicates()
    df_roster_slim = df_roster_slim[df_roster_slim['position'].notnull()]
    df_roster_slim = df_roster_slim[df_roster_slim['gsis_id'].notnull()]

    df_enriched = df_play_by_play.merge(
        df_roster_slim,
        how='left',
        left_on=['season', 'receiver_gsis_id'],
        right_on=['season', 'gsis_id']
    )
    df_enriched = df_enriched.rename(columns={'position': 'receiver_position'})
    df_enriched = df_enriched.drop(columns=['gsis_id'])

    df_enriched = df_enriched.merge(
        df_roster_slim,
        how='left',
        left_on=['season', 'rusher_gsis_id'],
        right_on=['season', 'gsis_id']
    )
    df_enriched = df_enriched.rename(columns={'position': 'rusher_position'})
    df_enriched = df_enriched.drop(columns=['gsis_id'])

    df_enriched = df_enriched.merge(
        df_roster_slim,
        how='left',
        left_on=['season', 'passer_gsis_id'],
        right_on=['season', 'gsis_id']
    )
    df_enriched = df_enriched.rename(columns={'position': 'passer_position'})
    df_enriched = df_enriched.drop(columns=['gsis_id'])

    # Because there is no passer_id on qb spikes, we need to join on something else
    df_roster['player'] = df_roster.apply(lambda row: f"{row['first_name'][0]}.{row['last_name']}", axis=1)
    df_roster_slim = df_roster[['season', 'team', 'player', 'position', 'gsis_id']]
    df_roster_slim = df_roster_slim.drop_duplicates()
    df_roster_slim = df_roster_slim[df_roster_slim['position'].notnull()]
    df_roster_slim = df_roster_slim[df_roster_slim['gsis_id'].notnull()]
    df_roster_slim = df_roster_slim.rename(
        columns={
            'gsis_id': "gsis_id_2",
            'position': 'position_2'
        }
    )
    df_enriched = df_enriched.merge(
        df_roster_slim,
        how='left',
        left_on=['season', 'posteam', 'passer'],
        right_on=['season', 'team', 'player']
    )
    df_enriched['passer_gsis_id'] = np.where(
        df_enriched['passer_gsis_id'].isna(),
        df_enriched['gsis_id_2'],
        df_enriched['passer_gsis_id']
    )
    df_enriched['passer_position'] = np.where(
        df_enriched['passer_position'].isna(),
        df_enriched['position_2'],
        df_enriched['passer_position']
    )
    df_enriched = df_enriched.drop(['team', 'player', 'gsis_id_2', 'position_2'], axis=1)

    logging.info(f"Enriched {len(df_enriched)} rows of play by play data.")
    return df_enriched


def run(year: int = CURRENT_YEAR) -> None:
    logging.info("Enriching play by play data with roster data...")
    with get_db_eng().connect() as db_conn:
        df_play_by_play, df_roster = _extract(db_conn)
        df_play_by_play_enriched = _transform(df_play_by_play, df_roster)
        load(db_conn, df_play_by_play_enriched, OUTPUT_TABLE_NAME, overwrite=False)


if __name__ == "__main__":
    configure_logging()
    run()
