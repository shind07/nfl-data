"""
https://github.com/fantasydatapros/nflfastpy/blob/master/examples/Roster%20Data%20and%20Team%20Logo%20Data.ipynb
"""
import logging

import pandas as pd

from app.config import configure_logging, HEADSHOTS_DIRECTORY
from app.db import get_db_eng, load
from app.utils import (
    get_headshot_path,
    download_image,
    init_directory,
)

OUTPUT_TABLE_NAME = 'headshots'


def _extract(db_conn) -> pd.DataFrame:
    """Join roster to headshots to get the roster entries without a headshot."""
    logging.info("Getting roster entries without a headshot saved...")
    query = """
        SELECT * FROM
            (SELECT DISTINCT
                r.season,
                r.team,
                r.full_name,
                r.gsis_id,
                r.update_dt,
                r.headshot_url,
                h.local_path
            FROM roster r
            LEFT JOIN headshots h
            ON
                r.season = h.season
                AND r.team = h.team
                AND r.gsis_id = h.gsis_id
            ) x
        WHERE
            local_path IS NULL
            AND headshot_url IS NOT NULL
            AND gsis_id IS NOT NULL
            AND x.local_path IS NOT NULL
    """
    return pd.read_sql(query, db_conn)


def _transform(df: pd.DataFrame) -> pd.DataFrame:
    """Get the local path and save the headshot."""
    logging.info(f"Downloading {len(df)} headshots...")
    df['local_path'] = df.apply(
        lambda row: get_headshot_path(row['season'], row['team'], row['gsis_id']),
        axis=1
    )
    df['success'] = df.apply(
        lambda row: download_image(row['headshot_url'], row['local_path']),
        axis=1
    )
    return df


def run() -> None:
    logging.info("Grabbing headshots...")

    with get_db_eng().connect() as db_conn:
        df = _extract(db_conn)
        if len(df) == 0:
            logging.info("No new headshots to download!")
            return

        df = _transform(df)
        load(db_conn, df, OUTPUT_TABLE_NAME, overwrite=False)
        logging.info(f"Successfully downloaded {df['success'].sum()}/{len(df)} headshots.")


if __name__ == "__main__":
    configure_logging()
    init_directory(HEADSHOTS_DIRECTORY)
    run()
