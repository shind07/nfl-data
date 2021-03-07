import logging
import os
import time
import urllib.request

import pandas as pd

from app.config import (
    HEADSHOTS_DIRECTORY,
    TEAM_LOGOS_DIRECTORY,
    TEAM_LOGOS_CSV_PATH
)


def init_directory(path: str) -> None:
    """Create a directory if it doesn't exist."""
    if not os.path.exists(path):
        logging.info(f"Creating directory {path}...")
        os.mkdir(path)


def get_headshot_path(season: str, team: str, gsis_id: str) -> str:
    """Deterministically generate the local path to the headshot png."""
    return os.path.join(
        HEADSHOTS_DIRECTORY,
        f"{season}_{team}_{gsis_id}.png"
    )


def download_image(remote_path: str, local_path: str) -> bool:
    """Download an image, return True if successful else False.

    Adding a sleep at the end to avoid spamming a remote path.
    """
    logging.info(f"Saving image from {remote_path} to {local_path}....")
    try:
        urllib.request.urlretrieve(remote_path, local_path)
        time.sleep(0.2)
        return True
    except Exception as e:
        logging.error(e)
        return False


def download_team_logos() -> None:
    """Download the team logos from the remote paths in the CSV."""
    init_directory(TEAM_LOGOS_DIRECTORY)
    df = pd.read_csv(TEAM_LOGOS_CSV_PATH)

    df['local_path'] = TEAM_LOGOS_DIRECTORY + '/wordmark_' + df['team_abbr'] + '.png'
    df['success'] = df.apply(
        lambda row: download_image(row['team_wordmark'], row['local_path']),
        axis=1
    )

    df['local_path'] = TEAM_LOGOS_DIRECTORY + '/logo_' + df['team_abbr'] + '.png'
    df['success'] = df.apply(
        lambda row: download_image(row['team_logo_wikipedia'], row['local_path']),
        axis=1
    )
