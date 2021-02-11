import logging
import os

APP_NAME = "app"
DATA_DIRECTORY = "data"
START_YEAR = 1999
CURRENT_YEAR = 2020

GAMES_DIRECTORY = os.path.join(DATA_DIRECTORY, "games")
PLAY_BY_PLAY_DIRECTORY = os.path.join(DATA_DIRECTORY, "play_by_play")
ROSTER_DIRECTORY = os.path.join(DATA_DIRECTORY, "roster")
DERIVED_DATA_DIRECTORY = os.path.join(DATA_DIRECTORY, 'derived')

HEADSHOTS_DIRECTORY = os.path.join(DATA_DIRECTORY, 'headshots')
TEAM_LOGOS_DIRECTORY = os.path.join(DATA_DIRECTORY, 'logos')
TEAM_LOGOS_CSV_PATH = os.path.join(TEAM_LOGOS_DIRECTORY, 'teams_logos_colors.csv')


def configure_logging():
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s {%(filename)s:%(lineno)d} - %(levelname)s - %(message)s',
        datefmt='%Y-%m-%d %H:%M:%S'
    )
