from app.config import configure_logging
from app.jobs import (
    get_play_by_play,
    get_roster,
    rushing_by_player_by_game,
)


def run():
    # raw data
    get_roster.run()
    get_play_by_play.run()

    # aggregations
    rushing_by_player_by_game.run()


if __name__ == "__main__":
    configure_logging()
    run()
