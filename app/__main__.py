from app.config import configure_logging
from app.jobs import (
    get_play_by_play,
    get_roster,
    rushing_by_player_by_game,
    rushing_by_player_by_year,
    rushing_by_team_by_game,
    rushing_by_team_by_year,
)


def run():
    # raw data
    get_roster.run()
    get_play_by_play.run()

    # aggregations
    rushing_by_player_by_game.run()
    rushing_by_player_by_year.run()
    rushing_by_team_by_game.run()
    rushing_by_team_by_year.run()


if __name__ == "__main__":
    configure_logging()
    run()
