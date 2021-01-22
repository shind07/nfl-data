from app.config import configure_logging
from app.jobs import (
    play_by_play,
    roster,
    play_by_play_enriched,
    rushing_by_player_by_game,
    rushing_by_player_by_year,
    rushing_by_team_by_game,
    rushing_by_team_by_year,
)


def run():
    # raw data
    roster.run()
    play_by_play.run()

    # aggregations
    play_by_play_enriched.run()
    rushing_by_player_by_game.run()
    rushing_by_player_by_year.run()
    rushing_by_team_by_game.run()
    rushing_by_team_by_year.run()


if __name__ == "__main__":
    configure_logging()
    run()
