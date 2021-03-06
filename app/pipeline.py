import argparse
import logging

from app.config import (
    configure_logging,
    CURRENT_YEAR,
)
from app.jobs import (
    play_by_play,
    roster,
    headshots,
    lateral_receiving_yards,
    play_by_play_enriched,
    passing_by_player_by_game,
    passing_by_player_by_year,
    passing_by_team_by_game,
    passing_by_team_by_year,
    receiving_by_player_by_game,
    receiving_by_player_by_year,
    rushing_by_player_by_game,
    rushing_by_player_by_year,
    rushing_by_team_by_game,
    rushing_by_team_by_year,
    two_point_conversions_by_player_by_game,
    two_point_conversions_by_player_by_year
)


def run(year: int = CURRENT_YEAR):
    """Run the pipeline if there are new games in the play by play data."""
    # raw data
    # try:
    #     play_by_play.run(year)
    # except play_by_play.NoNewGamesException as e:
    #     logging.info(e)
    #     return

    # roster.run(year)
    # # headshots.run()
    # lateral_receiving_yards.run(year)
     # play_by_play_enriched.run()

    # aggregations
    passing_by_player_by_game.run()
    passing_by_player_by_year.run()
    passing_by_team_by_game.run()
    passing_by_team_by_year.run()

    receiving_by_player_by_game.run()
    receiving_by_player_by_year.run()

    rushing_by_player_by_game.run()
    rushing_by_player_by_year.run()
    rushing_by_team_by_game.run()
    rushing_by_team_by_year.run()

    two_point_conversions_by_player_by_game.run()
    two_point_conversions_by_player_by_game.run()

    # aggregations of aggregations


if __name__ == "__main__":
    configure_logging()
    parser = argparse.ArgumentParser(description='Run the pipeline.')
    parser.add_argument('-y', '--year')
    args = parser.parse_args()
    if args.year:
        run(args.year)
    else:
        run()
