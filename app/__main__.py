from app.config import configure_logging
from app.jobs import (
    get_play_by_play,
    get_roster,
)


def run():
    get_roster.run()
    get_play_by_play.run()


if __name__ == "__main__":
    configure_logging()
    run()
