import logging

from app.config import (
    configure_logging,
    START_YEAR,
    CURRENT_YEAR,
)
import app.pipeline as pipeline


def run(start_year: int = START_YEAR, end_year: int = CURRENT_YEAR):
    """Run the pipeline for multiple years."""
    logging.info(f"Starting backfill from {start_year} to {end_year}..")

    for year in range(start_year, end_year + 1):
        logging.info(f"Backfilling year {year}...")
        pipeline.run(year)

    logging.info("Backfill complete")


if __name__ == "__main__":
    configure_logging()
    run()
