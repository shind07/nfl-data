import logging
import os


def init_directory(path: str) -> None:
    """Create a directory if it doesn't exist."""
    if not os.path.exists(path):
        logging.info(f"Creating directory {path}...")
        os.mkdir(path)
