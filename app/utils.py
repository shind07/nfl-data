import logging
import os


def init_directory(path: str) -> None:
    if not os.path.exists(path):
        logging.info(f"Creating directory {path}...")
        os.mkdir(path)