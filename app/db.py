import os

import sqlalchemy as sa


def get_db_eng() -> sa.engine.Engine:
    """
    Create an Engine - a lazy connection that has not connected
    to the DB until .connect() is called.
    """
    connection_string = _get_connection_string()
    return sa.create_engine(connection_string)


def get_db_conn() -> sa.engine.Connection:
    """Create a connection to the DB via the engine."""
    return get_db_eng().connect()


def _get_connection_string() -> str:
    """Formats a connection string using the configuration variables."""
    username = os.getenv('POSTGRES_USER')
    password = os.getenv('POSTGRES_PASSWORD')
    host = os.getenv('POSTGRES_HOST')
    db = os.getenv('POSTGRES_DB')
    return f"postgresql://{username}:{password}@{host}/{db}"
