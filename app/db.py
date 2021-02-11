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


def _atomic_rewrite(db_conn, df: pd.DataFrame, table_name: str) -> None:
    """Overwrite the table without dropping entirely.

    By default, pd.to_sql will drop the table and recreate its own schema,
    so by doing this we maintain the schema when overwriting a table.
    """
    db_conn.execute(f"""
        DROP TABLE IF EXISTS {table_name}_temp;
        CREATE TABLE {table_name}_temp AS TABLE {table_name};
        TRUNCATE TABLE {table_name};
    """)
    df.to_sql(table_name, db_conn, index=False, if_exists='append')
    db_conn.execute(f"""DROP TABLE {table_name}_temp;""")


def load(db_conn, df: pd.DataFrame, table_name: str, overwrite: bool = True, backup: bool = False) -> None:
    """Write a pandas DataFrame to a database."""
    logging.info(f"Writing {len(df)} rows to {table_name}...")

    if overwrite:
        _atomic_rewrite(db_conn, df, table_name)
    else:
        df.to_sql(table_name, db_conn, index=False, if_exists='append')

    if backup:
        output_path = os.path.join(
            DATA_DIRECTORY,
            f"{table_name}_{time.strftime('%Y%m%d-%H%M%S')}.csv")
        logging.info(f"Saving a csv copy to {output_path}...")
        df.to_csv(output_path, index=False)

    logging.info(f"{table_name} successfully loaded to the database.")
