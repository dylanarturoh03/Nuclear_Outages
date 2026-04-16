import logging
import os
from src.config import DATA_DIR, DB_DIR
from src.db import create_db, fill_db
from src.connector import fetch_data
from src.processing import clean_data
from src.storage import save_to_parquet
import pandas as pd


logger = logging.getLogger(__name__)

ENDPOINT: str = 'generator-nuclear-outages'


def run_ETLpipeline(start_date: str = None) -> None:
    '''
    Runs the full ETL pipeline.

    Fetches data from the EIA API starting from start_date,
    cleans and saves it to a parquet file, then inserts it
    into the DuckDB database.

    Args:
        start_date: Start date for data fetching in YYYY-MM-DD format.
                    Defaults to today - 365 days if not provided.
    '''
    logger.info('Initializing pipeline')
    logger.info(f'{DB_DIR.exists()}, {DB_DIR}')

    os.makedirs(DATA_DIR, exist_ok=True)  # Create ./data if it does not exist.

    if not DB_DIR.exists():
        logger.info('Creating DB.')
        create_db()
        logger.info('DB created.')

    logger.info(f'Processing {ENDPOINT} data')

    data: list[dict] = fetch_data(ENDPOINT, start_date)

    if data:
        df: pd.DataFrame = clean_data(data)
        save_to_parquet(df)
        logger.info(f'{ENDPOINT} data processed successfully')
        fill_db()
        logger.info('Data from .parquet file has been loaded into DB.')
    else:
        logger.info('No new data available.')
    logger.info('Successful')


def main() -> None:
    run_ETLpipeline('2026-03-01')


# if __name__ == '__main__':
    # main()
