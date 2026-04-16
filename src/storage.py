import pandas as pd
import os
import logging
from src.config import RDATA_DIR

logger = logging.getLogger(__name__)


def save_to_parquet(df: pd.DataFrame) -> None:
    'Create a parquet file from given df, and save it to data/'
    # Create the data folder if it doesn't exit
    os.makedirs('data', exist_ok=True)

    logger.debug(f'DataFrame info:\n{df.info()}')
    logger.debug(f'Head of DataFrame:\n{df.head()}')

    # save to parquet
    df.to_parquet(RDATA_DIR)
    logger.info(f'Saved {len(df)} rows to {RDATA_DIR}')
