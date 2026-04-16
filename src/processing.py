import logging
import pandas as pd

# Create child logger
logger = logging.getLogger(__name__)


def clean_data(data: list[dict]) -> pd.DataFrame:
    '''Cleans and standardizes data from endpoint.'''
    logger.info('Converting data to DataFrame.')
    df = pd.DataFrame(data)
    logger.debug(df)
    df.drop(columns=[
        'capacity-units',
        'outage-units',
        'percentOutage-units'
        ], inplace=True, errors='ignore')

    df.rename(columns={
        'facility': 'facility_id',
        'facilityName': 'facility_name',
        'generator': 'generator_number'
    }, inplace=True)

    # Period as datetime
    df['period'] = pd.to_datetime(df['period'], errors='coerce').dt.date
    logger.debug(df['period'][0])

    # Facility_id as a nullable integer
    df['facility_id'] = (
        pd.to_numeric(df['facility_id'], errors='coerce')
          .astype('Int64')
    )

    # Generator_number as a nullable integer
    df['generator_number'] = (
        pd.to_numeric(df['generator_number'], errors='coerce')
          .astype('Int64')
    )

    # Power values in Megawatts (MW)
    df['capacity'] = pd.to_numeric(df['capacity'], errors='coerce')
    df['outage'] = pd.to_numeric(df['outage'], errors='coerce')

    df = df.dropna(subset=[
        'facility_id',
        'facility_name',
        'generator_number',
        'period',
        'capacity',
        'outage'
    ])

    logger.info('Data converted to Data Frame.')

    return df
