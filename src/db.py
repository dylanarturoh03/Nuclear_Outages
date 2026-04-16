import logging
import duckdb
from src.config import RDATA_DIR, DB_DIR

logger = logging.getLogger(__name__)


def create_db() -> None:
    '''Create DuckDB file.'''
    logger.info('Connecting to duckdb...')

    try:
        with duckdb.connect(database=DB_DIR) as con:

            # Create facilities table
            con.execute('''
            CREATE TABLE IF NOT EXISTS facilities (
                id BIGINT PRIMARY KEY,
                name VARCHAR
            );
            ''')

            # Create generators table
            con.execute('''
            CREATE TABLE IF NOT EXISTS generators (
                id BIGINT PRIMARY KEY,
                facility_id BIGINT NOT NULL,
                generator_number BIGINT NOT NULL,
                capacity DOUBLE,

                UNIQUE(facility_id, generator_number),

                FOREIGN KEY(facility_id) REFERENCES facilities(id)
            );
            ''')

            # Create outages table
            con.execute('''
            CREATE TABLE IF NOT EXISTS outages (
                id BIGINT PRIMARY KEY,
                period DATE NOT NULL,
                generator_id BIGINT NOT NULL,
                outage DOUBLE,

                UNIQUE(period, generator_id),

                FOREIGN KEY(generator_id) REFERENCES generators(id)
            );
            ''')

    except duckdb.Error as e:
        logger.error(f'Failed to create database: {e}')
        raise


def fill_db() -> None:
    '''Fill DuckDB database from parquet file.'''
    logger.info('Filling DB.')

    try:
        with duckdb.connect(database=DB_DIR) as con:
            con.begin()

            logger.info('Filling facilities table.')
            con.execute(f'''
                INSERT OR IGNORE INTO facilities (id, name)
                SELECT DISTINCT facility_id, facility_name
                FROM '{RDATA_DIR}';
            ''')

            logger.info('Filling generators table.')

            con.execute(f'''
                INSERT OR IGNORE INTO generators (
                    id,
                    facility_id,
                    generator_number,
                    capacity
                )
                SELECT
                    (SELECT COALESCE(MAX(id), 0) FROM generators) +
                    ROW_NUMBER() OVER (ORDER BY facility_id, generator_number),
                    facility_id,
                    generator_number,
                    MAX(capacity) AS capacity
                FROM '{RDATA_DIR}'
                GROUP BY facility_id, generator_number;
            ''')

            logger.info('Filling outages table.')

            con.execute(f"""
                INSERT OR IGNORE INTO outages (
                    id,
                    period,
                    generator_id,
                    outage
                )
                SELECT
                    (SELECT COALESCE(MAX(id), 0) FROM outages) +
                    ROW_NUMBER() OVER (ORDER BY p.period, g.id),
                    p.period,
                    g.id AS generator_id,
                    p.outage
                FROM '{RDATA_DIR}' p
                JOIN generators g
                    ON p.facility_id = g.facility_id
                    AND p.generator_number = g.generator_number
            """)
            con.commit()
            logger.info('DB filled.')

    except duckdb.Error as e:
        logger.error(f"Database error: {e}")
        raise

    except Exception as e:
        con.rollback()
        logger.error(f"Unexpected error: {e}")
        raise
