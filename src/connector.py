import os
import requests
import logging
import time
from datetime import datetime, timedelta
from dotenv import load_dotenv

load_dotenv()
BASE_URL: str = 'https://api.eia.gov/v2/nuclear-outages'
API_KEY: str = os.getenv('API_KEY')

# Create logger
logger = logging.getLogger(__name__)


def is_date(s: str) -> bool:
    try:
        datetime.strptime(s, '%Y-%m-%d')
        return True
    except (ValueError, TypeError):
        return False


def fetch_page(url: str, params: dict) -> dict:
    '''
    Fetches a single page from the given URL with retry logic.

    Retries up to 3 times on network or HTTP errors with a 2 second
    delay between attempts. Raises immediately on 401, 403, and 404.
    '''
    for attempt in range(3):
        try:
            response = requests.get(url, params=params, timeout=5)
            response.raise_for_status()
            return response.json()
        except requests.exceptions.HTTPError:
            if response.status_code in (401, 403):
                raise ValueError('Invalid or missing API key.')
            if response.status_code == 404:
                raise ValueError(f'Endpoint not found: {url}')
            if attempt < 2:
                logger.warning(
                    f'HTTP error, attempt {attempt + 1}, retrying...'
                )
                time.sleep(2)
            else:
                raise
        except (
            requests.exceptions.Timeout,
            requests.exceptions.ConnectionError
        ):
            if attempt < 2:
                logger.warning(
                    f'Network error, attempt {attempt + 1}, retrying...'
                )
                time.sleep(2)
            else:
                raise ConnectionError(
                    'Could not connect to external API. '
                    'Check your internet connection.'
                )


def fetch_data(endpoint: str, start_date: str = None) -> list:
    """Fetches all pages from a given endpoint."""
    logger.info(f'Fetching data from {endpoint}')

    if start_date is None:
        # Defaults to a year ago and formats it as string
        logger.info('start_date is None. Defaulting to a year ago.')
        start_date = ((datetime.today() -
                      timedelta(days=365)).strftime('%Y-%m-%d'))
        logger.info('Could proccess None.')

    if not is_date(start_date):
        raise ValueError(f'{start_date} is not a valid date.')

    url = f"{BASE_URL}/{endpoint}/data/"
    all_rows = []
    offset = 0
    length = 5000
    page_num = 1

    params = {
        "api_key": API_KEY,
        "frequency": "daily",
        "data[0]": "capacity",
        "data[1]": "outage",
        "start": start_date,
        "offset": offset,
        "length": length
    }

    while True:
        data = fetch_page(url, params)
        page = data['response']
        rows = page['data']
        total = int(page['total'])
        if not rows:
            logger.info(f'Data from {endpoint} has been fully retrieved.')
            break

        all_rows.extend(rows)
        logger.info(f'Page {page_num} fetched. {len(all_rows)} rows.')

        if len(rows) < length:
            logger.info(f'Data from {endpoint} has been fully retrieved.')
            break

        offset += length
        if offset >= total:
            logger.info(f'Data from {endpoint} has been fully retrieved.')
            break

        params['offset'] = offset
        page_num += 1

    return all_rows
