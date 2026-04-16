from pathlib import Path

BASE_DIR = Path(__file__).parent.parent
DATA_DIR = BASE_DIR / 'data'
RDATA_DIR = DATA_DIR / 'generator-nuclear-outages.parquet'
DB_DIR = DATA_DIR / 'gen_outages.db'
