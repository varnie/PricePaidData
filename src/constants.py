import os
from pathlib import Path

ROOT_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), os.pardir))

# folder for logs
LOGS_DIR = Path(ROOT_DIR) / 'logs'
# folder where initial downloaded data resides
DATA_DIR = Path(ROOT_DIR) / 'data'
# helper folder for keeping the single CSV file, single.csv, with unique initial data entries
PROCESSED_DATA_DIR = Path(ROOT_DIR) / 'processed_data'
# folder for queried data entries (results)
RESULTS_DIR = Path(ROOT_DIR) / 'results'

LOG_FILE = LOGS_DIR / 'app.log'

DOWNLOAD_LINKS = [
    'http://prod.publicdata.landregistry.gov.uk.s3-website-eu-west-1.amazonaws.com/pp-2020.csv',
    'http://prod.publicdata.landregistry.gov.uk.s3-website-eu-west-1.amazonaws.com/pp-2019.csv',
    'http://prod.publicdata.landregistry.gov.uk.s3-website-eu-west-1.amazonaws.com/pp-2018.csv',
    'http://prod.publicdata.landregistry.gov.uk.s3-website-eu-west-1.amazonaws.com/pp-2017-part2.csv',
    'http://prod.publicdata.landregistry.gov.uk.s3-website-eu-west-1.amazonaws.com/pp-2017-part1.csv',
    'http://prod.publicdata.landregistry.gov.uk.s3-website-eu-west-1.amazonaws.com/pp-2016-part2.csv',
    'http://prod.publicdata.landregistry.gov.uk.s3-website-eu-west-1.amazonaws.com/pp-2016-part1.csv',
    'http://prod.publicdata.landregistry.gov.uk.s3-website-eu-west-1.amazonaws.com/pp-2015-part2.csv',
    'http://prod.publicdata.landregistry.gov.uk.s3-website-eu-west-1.amazonaws.com/pp-2015-part1.csv',
    #TODO: add other links if needed
]


INIT_ACTION_NAME = 'init'
QUERY_POSTCODES_ACTION_NAME = 'query'

for dirpath in [LOGS_DIR, DATA_DIR, PROCESSED_DATA_DIR, RESULTS_DIR]:
    os.makedirs(dirpath, exist_ok=True)
