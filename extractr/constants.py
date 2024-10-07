# constants.py

import os

# Folder paths
FOLDER_PATH = './'
INPUT_FOLDER = os.path.join(FOLDER_PATH, 'drop')
OUTPUT_FOLDER = os.path.join(FOLDER_PATH, 'output')
ARCHIVE_FOLDER = os.path.join(FOLDER_PATH, 'archive')
CACHE_FOLDER = os.path.join(FOLDER_PATH, 'cache')
CHECKPOINT_FILE = os.path.join(OUTPUT_FOLDER, 'checkpoint.json')

# API URLs
API_URLS = {
    'basic_info': 'https://api.brokercheck.finra.org/search/individual',
    'detailed_info': 'https://api.brokercheck.finra.org/search/individual/{crd_number}'
}
