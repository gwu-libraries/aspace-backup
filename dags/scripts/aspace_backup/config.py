# ASPace API base URL
BASE_URL = 'https://archivesspace.library.gwu.edu/api'
# Repository number (int)
REPO = 2
# API credentials
USER = 'github'
PASSWORD = 'Summ3R2022'
# Adjust settings as needed
# Use base_local_path to reflect local directory in which to store backup
config = {'base_url': BASE_URL,
         'repo': REPO,
         'user': USER,
         'password': PASSWORD,
         'include_unpublished': 'false',
         'include_daos': 'false',
          'numbered_cs': 'false',
          'base_local_path': '/opt/aspace-backup/data',
          'log_level': 'DEBUG' # One of DEBUG, INFO, WARNING, ERROR
         }