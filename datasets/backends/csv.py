import logging
import os

import prf
from prf.utils.csv import dict2tab
import impala

logger = logging.getLogger(__name__)

class CSVBackend(object):
    def __init__(self, params):
        self.params = params
        self.path = impala.Settings.get('csv.root')

        if not self.path:
            raise prf.exc.HTTPBadRequest('Missing csv.root in config file')

    def process_many(self, dataset):

        file_name = '%s/%s' % (self.path, self.params['name'])
        file_opts = 'w+'
        skip_headers = False
        #if file already exists, append to it
        if os.path.isfile(file_name) and os.path.getsize(file_name):
            file_opts = 'a+'
            skip_headers = True

        with open(file_name, file_opts) as csv_file:
            logger.info('Writing csv data to %s' % file_name)
            csv_data = dict2tab(dataset, self.params.fields,'csv', skip_headers)
            csv_file.write(csv_data)
            logger.info('Done')

