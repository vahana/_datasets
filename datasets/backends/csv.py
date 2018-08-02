import logging
import os

import prf
from prf.utils.utils import maybe_dotted
from prf.utils.csv import dict2tab
import datasets

logger = logging.getLogger(__name__)


class csv_dataset(object):
    def drop_collection(self):
        pass

    def unregister(self):
        pass

class CSVBackend(object):
    @classmethod
    def get_dataset(cls, ds):
        return csv_dataset()

    def __init__(self, params, job_log):
        params.asstr('csv_root', default=datasets.Settings.get('csv.root'))
        params.asbool('drop', default=False)

        if not params.get('fields'):
            fields = maybe_dotted(params.get('fields_file'), throw=False)
            if not fields:
                raise prf.exc.HTTPBadRequest('Missing fields or fields_file')

            if not isinstance(fields, list):
                raise prf.exc.HTTPBadRequest('Expecting list object in fields_file. Got %s' % fields)

            params.fields = fields

        if not params.csv_root:
            raise prf.exc.HTTPBadRequest('Missing csv root. Pass it in params(csv_root) or in config file(csv.root)')

        self.params = params

    def process_many(self, dataset):

        file_name = os.path.join(self.params.csv_root, self.params.name)
        file_opts = 'w+'
        skip_headers = False

        #if file already exists, append to it since data is being processed in batches.
        if not self.params.drop and os.path.isfile(file_name) and os.path.getsize(file_name):
            file_opts = 'a+'
            skip_headers = True

        with open(file_name, file_opts) as csv_file:
            logger.info('Writing csv data to %s' % file_name)
            csv_data = dict2tab(dataset, self.params.fields,'csv', skip_headers)
            csv_file.write(csv_data)
            logger.info('Done')

