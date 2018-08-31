import logging
import os
import pandas as pd

from slovar import slovar

import prf
from prf.utils.utils import maybe_dotted, prep_params
from prf.utils.csv import dict2tab
import datasets
from datasets.backends.base import Base

logger = logging.getLogger(__name__)

def normalize(_dict):
    n_dict = slovar()

    def _n(text):
        sc = ' /,()'
        return ''.join(['_' if c in sc else c for c in text]).lower()

    for kk,vv in list(_dict.items()):
        if isinstance(vv, str):
            vv = vv.strip()

        n_dict[_n(kk)] = vv

    return n_dict

class csv_dataset(object):
    NA_LIST = ['', '#N/A', '#N/A N/A', '#NA', '-1.#IND', '-1.#QNAN', '-NaN', '-nan',
                '1.#IND', '1.#QNAN', 'N/A',
                # 'NA', #removed this b/c we are using it in `parent` fields as a legit value not None.
                'NULL', 'NaN', 'n/a', 'nan', 'null']

    def __init__(self, file_name):
        if not os.path.isfile(file_name):
            raise ValueError('File does not exist %s' % file_name)
        self.file_name = file_name

    def process_params(self, params):
        _, specials = prep_params(slovar(params))

        par = slovar()
        par.skiprows = specials._start or None
        par.nrows = None if specials._limit == -1 else specials._limit

        return par, specials

    def process_row(self, row, specials):
        return slovar(row).unflat().extract(specials._fields)

    def read_csv(self, page_size, params):
        return pd.read_csv(self.file_name,
                        infer_datetime_format=True,
                        na_values = self.NA_LIST,
                        keep_default_na = False,
                        dtype=object,
                        chunksize = page_size,
                        **params)

    def get_collection(self, **params):
        for chunk in self.get_collection_paged(1000, **params):
            for each in chunk:
                yield each

    def get_collection_paged(self, page_size, **params):
        params, specials = self.process_params(params)

        df = self.read_csv(page_size, params)
        for chunk in df:
            yield [self.process_row(each[1],specials) for each in chunk.fillna('').iterrows()]

    def get_total(self, **query):
        params, specials = self.process_params(query)
        df = self.read_csv(None, params)
        return df.shape[0]

    def drop_collection(self):
        pass

    def unregister(self):
        pass

class CSVBackend(object):

    @classmethod
    def get_dataset(cls, ds):
        ds = Base.process_ds(ds)
        file_name = os.path.join(datasets.Settings.get('csv.root'), ds.ns, ds.name)
        return csv_dataset(file_name)

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

