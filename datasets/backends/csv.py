import logging
import os
import pandas as pd
import csv

from slovar import slovar

import prf
from prf.es import ESDoc, Results

from prf.utils.utils import maybe_dotted, parse_specials
from prf.utils.csv import dict2tab
import datasets
from datasets.backends.base import Base

log = logging.getLogger(__name__)

NA_LIST = ['', '#N/A', '#N/A N/A', '#NA', '-1.#IND', '-1.#QNAN', '-NaN', '-nan',
            '1.#IND', '1.#QNAN', 'N/A',
            # 'NA', #removed this b/c we are using it in `parent` fields as a legit value not None.
            'NULL', 'NaN', 'n/a', 'nan', 'null']


class CSV(object):

    def __init__(self, file_name):
        if not os.path.isfile(file_name):
            log.error('File does not exist %s' % file_name)
        self.file_name = file_name

    def sniff(self, file_name):
        try:
            with open(file_name, 'r') as csvfile:
                return csv.Sniffer().sniff(csvfile.read(1024))
        except Exception as e:
            log.error('Error sniffing %s file. error: %s', file_name, e)

    def process_params(self, params):
        _, specials = parse_specials(slovar(params))

        par = slovar()
        par.skiprows = specials._start or None
        par.nrows = None if specials._limit == -1 else specials._limit

        return par, specials

    def process_row(self, cell_dict, specials):
        def clean(_dict):
            n_dict = slovar()

            def _n(text):
                unders = ' ,\n'
                removes = '.()/'

                clean = ''
                for ch in text:
                    if ch in unders:
                        clean += '_'
                    elif ch in removes:
                        pass
                    else:
                        clean += ch

                return clean.lower()

            for kk,vv in list(_dict.items()):
                n_dict[_n(kk)] = vv

            return n_dict

        if '_clean' in specials:
            _d = clean(cell_dict)
        else:
            _d = slovar(cell_dict)

        return _d.unflat() # mongo freaks out when there are dots in the names

    def read_csv(self, page_size, params):
        return pd.read_csv(self.file_name,
                        infer_datetime_format=True,
                        na_values = NA_LIST,
                        keep_default_na = False,
                        dtype=object,
                        chunksize = page_size,
                        skip_blank_lines=True,
                        engine = 'c',
                        dialect = self.sniff(self.file_name),
                        **params)

    def get_collection(self, **params):
        params = slovar(params)
        _, specials = self.process_params(params)

        if specials._count:
            return self.get_total(**params)

        items = []
        for chunk in self.get_collection_paged(1000, **params):
            for each in chunk:
                items.append(each)

        return Results(None, specials, items, self.get_total(_limit=-1), 0)

    def get_collection_paged(self, page_size, **params):
        params = slovar(params)
        params, specials = self.process_params(params)

        df = self.read_csv(page_size, params)
        for chunk in df:
            yield [self.process_row(each[1],specials) for each in chunk.fillna('').iterrows()]

    def get_total(self, **query):
        params, specials = self.process_params(query)
        df = self.read_csv(None, params)
        return df.shape[0]

    def drop_collection(self):
        try:
            os.remove(self.file_name)
        except FileNotFoundError as e:
            log.error(e)

    def unregister(self):
        pass

class CSVBackend(object):

    @classmethod
    def ls_namespaces(cls):
        return os.listdir(datasets.Settings.get('csv.root'))

    @classmethod
    def ls_ns(cls, ns):
        path = os.path.join(datasets.Settings.get('csv.root'), ns)
        if os.path.isdir(path):
            return [it for it in os.listdir(os.path.join(datasets.Settings.get('csv.root'), ns))]

        raise prf.exc.HTTPBadRequest('%s is not a dir' % ns)

    @classmethod
    def get_dataset(cls, ds):
        ds = Base.process_ds(ds)
        if ds.name.startswith('/'):
            file_name = ds.name
        else:
            file_name = os.path.join(datasets.Settings.get('csv.root'), ds.ns, ds.name)
        return CSV(file_name)

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

        dir_path = os.path.join(self.params.csv_root, self.params.ns)
        file_name = os.path.join(self.params.csv_root, self.params.ns, self.params.name)

        if not os.path.exists(dir_path):
            os.makedirs(dir_path)

        if not file_name.endswith('csv'):
            file_name +='.csv'

        file_opts = 'w+'
        skip_headers = False

        #if file already exists, append to it since data is being processed in batches.
        if not self.params.drop and os.path.isfile(file_name) and os.path.getsize(file_name):
            file_opts = 'a+'
            skip_headers = True

        with open(file_name, file_opts) as csv_file:
            log.info('Writing csv data to %s' % file_name)
            csv_data = dict2tab(dataset, self.params.fields, 'csv', skip_headers, processor=self.params.get('processor'))
            csv_file.write(csv_data)
            log.info('Done')

