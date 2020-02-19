import logging
import os
import csv
import fnmatch

from slovar import slovar

import prf

from prf.utils.utils import maybe_dotted, parse_specials, pager
from prf.utils.csv import (dict2tab, csv2dict, pd_read_csv,
                            get_csv_header, get_csv_total)

import datasets
from datasets.backends.base import Base

log = logging.getLogger(__name__)


FNMATCH_PATTERN = '*'

class field_processor:
    def __init__(self, fields):
        self.fields = fields

    def __call__(self, data):
        return data.extract(self.fields).flat(keep_lists=False)

class Results(list):
    def __init__(self, specials, data, total):
        list.__init__(self, [slovar(each) for each in data])
        self.total = total
        self.specials = specials

class CSV(object):

    def create_if(self, path):
        if os.path.isdir(path):
            return

        basedir = os.path.dirname(path)
        if not os.path.exists(basedir):
            os.makedirs(basedir)

        open(path, 'a').close()

    def __init__(self, ds, create=False):
        if ds.name.startswith('/'):
            file_name = ds.name
        else:
            file_name = os.path.join(datasets.Settings.get('csv.root'), ds.ns, ds.name)

        if not os.path.isfile(file_name):
            if create:
                self.create_if(file_name)
            else:
                log.error('File does not exist %s' % file_name)
        self.file_name = file_name
        self._total = None

    def sniff(self, file_name):
        try:
            with open(file_name, 'r') as csvfile:
                return csv.Sniffer().sniff(csvfile.read(1024))
        except Exception as e:
            log.error('Error sniffing %s file. error: %s', file_name, e)

    def get_file_or_buff(self):
        return self.file_name

    def clean_row(self, _dict):
        n_dict = slovar()

        def _n(text):
            text = text.strip()
            unders = ' ,\n'
            removes = '()/'

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

        return n_dict.unflat() # mongo freaks out when there are dots in the names

    def get_collection(self, **params):
        _, specials = parse_specials(slovar(params))

        if '_clean' in specials:
            processor = self.clean_row
        else:
            processor = lambda x: x.unflat()

        if specials._count:
            return self.get_total(**specials)

        items = csv2dict(self.get_file_or_buff(), processor=processor, **specials)
        return Results(specials, items, self.get_total(**specials))

    def get_collection_paged(self, page_size, **params):
        _, specials = parse_specials(slovar(params))

        #read the header here so get_collection doesnt need to
        params['_header'] = get_csv_header(self.get_file_or_buff())

        pgr = pager(specials._start, page_size, specials._limit)

        results = []
        for start, count in pgr():
            params.update({'_start':start, '_limit': count})
            results = self.get_collection(**params)
            yield results

    def get_total(self, **query):
        if not self._total:
            self._total = get_csv_total(self.get_file_or_buff())
        return self._total

    def drop_collection(self):
        try:
            os.remove(self.file_name)
        except FileNotFoundError as e:
            log.error(e)

    def unregister(self):
        pass

class CSVBackend(Base):

    @classmethod
    def ls_namespaces(cls):
        return os.listdir(datasets.Settings.get('csv.root'))

    @classmethod
    def is_ns(cls, path):
        _path = os.path.join(datasets.Settings.get('csv.root'), path)
        return os.path.isdir(_path)

    @classmethod
    def ls_ns(cls, ns, flat=False):
        base_path = os.path.join(datasets.Settings.get('csv.root'), ns)
        folders = []
        files = [] # only by extension

        if os.path.isdir(base_path):
            for root, subdirs, _files in os.walk(base_path):
                if not flat:
                    folders += subdirs
                    files += _files
                    break
                else:
                    for fl in _files:
                        path = root.split(base_path)[-1].strip('/')
                        files.append(os.path.join(path, fl))

            return sorted(folders) + sorted(files)

        raise prf.exc.HTTPBadRequest('%s is not a dir' % ns)

    @classmethod
    def get_dataset(cls, ds, define=False):
        return CSV(Base.process_ds(ds), create=define)

    def __init__(self, params, job_log):
        self.define_op(params, 'asstr', 'csv_root', default=datasets.Settings.get('csv.root'))
        self.define_op(params, 'asbool', 'drop', default=False)

        super().__init__(params, job_log)

        if not self.params.get('fields'):
            fields = maybe_dotted(self.params.get('fields_file'), throw=False)
            if not fields:
                raise prf.exc.HTTPBadRequest('Missing fields or fields_file')

            if not isinstance(fields, list):
                raise prf.exc.HTTPBadRequest('Expecting list object in fields_file. Got %s' % fields)

            self.params.fields = fields

        if not self.params.csv_root:
            raise prf.exc.HTTPBadRequest('Missing csv root. Pass it in params(csv_root) or in config file(csv.root)')

        self.transformer = self.get_transformer()

        dir_path = os.path.join(self.params.csv_root, self.params.ns)
        if not os.path.exists(dir_path):
            os.makedirs(dir_path)

        self.file_name = os.path.join(self.params.csv_root, self.params.ns, self.params.name)

    def get_transformer(self):
        if self.params.get('transformer'):
            trans, _, trans_as = self.params.transformer.partition('__as__')
            return maybe_dotted(trans)(trans_as=trans_as,
                **datasets.Settings.update_with(self.params.get('settings', {})))

    def flush(self, objs, **kw):
        #if file already exists, append to it since data is being processed in batches.
        if not self.params.drop and os.path.isfile(self.file_name) and os.path.getsize(self.file_name):
            file_opts = 'a+'
            skip_headers = True
        else:
            file_opts = 'w+'
            skip_headers = False

        with open(self.file_name, file_opts) as csv_file:
            csv_data = dict2tab(objs, self.params.fields, 'csv', skip_headers,
                                processor=self.params.get('processor', field_processor(self.params.fields)))
            csv_file.write(csv_data)

        success = total = len(objs)
        log.debug('BULK FLUSH: total=%s, success=%s, errors=%s, retries=%s', total, success, 0, 0)

        return success, 0, 0

    def create(self, data):
        with self._buffer_lock:
            self._buffer.append(data)

