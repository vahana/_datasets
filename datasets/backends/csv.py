import logging
import os

from slovar import slovar

import prf

from prf.utils.utils import maybe_dotted, parse_specials, pager
from prf.utils.csv import (dict2tab, csv2dict, pd_read_csv,
                            get_csv_header, get_csv_total)
from prf.csv import CSV

import datasets
from datasets.backends.base import Base

log = logging.getLogger(__name__)


class Results(list):
    def __init__(self, specials, data, total):
        list.__init__(self, [slovar(each) for each in data])
        self.total = total
        self.specials = specials

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
        return CSV(Base.process_ds(ds), create=define,
                                    root_path=datasets.Settings.get('csv.root'))

    def __init__(self, params, job_log):

        self.define_op(params, 'asstr', 'csv_root', default=datasets.Settings.get('csv.root'))
        self.define_op(params, 'asbool', 'drop', default=False)

        super().__init__(params, job_log)

        if not self.params.fields:
            fields = maybe_dotted(self.params.get('fields_file'), throw=False)
            # if not fields:
            #     raise prf.exc.HTTPBadRequest('Missing fields or fields_file')

            if fields and not isinstance(fields, list):
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
            csv_data = dict2tab(objs, self.params.fields, 'csv', skip_headers)
            csv_file.write(csv_data)

        success = total = len(objs)
        log.debug('BULK FLUSH: total=%s, success=%s, errors=%s, retries=%s', total, success, 0, 0)

        return success, 0, 0

    def log_action(self, data, action):
        msg = '%s\n%s' % (action.upper(), self.format4logging(data=data))
        if self.params.dry_run:
            log.warning('DRY RUN: %s' % msg)
        else:
            log.debug(msg)

    def create(self, data):
        data = data.extract(self.params.fields)

        with self._buffer_lock:
            self._buffer.append(data)

        self.log_action(data, 'create')

