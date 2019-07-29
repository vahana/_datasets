import logging
from bson import ObjectId
from datetime import datetime
from pprint import pformat
from threading import Lock

from slovar import slovar
from slovar.strings import split_strip
from prf.utils import typecast, str2dt

import datasets
from prf.es import ES
from prf.utils import chunks

log = logging.getLogger(__name__)


class Base(object):
    _operations = slovar()

    @classmethod
    def process_ds(cls, ds):
        if isinstance(ds, str):
            backend, _, name = ds.rpartition('://')
            ns, _, name = ds.partition('.')
            ds = slovar(name=name, backend=backend, ns=ns)
        elif isinstance(ds, dict):
            ds = slovar.to(ds)
        else:
            raise ValueError('ds should be either string or dict. got %s' % ds)

        ds.has('name')
        ds.has('backend', forbidden_values=['', None])
        ds.has('ns')

        return ds

    @classmethod
    def define_op(cls, params, _type, name, **kw):

        if 'default' not in kw:
            kw.setdefault('raise_on_values', [None, '', []])

        ret_val = getattr(params, _type)(name, **kw)
        cls._operations[name] = type(ret_val)
        return ret_val

    @classmethod
    def validate_ops(cls, params):
        log.debug('params:\n%s', pformat(params))

        invalid_ops = set(params.keys()) - set(cls._operations.keys())
        if invalid_ops:
            raise KeyError('Invalid operations %s' % list(invalid_ops))

    def __init__(self, params, job_log=None):
        params = slovar.to(params)

        self.define_op(params, 'asstr',  'name', raise_on_values=['', None])

        if self.define_op(params, 'asbool', 'pop_empty', _raise=False, default=False) is None:
            self.define_op(params, 'aslist', 'pop_empty', default=[])

        self.define_op(params, 'aslist', 'fields', allow_missing=True)
        self.define_op(params, 'aslist', 'remove_fields', default=[])
        self.define_op(params, 'aslist', 'update_fields', default=[])
        self.define_op(params, 'asbool', 'keep_source_logs', default=False)
        self.define_op(params, 'asbool', 'dry_run', default=False)
        self.define_op(params, 'asint',  'log_size', default=1000)
        self.define_op(params, 'aslist', 'log_fields', default=[])
        self.define_op(params, 'asbool', 'log_pretty', default=False)
        self.define_op(params, 'asbool', 'fail_on_error', default=True)
        self.define_op(params, 'asstr',  'op')
        self.define_op(params, 'aslist', 'skip_by', allow_missing=True)
        self.define_op(params, 'asstr',  'backend', allow_missing=True)
        self.define_op(params, 'asstr',  'ns', raise_on_values=[None])
        self.define_op(params, 'aslist', 'pk', allow_missing=True)
        self.define_op(params, 'asbool', 'skip_timestamp', default=False)
        self.define_op(params, 'asbool', 'verbose_logging', default=False)
        self.define_op(params, 'asbool', 'update_multi', default=False)

        self.define_op(params, 'asint', 'write_buffer_size', default=1000)
        self.define_op(params, 'asint', 'flush_retries', default=1)


        self._operations['query'] = dict
        self._operations['default'] = dict
        self._operations['settings'] = dict

        self.validate_ops(params)

        params.op, _, params.op_params = params.op.partition(':')
        params.aslist('op_params', default=[])
        params.is_insert = params.op == 'create' and params.get('skip_by')
        self.params = params

        self.klass = datasets.get_dataset(self.params, define=True)
        self.job_log = job_log or slovar()

        self._buffer_lock = Lock()
        self._buffer = []

    def process_many(self, dataset):
        for data in dataset:
            self.process(slovar.to(data))

        nb_retries = self.params.flush_retries

        if self.params.dry_run:
            return

        with self._buffer_lock:
            flush_buffer = self._buffer
            self._buffer = []

        for chunk in chunks(flush_buffer, self.params.write_buffer_size):
            success, errors, retries = self.flush(chunk)

            while(retries and nb_retries):
                log.debug('RETRY BULK FLUSH for %s docs', len(retries))
                success2, errors2, retries = self.flush(retries)
                success +=success2
                errors +=errors2
                nb_retries -=1

            if errors:
                self.raise_or_log(len(chunk), errors)

    def raise_or_log(self, data_size, errors):
        msg = '`%s` out of `%s` documents failed to index\n%.1024s' % (len(errors), data_size, errors)
        if self.params.fail_on_error:
            raise ValueError(msg)
        else:
            log.error(msg)

    def extract_log(self, data):
        return slovar.to(data.pop('log', {})).update_with(self.job_log)

    def extract_meta(self, data):
        log = self.extract_log(data)
        source = data.get('source', {})
        return dict(log=log, source=source)

    def process(self, data):
        data = self.add_defaults(data)

        _op = self.params.op
        _op_params = self.params.op_params

        if _op in ['update', 'upsert', 'delete'] and not _op_params:
            raise ValueError('missing op params for `%s` operation' % _op)

        if _op == 'create':
            return self.create(data)

        elif _op == 'update':
            return self.update(data)

        elif _op == 'upsert':
            return self.upsert(data)

        elif _op == 'delete':
            return self.delete(data)

        else:
            raise KeyError(
                'Must provide `op` param. e.g. op=update:key1,key2')

    def format4logging(self, query={}, data={}):

        if not self.params.verbose_logging:
            return '\nQUERY: %s\nDATA KEYS: %s\nDATA: %s' % (
                                    query, data.keys(), data.get('id'))


        msg = []
        if self.params.dry_run:
            data_tmpl = 'DATA dict: %s'
        else:
            data_tmpl = 'DATA dict: %%.%ss' % self.params.log_size

        if query:
            msg.append('QUERY: `%s`' % query)
        if data:
            _data = slovar.to(data).extract(self.params.log_fields)
            _fields = list(data.keys())
            if self.params.log_pretty:
                _data = pformat(_data)

            msg.append('DATA keys: `%s`' % _fields)
            if self.params.log_fields:
                msg.append('LOG KEYS: `%s`' % self.params.log_fields)
            msg.append(data_tmpl % _data)

        return '\n'.join(msg)

    def add_defaults(self, data):
        if not self.params.get('default'):
            return data

        default_f = self.params.default.flat()

        for k in default_f:
            if default_f[k] == '__oid__':
                default_f[k] = str(ObjectId())
            elif default_f[k] == '__today__':
                default_f[k] = datetime.today()
            elif default_f[k] == '__now__':
                default_f[k] = datetime.now()

        default_f = typecast(default_f)
        data_f = slovar.to(data).flat()

        dkeys = default_f.key_diff(data_f.keys())
        if dkeys:
            log.debug('DEFAULT values for %s:\n %s', dkeys, pformat(default_f.extract(dkeys)))

        return data_f.update_with(default_f, overwrite=0).unflat()


    def log_not_found(self, params, data, tags=[], msg=''):
        msg = msg or 'NOT FOUND in <%s> with:\n%s' % (self.klass,
                                        self.format4logging(
                                            query=params, data=data))

        if msg:
            log.warning(msg)

    def new_logs(self, data, meta):
        logs = data.pop('logs', [])
        if not isinstance(logs, list):
            raise ValueError(
                '`logs` field suppose to be a `list` type, got %s instead.\nlogs=%s' % (type(logs), logs))

        logs.insert(0, meta.get('log'))

        #TODO: remove this at some point when all logs are converted to dict
        #check the very first log item
        if isinstance(logs[-1]['source'], dict):
            #all logs are converted. nothing to do.
            return logs

        new_logs = []

        for each in logs:
            #old format `source` was just a string
            if isinstance(each.source, str):
                each.update(slovar.to(each).extract(
                    'source__as__source.name,target__as__target.name,merger__as__merger.name'))

            new_logs.append(each)

        return new_logs

    def process_empty(self, data):
        if self.params.pop_empty:
            if isinstance(self.params.pop_empty, bool):
                data = data.pop_by_values(['', None, []])
            else:
                for ekey in self.params.pop_empty:
                    if ekey in data and data[ekey] in ['', None, []]:
                        data.pop(ekey)
        return data

    def pre_save(self, data):
        is_new = self.params.op == 'create'
        meta = self.extract_meta(data)

        logs = self.new_logs(data, meta)
        data = self.process_empty(data)

        if not data:
            return data

        if 'fields' in self.params:
            data = typecast(data.extract(self.params.fields))

        data['logs'] = logs

        if not self.params.skip_timestamp:
            if is_new:
                data['created_at'] = logs[0].created_at

            data['updated_at'] = logs[0].created_at

        return data

