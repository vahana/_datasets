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
        ds = slovar.copy(ds)
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
        params = slovar.copy(params)

        self.define_op(params, 'asstr',  'name', raise_on_values=['', None])

        if self.define_op(params, 'asbool', 'pop_empty', _raise=False, default=False) is None:
            self.define_op(params, 'aslist', 'pop_empty', default=[])

        self.define_op(params, 'aslist', 'fields', allow_missing=True)
        self.define_op(params, 'aslist', 'remove_fields', default=[])
        self.define_op(params, 'aslist', 'update_fields', default=[])
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
        self.define_op(params, 'asint', 'flush_retries', default=2)

        self.define_op(params, 'asstr',  'log_ds', default='', mod=str.lower)

        self._operations['query'] = dict
        self._operations['default'] = dict
        self._operations['settings'] = dict

        self.validate_ops(params)

        params.op, _, params.op_params = params.op.partition(':')
        params.pk = params.get('pk') or params.op_params or 'id'

        params.aslist('op_params', default=[])
        params.is_insert = params.op == 'create' and params.get('skip_by')
        self.params = params

        self.klass = datasets.get_dataset(self.params, define=True)

        self.job_log = job_log or slovar()

        self._buffer_lock = Lock()
        self._buffer = []
        self._log_buffer = []

    def process_many(self, dataset):
        for data in dataset:
            self.process(data)

        nb_retries = self.params.flush_retries

        if self.params.dry_run:
            return

        with self._buffer_lock:
            flush_buffer = self._buffer
            flush_log_buffer = self._log_buffer
            self._buffer = []
            self._log_buffer = []

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

        for chunk in chunks(flush_log_buffer, self.params.write_buffer_size):
            success, errors, retries = ES.flush(chunk)
            if errors:
                self.raise_or_log(len(chunk), errors)

    def raise_or_log(self, data_size, errors):
        msg = '`%s` out of `%s` documents failed to index\n%.1024s' % (len(errors), data_size, errors)
        if self.params.fail_on_error:
            raise ValueError(msg)
        else:
            log.error(msg)

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
            _data = data.extract(self.params.log_fields)
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
            if default_f[k] == '__OID__':
                default_f[k] = str(ObjectId())
            elif default_f[k] == '__TODAY__':
                default_f[k] = datetime.today()
            elif default_f[k] == '__NOW__':
                default_f[k] = datetime.now()

        default_f = typecast(default_f)
        data_f = data.flat()

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

    def process_empty(self, data):
        if self.params.pop_empty:
            if isinstance(self.params.pop_empty, bool):
                data = data.pop_by_values(['', None, []])
            else:
                for ekey in self.params.pop_empty:
                    if ekey in data and data[ekey] in ['', None, []]:
                        data.pop(ekey)
        return data

    def build_pk(self, data):
        if 'id' == self.params.pk:
            if self.params.pk not in data:
                data[self.params.pk] = str(ObjectId())
                return self.params.pk, data[self.params.pk]

        pk_data = data.extract(self.params.pk).flat()

        if not pk_data:
            raise KeyError('missing data for pk `%s`' % (self.params.pk))

        return pk_data.concat_values(sep=':')

    def save_log(self, data):
        if not self.params.get('log_ds'):
            return

        def build_log(data):
            pk, pkv = self.build_pk(data)
            log = slovar({
                'id': str(ObjectId()),
                pk: pkv,
                'target_pk_field': pk,
            })
            return self.job_log.update_with(log)

        log = build_log(data)

        action = slovar({
            '_index': self.params.log_ds,
            '_op_type': 'create',
            '_source': log.unflat()
        })

        if ES.version.major < 7:
            action['_type'] = 'notanalyzed'

        self._log_buffer.append(action)

        data.add_to_list(
            'logs', log.extract('job.contid,job.uid'))

    def process_fields(self, data):
        return typecast(data.extract(self.params.fields))

    def pre_save(self, data):
        is_new = self.params.op == 'create'

        self.save_log(data)

        data = self.process_empty(data)

        if 'fields' in self.params:
            data = self.process_fields(data)

        if not data:
            return data

        if not self.params.skip_timestamp:
            _now = datetime.utcnow()
            if is_new:
                data['created_at'] = _now

            data['updated_at'] = _now

        return data

