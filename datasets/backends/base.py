import logging
from bson import ObjectId
from datetime import datetime
from pprint import pformat
import os

from slovar import slovar
from slovar.strings import split_strip
from prf.utils import typecast, str2dt

import datasets

logger = logging.getLogger(__name__)


from prf.es import ES
from elasticsearch import helpers

class JobLoggingHandler(logging.Handler):
    __LOGGING_FILTER_FIELDS = ['msecs',
                               'relativeCreated',
                               'levelno',
                               'created']

    @staticmethod
    def __get_es_datetime_str(timestamp):
        current_date = datetime.utcfromtimestamp(timestamp)
        return "{0!s}.{1:03d}Z".format(current_date.strftime('%Y-%m-%dT%H:%M:%S'), int(current_date.microsecond / 1000))

    def __init__(self, job):
        logging.Handler.__init__(self)
        self.job = job
        self._buffer = []
        self._index_name = 'job_logs.job_%s' % job.job.uid

    def flush(self):
        if self._buffer:
            actions = (
                {
                    '_index': self._index_name,
                    '_type': 'job_log',
                    '_source': log_record
                }
                for log_record in self._buffer
            )

            helpers.bulk(
                client=ES.api,
                actions=actions,
                stats_only=True
            )

    def emit(self, record):
        self.format(record)
        rec = dict(job=self.job)
        for key, value in record.__dict__.items():
            if key not in JobLoggingHandler.__LOGGING_FILTER_FIELDS:
                if key == "args":
                    value = tuple(str(arg) for arg in value)
                rec[key] = "" if value is None else value
        rec['timestamp'] = self.__get_es_datetime_str(record.created)

        self._buffer.append(rec)
        self.flush()


class Base(object):
    _operations = slovar()

    @classmethod
    def process_ds(cls, ds):
        if isinstance(ds, str):
            #to avoid circular dep
            from datasets.backends import MONGO_BE_NAME

            backend, _, name = ds.rpartition('://')
            backend = backend or MONGO_BE_NAME
            ns, _, name = ds.partition('.')
            return slovar(name=name, backend=backend, ns=ns)
        elif isinstance(ds, dict):
            ds.has('name')
            ds.has('backend')
            ds.has('ns')
        else:
            raise ValueError('ds should be either string or dict. got %s' % ds)

        return ds

    @classmethod
    def setup_job_logger(cls, job):
        # handler = JobLoggingHandler(job)
        es_log = logging.getLogger(__name__)
        # es_log.setLevel(logging.INFO)
        # es_log.addHandler(handler)
        return es_log

    @classmethod
    def define_op(cls, params, _type, name, **kw):

        if 'default' not in kw:
            kw.setdefault('raise_on_values', [None, '', []])

        ret_val = getattr(params, _type)(name, **kw)
        cls._operations[name] = type(ret_val)
        return ret_val

    @classmethod
    def validate_ops(cls, params):
        logger.debug('params:\n%s', pformat(params))

        invalid_ops = set(params.keys()) - set(cls._operations.keys())
        if invalid_ops:
            raise KeyError('Invalid operations %s' % list(invalid_ops))

    def __init__(self, params, job_log=None):
        params = slovar(params)

        self.define_op(params, 'asstr',  'name', raise_on_values=['', None])
        self.define_op(params, 'asbool', 'keep_ids', default=False)

        if self.define_op(params, 'asbool', 'overwrite', _raise=False, default=True) is None:
            self.define_op(params, 'aslist', 'overwrite', default=[])

        if self.define_op(params, 'asbool', 'flatten', _raise=False, default=False) is None:
            self.define_op(params, 'aslist', 'flatten', default=[])

        if self.define_op(params, 'asbool', 'pop_empty', _raise=False, default=False) is None:
            self.define_op(params, 'aslist', 'pop_empty', default=[])

        self.define_op(params, 'aslist', 'append_to', default=[])
        self.define_op(params, 'aslist', 'append_to_set', default=[])
        self.define_op(params, 'aslist', 'fields', allow_missing=True)
        self.define_op(params, 'aslist', 'remove_fields', default=[])
        self.define_op(params, 'aslist', 'update_fields', default=[])
        self.define_op(params, 'asbool', 'keep_source_logs', default=False)
        self.define_op(params, 'asbool', 'dry_run', default=False)
        self.define_op(params, 'asint',  'log_size', default=1000)
        self.define_op(params, 'aslist', 'log_fields', default=[])
        self.define_op(params, 'asbool', 'log_pretty', default=False)
        self.define_op(params, 'asbool', 'fail_on_error', default=True)
        self.define_op(params, 'aslist', 'show_diff', allow_missing=True)
        self.define_op(params, 'asstr',  'op')
        self.define_op(params, 'aslist', 'skip_by', allow_missing=True)
        self.define_op(params, 'asstr',  'transformer', allow_missing=True)
        self.define_op(params, 'asstr',  'backend', allow_missing=True)
        self.define_op(params, 'asstr',  'ns', raise_on_values=[None])
        self.define_op(params, 'aslist', 'pk', allow_missing=True)
        self.define_op(params, 'asbool', 'skip_timestamp', default=False)

        self._operations['query'] = dict
        self._operations['default'] = dict
        self._operations['settings'] = dict
        self._operations['transformer_args'] = dict

        self.validate_ops(params)

        params.op, _, params.op_params = params.op.partition(':')
        params.aslist('op_params', default=[])

        self.klass = datasets.get_dataset(params, define=True)

        self.params = params
        self.job_log = job_log or slovar()

        self.job_logger = self.setup_job_logger(self.job_log)

        if (self.params.append_to_set or self.params.append_to) and not self.params.flatten:
            for kk in self.params.append_to_set+self.params.append_to:
                if '.' in kk:
                    self.job_logger.warning('`%s` for append_to/appent_to_set is nested but `flatten` is not set', kk)

        self.transformer = datasets.get_transformer(params)

    def process_many(self, dataset):
        for data in dataset:
            self.process(data)

    def extract_log(self, data):
        return slovar(data.pop('log', {})).update_with(self.job_log)

    def extract_meta(self, data):
        log = self.extract_log(data)
        source = data.get('source', {})
        return slovar(log=log, source=source)

    def process(self, data):
        data = self.add_defaults(slovar(data))

        _op = self.params.op
        _op_params = self.params.op_params

        if _op in ['update', 'upsert', 'delete'] and not _op_params:
            raise ValueError('missing op params for `%s` operation' % _op)

        if _op == 'create':
            self.create(data)

        elif _op == 'update':
            params, objects = self.get_objects(_op_params, data)
            if not objects:
                self.log_not_found(params, data)
                return

            self.update_objects(params, objects, data)

        elif _op == 'upsert':
            params, objects = self.get_objects(_op_params, data)
            if objects:
                #udpate_fields allows to update only partially if data exists
                if self.params.update_fields:
                    data = data.extract(self.params.update_fields)

                self.update_objects(params, objects, data)
            else:
                self.create(data)

        elif _op == 'delete':
            params, objects = self.get_objects(_op_params, data)
            if not objects:
                self.log_not_found(params, data)
                return

            for obj in objects:
                self.delete(obj)

        else:
            raise KeyError(
                'Must provide `op` param. e.g. op=update:key1,key2')

    def create(self, data):
        if 'skip_by' in self.params:
            _params = self.build_query_params(data, self.params.skip_by)
            _params['_count'] = 1
            exists = self.klass.get_collection(**_params)
            if exists:
                logger.debug('SKIP creation: %s objects exists with: %s', exists, _params)
                return

        if 'id' in data and not self.params.keep_ids and 'original_id' not in data:
            data['original_id'] = data.pop('id', None)

        obj = self.klass()
        self._save(obj, data, self.extract_meta(data), is_new=True)
        logger.debug('CREATED %r', obj)

    def get_objects(self, keys, data):
        _params = self.build_query_params(data, keys)
        if 'query' in self.params:
            _params = _params.update_with(typecast(self.params.query))

        return _params, self.klass.get_collection(**_params.flat())

    def update_objects(self, _params, objects, data):
        update_count = len(objects)

        if update_count > 1:
            msg = 'Multiple (%s) updates for\n%s' % (update_count,
                                                     self.format4logging(query=_params))
            self.job_logger.warning(msg)

        if not self.params.keep_source_logs:
            #pop the source logs so it does not overwrite the target's
            data.pop('logs', [])

        meta = self.extract_meta(data)

        for each in objects:
            each_d = each.to_dict()

            if 'show_diff' in self.params:
                self.diff(data, each_d, self.params.show_diff)

            new_data = self.update_object(each_d, data)
            self._save(each, new_data, meta, is_new=False)

        return update_count

    def update_object(self, data, new_data):

        actions = self.params.extract(
                    ['overwrite', 'append_to', 'append_to_set', 'flatten'])

        if self.transformer:
            actions = self.transformer.pre_update(new_data, data) or actions
            logger.debug('ACTIONS:\n%s', pformat(actions))

        return data.update_with(new_data, **actions)

    def delete(self, obj):
        _d = obj.to_dict()

        try:
            if self.params.dry_run:
                logger.warning('DRY RUN')
                return _d

            obj.delete()
        finally:
            logger.debug('DELETED %r with data:\n%s', obj,
                                self.format4logging(data=_d))

    def format4logging(self, query=None, data=None):

        msg = []
        data_tmpl = 'DATA dict: %%.%ss' % self.params.log_size

        if query:
            msg.append('QUERY: `%s`' % query)
        if data:
            _data = slovar(data).extract(self.params.log_fields)
            _fields = list(data.keys())
            if self.params.log_pretty:
                _data = pformat(_data)

            msg.append('DATA keys: `%s`' % _fields)
            if self.params.log_fields:
                msg.append('LOG KEYS: `%s`' % self.params.log_fields)
            msg.append(data_tmpl % _data)

        return '\n'.join(msg)

    def add_defaults(self, data):
        if 'default' not in self.params:
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

        return data.flat().update_with(default_f, overwrite=0).unflat()

    def log_not_found(self, params, data, tags=[], msg=''):
        msg = msg or 'NOT FOUND in <%s> with:\n%s' % (self.klass,
                                        self.format4logging(
                                            query=params, data=data))

        if msg:
            self.job_logger.warning(msg)

    def new_logs(self, data, meta):
        logs = data.pop('logs', [])
        if not isinstance(logs, list):
            raise ValueError(
                '`logs` field suppose to be a `list` type, got %s instead.\nlogs=%s' % (type(logs), logs))

        logs.insert(0, meta.log)

        #TODO: remove this at some point when all logs are converted to dict
        #check the very first log item
        if isinstance(logs[-1]['source'], dict):
            #all logs are converted. nothing to do.
            return logs

        new_logs = []

        for each in logs:
            #old format `source` was just a string
            if isinstance(each.source, str):
                each.update(slovar(each).extract(
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

    def build_pk(self, data):
        pass

    def pre_save(self, data, meta, is_new):
        logs = self.new_logs(data, meta)

        if 'fields' in self.params:
            data = typecast(data.extract(self.params.fields))

        data = self.process_empty(data)

        if not data:
            return data

        if not self.params.skip_timestamp:
            if is_new:
                data['created_at'] = logs[0].created_at
            else:
                data.pop('created_at', None)
                data['updated_at'] = logs[0].created_at

        data['logs'] = logs

        return data

    def _save(self, obj, data, meta, is_new=False):
        pk = self.build_pk(data)

        if self.transformer:
            #pre_save returns iterator. we only care about the first item here.
            for data in self.transformer.pre_save(data, is_new=is_new):
                break

        _data = self.pre_save(data, meta, is_new)

        if not _data:
            logger.debug('NOTHING TO SAVE')
            return

        try:
            _data = self.save(obj, _data, pk)
            return _data
        finally:
            msg = 'SAVED (%s) %r with PK:%s\nDATA:\n%s' % (
                'UPDATE' if not is_new else 'NEW', obj, pk, self.format4logging(data=_data))
            if self.params.dry_run:
                logger.warning('DRY RUN: %s' % msg)
            else:
                logger.debug(msg)

    def save(self, obj, new_data, pk=None):
        raise NotImplementedError

    def build_query_params(self, data, _keys):
        query = slovar()

        for _k in _keys:
            #unflat if nested
            if '.' in _k:
                query.update(typecast(data.extract(_k).flat()))
            else:
                query.update(typecast(data.extract(_k)))

        if not query:
            if not _keys:
                raise ValueError('empty op params')

            raise ValueError('update query is empty for:\n%s' %
                             self.format4logging(
                                query=_keys, data=data))

        query['_limit'] = -1
        return query

    def diff(self, d1, d2, keys=[]):
        _diff = d1.extract(keys).diff(d2.extract(keys))
        if _diff:
            logger.info('DIFF:\n%s\n\n', self.format4logging(data=_diff))
        else:
            logger.info('DIFF: Identical')