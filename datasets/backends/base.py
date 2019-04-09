import logging
from bson import ObjectId
from datetime import datetime
from pprint import pformat

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
        self.define_op(params, 'asstr',  'op')
        self.define_op(params, 'aslist', 'skip_by', allow_missing=True)
        self.define_op(params, 'asstr',  'transformer', allow_missing=True)
        self.define_op(params, 'asstr',  'backend', allow_missing=True)
        self.define_op(params, 'asstr',  'ns', raise_on_values=[None])
        self.define_op(params, 'aslist', 'pk', allow_missing=True)
        self.define_op(params, 'asbool', 'skip_timestamp', default=False)
        self.define_op(params, 'asbool', 'update_many', default=False)


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
            return self.create(data)

        elif _op == 'update':
            #TODO not sure why we extract source data keys to do remove.
            # if self.params.remove_fields:
                # self.params.remove_fields = data.extract(self.params.remove_fields).keys()

            return self.update(data)

        elif _op == 'upsert':
            return self.upsert(data)

        elif _op == 'delete':
            return self.delete(data)

        else:
            raise KeyError(
                'Must provide `op` param. e.g. op=update:key1,key2')

    def format4logging(self, query=None, data=None):

        msg = []
        if self.params.dry_run:
            data_tmpl = 'DATA dict: %ss'
        else:
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
        data_f = data.flat()

        dkeys = default_f.key_diff(data_f.keys())
        if dkeys:
            logger.debug('DEFAULT values for %s:\n %s', dkeys, pformat(default_f.extract(dkeys)))

        return data_f.update_with(default_f, overwrite=0).unflat()


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

    def pre_save(self, data):
        is_new = self.params.op == 'create'
        meta = self.extract_meta(data)

        if self.transformer:
            #pre_save returns iterator. we only care about the first item here.
            for data in self.transformer.pre_save(data, is_new=is_new):
                break

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

