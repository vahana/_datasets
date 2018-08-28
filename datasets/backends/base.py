import logging
from bson import ObjectId
from datetime import datetime
from pprint import pformat
import os

from slovar import slovar
from slovar.strings import split_strip
from slovar.utils import maybe_dotted
from prf.utils.utils import typecast

import datasets

logger = logging.getLogger(__name__)

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

    def run_transformer(self, data):
        if 'transformer' in self.params:
            trans, _, trans_as = self.params.transformer.partition('__as__')
            trans = maybe_dotted(trans)(**datasets.Settings)
            for _d in trans(data):
                if trans_as:
                    _d = slovar({trans_as:_d})
                return _d

        return data

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
        self.define_op(params, 'asbool', 'overwrite', default=True)

        if self.define_op(params, 'asbool', 'flatten', _raise=False, default=False) is None:
            self.define_op(params, 'aslist', 'flatten', default=[])

        self.define_op(params, 'aslist', 'append_to', default=[])
        self.define_op(params, 'aslist', 'append_to_set', default=[])
        self.define_op(params, 'aslist', 'fields', allow_missing=True)
        self.define_op(params, 'aslist', 'remove_fields', default=[])
        self.define_op(params, 'aslist', 'update_fields', default=[])
        self.define_op(params, 'aslist', 'pop_empty', default=[])
        self.define_op(params, 'asbool', 'keep_source_logs', default=False)
        self.define_op(params, 'asbool', 'dry_run', default=False)
        self.define_op(params, 'asint',  'log_size', default=5000)
        self.define_op(params, 'aslist', 'log_fields', default=[])
        self.define_op(params, 'asbool', 'log_pretty', default=False)
        self.define_op(params, 'asbool', 'fail_on_error', default=True)
        self.define_op(params, 'aslist', 'show_diff', allow_missing=True)
        self.define_op(params, 'asstr',  'op')
        self.define_op(params, 'aslist', 'skip_by', allow_missing=True)
        self.define_op(params, 'asstr',  'transformer', allow_missing=True)
        self.define_op(params, 'asstr',  'backend', allow_missing=True)
        self.define_op(params, 'asstr',  'ns', raise_on_values=[None])
        self.define_op(params, 'asstr',  'pk')

        self._operations['query'] = dict
        self._operations['extra'] = dict
        self._operations['extra_options'] = dict

        self.validate_ops(params)

        params.op, _, params.op_params = params.op.partition(':')
        self.klass = datasets.get_dataset(params, define=True)

        self.params = params
        self.job_log = job_log or slovar()

        if (self.params.append_to_set or self.params.append_to) and not self.params.flatten:
            for kk in self.params.append_to_set+self.params.append_to:
                if '.' in kk:
                    logger.warning('`%s` for append_to/appent_to_set is nested but `flatten` is not set', kk)

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
        data = self.add_extra(slovar(data))

        _op = self.params.op
        _op_params = self.params.op_params

        if _op_params:
            _op_params = split_strip(_op_params)

        if _op in ['update', 'upsert', 'delete'] and not _op_params:
            raise ValueError('missing op params for `%s` operation' % _op)

        if _op == 'create':
            self.create(data)

        elif _op == 'update':
            params, objects = self.objects_to_update(_op_params, data)
            if not objects:
                self.log_not_found(params, data)
                return

            self.update_objects(params, objects, data)

        elif _op == 'upsert':
            params, objects = self.objects_to_update(_op_params, data)
            if objects:
                #udpate_fields allows to update only partially if data exists
                if self.params.update_fields:
                    data = data.extract(self.params.update_fields)

                self.update_objects(params, objects, data)
            else:
                self.create(data)

        elif _op == 'delete':
            params, objects = self.objects_to_update(_op_params, data)
            if not objects:
                self.log_not_found(params, data)
                return

            for obj in objects:
                self.delete(obj)

            logger.debug('DELETED %s objects by:\n%s', len(objects),
                            self.log_data_format(query=_op_params))

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
        self.save(obj, data, self.extract_meta(data))
        logger.debug('CREATED %r', obj)

    def objects_to_update(self, keys, data):
        for kk in keys:
            if '.' in kk and not self.params.flatten:
                logger.warning('Nested key `%s`? Consider using flatten=1', kk)

        _params = self.build_query_params(data, keys)
        if 'query' in self.params:
            _params = _params.update_with(typecast(self.params.query))

        return _params, self.klass.get_collection(**_params)

    def update_objects(self, _params, objects, data):
        update_count = len(objects)

        if update_count > 1:
            msg = 'Multiple (%s) updates for\n%s' % (update_count,
                                                     self.log_data_format(query=_params))
            logger.warning(msg)

        if not self.params.keep_source_logs:
            #pop the source logs so it does not overwrite the target's
            data.pop('logs', [])

        meta = self.extract_meta(data)

        for each in objects:
            each_d = each.to_dict()

            if 'show_diff' in self.params:
                self.diff(data, each_d, self.params.show_diff)

            new_data = each_d.update_with(
                                    data,
                                    overwrite=self.params.overwrite,
                                    append_to=self.params.append_to,
                                    append_to_set=self.params.append_to_set,
                                    flatten=self.params.flatten)

            logger.debug('UPDATE with data:\n%s', pformat(data))
            self.save(each, new_data, meta)

        return update_count

    def delete(self, obj):
        _d = obj.to_dict()

        try:
            if self.params.dry_run:
                logger.warning('DRY RUN')
                return _d

            obj.delete()
        finally:
            logger.debug('DELETED %r with data:\n%s', obj,
                                self.log_data_format(data=_d))

    def log_data_format(self, query=None, data=None):

        msg = []
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

    def add_extra(self, data):
        if 'extra' not in self.params:
            return data

        extra_opts = self.params.get('extra_options', {})
        extra_f = self.params.extra.flat()

        if '_transformer' in extra_f:
            extra_f.update(
                data.extract(extra_f.pop('_transformer', '')).flat())

        for k in extra_f:
            if extra_f[k] == '__gid__':
                extra_f[k] = str(ObjectId())
            elif extra_f[k] == '__today__':
                extra_f[k] = datetime.today()

        return data.flat().update_with(extra_f, **extra_opts).unflat()

    def log_not_found(self, params, data, tags=[], msg=''):
        msg = msg or 'NOT FOUND in <%s> with:\n%s' % (self.klass,
                                        self.log_data_format(
                                            query=params, data=data))

        if msg:
            logger.warning(msg)

    def pre_save(self, data, meta):
        data = self.run_transformer(data)

        logs = data.setdefault('logs', [])
        logs.insert(0, meta.log)

        if 'fields' in self.params:
            data = typecast(data.extract(self.params.fields))

        data['logs'] = logs

        for ekey in self.params.pop_empty:
            if ekey in data and data[ekey] in ['', None, []]:
                data.pop(ekey)

        return data

    def save(self, obj, data, meta):
        if not data:
            logger.debug('NOTHING TO SAVE')
            return

        _data = self.pre_save(data, meta)

        try:
            _data = self._save(obj, _data)
            return _data
        finally:
            msg = 'SAVED %r with data:\n%s' % (obj, self.log_data_format(data=_data))
            if self.params.dry_run:
                logger.warning('DRY RUN: %s' % msg)
            else:
                logger.debug(msg)

    def _save(self, obj, new_data):
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
                             self.log_data_format(
                                query=_keys, data=data))

        query['_limit'] = -1
        return query

    def diff(self, d1, d2, keys=None):
        _d1 = d1.extract(keys)
        _d2 = d2
        identical = True
        for k in _d1:
            if k in ['id', 'logs']:
                continue
            _d1k = _d1.get(k)
            _d2k = _d2.get(k)

            if _d1k != _d2k:
                identical = False
                logger.info('DIFF:\n\t`%s`:`%s`\n\t`%s`:`%s`', k, _d1k, k, _d2k)

        if identical:
            logger.info('DIFF: target contains source data')
