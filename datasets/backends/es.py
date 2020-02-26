import logging
from elasticsearch import TransportError
from bson import ObjectId
from pprint import pformat

from slovar import slovar
from slovar.utils import maybe_dotted
from prf.es import ES
from prf.utils import chunks

import datasets
from datasets.backends.base import Base

log = logging.getLogger(__name__)


NOT_ANALLYZED = {
    "dynamic_templates": [
        { "notanalyzed": {
              "match":"*",
              "match_mapping_type": "string",
              "mapping": {
                  "type":"string",
                  "index":"not_analyzed",
                  "ignore_above": 10000,
              }
           }
        }
      ]
}


class ESBackend(Base):
    _ES_OP = ['create', 'update', 'upsert', 'delete']

    @classmethod
    def get_dataset(cls, ds, define=False):
        ds = cls.process_ds(ds)

        if ds.ns:
            name = '%s.%s' % (ds.ns, ds.name or '*')
        else:
            name = ds.name

        return ES(name)

    @classmethod
    def get_collections(cls, match=''):
        return ES.api.indices.get_alias(match, ignore_unavailable=True)

    @classmethod
    def get_aliases(cls, match=''):
        aliases = []
        for it in cls.get_collections(match).values():
            if not it['aliases']:
                continue
            aliases += it['aliases'].keys()

        return list(set(aliases))

    @classmethod
    def get_namespaces(cls):
        ns = []
        for each in cls.get_collections().keys():
            if '.' in each:
                ns.append(each.split('.')[0])

        return list(set(ns))

    @classmethod
    def get_root_collections(cls):
        return [it for it in ESBackend.get_collections() if '.' not in it]

    def __init__(self, params, job_log=None):
        self.define_op(params, 'asstr', 'mapping', allow_missing=True)
        self.define_op(params, 'asbool', 'mapping_update', default=False)

        if params.mapping_update and not params.get('mapping'):
            raise ValueError('mapping must be supplied with mapping_update flag')

        super(ESBackend, self).__init__(params, job_log)

        if self.params.op not in self._ES_OP:
            raise ValueError('wrong op %s. Must be one of %s' % (self.params.op, self._ES_OP))

        if self.params.op in ['update', 'upsert', 'delete'] and not self.params.op_params:
            raise ValueError('op params must be supplied')

        self.process_mapping()

    def log_action(self, data, index, pk, action):
        msg = '%s with %s(pk=%s)\n%s' % (action.upper(), index, pk, self.format4logging(data=data))
        if self.params.dry_run:
            log.warning('DRY RUN: %s' % msg)
        else:
            log.debug(msg)

    def create(self, data):
        pk, pk_val = self.build_pk(data)

        index = self.process_index(data)

        data = self.pre_save(data)

        if self.params.remove_fields:
            data = data.remove(self.params.remove_fields)

        self.add_to_buffer(index, data, pk_val=pk_val)

    def update(self, data):
        self.create(data)

    def upsert(self, data):
        self.create(data)

    def delete(self, data):
        index = self.process_index(data)
        self.add_to_buffer(index, data)

    def process_mapping(self):

        def set_default_mapping():
            log.warning('Forgot to pass the mapping param ? `notanalyzed` default mapping will be used')
            self.params.doc_type = 'notanalyzed'
            self.params.mapping_body = NOT_ANALLYZED

        def set_mapping(mapping):
            self.params.doc_type, self.params.mapping_body = maybe_dotted(self.params.mapping)()

        def use_or_create_mapping(index, mapping, force_update=False):
            doc_types = ES.get_doc_types(index)

            if doc_types and not force_update:
                self.params.doc_type = doc_types[0]

            else:
                if mapping:
                    set_mapping(mapping)
                else:
                    set_default_mapping()

                if not self.params.dry_run:
                    self.create_mapping(self.params)

            msg = 'Using mapping `%s`' % self.params.doc_type
            log.info(msg)

        ds = self.get_dataset(self.params)
        use_or_create_mapping(ds.index, self.params.get('mapping'),
                                            self.params.mapping_update)

        #disable throttling for fast bulk indexing
        ES.api.cluster.put_settings(body={
            'transient':{'indices.store.throttle.type' : 'none'}})

    def flush(self, data, **kw):
        return ES.flush(data)

    def raise_or_log(self, data_size, errors):

        def sort_by_status():
            errors_by_status = slovar()
            for each in errors:
                try:
                    errors_by_status.add_to_list(each[self.params.op]['status'], each)
                except KeyError:
                    errors_by_status.add_to_list('unknown', each)

            return errors_by_status

        errors_by_status = sort_by_status()

        if not self.params.fail_on_error:
            log.warning('`fail_on_error` is turned off !')

        if self.params.is_insert:
            log.warning('SKIP creation: %s documents already exist.',
                        len(errors_by_status.pop(409, [])))

        if self.params.op == 'delete':
            log.warning('SKIP deletion: %s documents did not exist.',
                        len(errors_by_status.pop(404, [])))

        for status, errors in errors_by_status.items():
            msg = '`%s` out of `%s` documents failed to index\n%.1024s' % (len(errors), data_size, errors)
            if self.params.fail_on_error:
                raise ValueError(msg)
            else:
                log.error(msg)

    def process_index(self, data):
        '''
            Choose the target index.
            Rules:
                if the index is an alias
                    if data._index is part of that alias: use it
                    else raise error, since the actual target index is ambiguous
                if target index is actually an index, use it.
        '''

        #pop incoming meta from data first. not the best place, but we need the `_index`.

        #if _id is used to update, keep it
        if '_id' not in self.params.pk:
            data.pop('_id', None)

        data.pop('_type', None)

        data_index = data.pop('_index', None)
        index = self.klass.index

        #target is an alias ?
        if index in self.klass.alias_map:
            if data_index and data_index in self.klass.index_map:
                index = data_index
            else:
                raise ValueError('Target is an alias that does not contain the incoming data index.'
                                 '\nTarget index: `%s`\nData index: `%s`' % (self.klass.alias_map, data_index))

        return index

    def add_to_buffer(self, index, data, pk_val=None):
        if not pk_val:
            pk, pk_val = self.build_pk(data)

        action = slovar({
            '_index': index,
            '_id': pk_val,
        })

        if ES.version.major < 7:
            action['_type'] = self.params.doc_type

        #pop previous action key fields if any
        data.pop_many(action.keys())

        if self.params.op == 'create':
            if self.params.is_insert:
                # use create operation to insert new doc or fail if exists.
                # failures are handled in flush method
                action['_op_type'] = 'create'
            else:
                # otherwise using index to create or overwrite if exists.
                action['_op_type'] = 'index'

            action['_source'] = data

        elif self.params.op in ['update', 'upsert']:
            action['_op_type'] = 'update'
            action['_retry_on_conflict'] = 3
            action['doc'] = data

            if self.params.op == 'upsert':
                action['doc_as_upsert'] = True

        elif self.params.op == 'delete':
            action['_op_type'] = 'delete'

        else:
            raise ValueError('Bad op %s' % self.params.op)

        with self._buffer_lock:
            self._buffer.append(action)

        self.log_action(data, index, pk_val, self.params.op)

        return data

    @classmethod
    def index_name(cls, params):
        return '%s.%s' % (params.ns, params.name)

    @classmethod
    def create_mapping(cls, params):
        ds = cls.get_dataset(params)

        if not ES.api.indices.exists(ds.index):
            index_settings = datasets.Settings.extract('es.index.*')

            if params.get('settings'):
                index_settings.update(params.settings.extract('es.*'))

            log.info('Index settings: %s' % index_settings)
            try:
                ES.api.indices.create(ds.index, body=index_settings or None)
            except TransportError as e:
                if 'index_already_exists_exception' not in e.error:
                    raise e

        meta = ES.get_meta(ds.index)
        if not meta.get('mapping'):
            ES.put_mapping(index = ds.index,
                                  doc_type = params.doc_type,#obsolete in ver >= 7
                                  body = params.mapping_body)

    @classmethod
    def update_settings(cls, index, body):
        ES.api.indices.close(index)

        ES.api.indices.put_settings(**dict(
            index=index,
            body=body,
        ))

        ES.api.indices.open(index)
        return ES.api.indices.get_settings(index)

    @classmethod
    def drop_index(cls, params):
        ds = cls.get_dataset(params)
        ES.api.indices.delete(index=ds.index, ignore=[400, 404])

    @classmethod
    def drop_namespace(cls, name):
        ES.api.indices.delete(index='%s.*' % name, ignore=[400, 404])

