import logging
from elasticsearch import helpers
from bson import ObjectId

from slovar import slovar
from slovar.utils import maybe_dotted
from prf.es import ES

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
    # _ROOT_NS = 'ROOT'
    _buffer = []
    _ES_OP = slovar({
        'create': 'index',
        'update': 'index',
        'upsert': 'index',
        'delete': 'delete',
    })

    @classmethod
    def get_dataset(cls, ds, define=False):
        ds = cls.process_ds(ds)

        if ds.ns:
            name = '%s.%s' % (ds.ns, ds.name)
        else:
            name = ds.name

        return ES(name)

    @classmethod
    def get_collections(cls, match=''):
        return ES.api.indices.get_alias(match, ignore_unavailable=True)

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

    @classmethod
    def get_meta(cls, ns, name):
        return ES(name).get_meta()

    def __init__(self, params, job_log=None):
        self.define_op(params, 'asstr', 'mapping', allow_missing=True)

        super(ESBackend, self).__init__(params, job_log)

        if self.params.op not in self._ES_OP:
            raise ValueError('wrong op %s. Must be one of %s' % (self.params.op, self._ES_OP))

        if self.params.op in ['update', 'upsert', 'delete'] and not self.params.op_params:
            raise ValueError('op params must be supplied')

        self.process_mapping()

    def process_mapping(self):

        def set_default_mapping():
            self.job_logger.warning('Forgot to pass the mapping param ? `notanalyzed` default mapping will be used')
            self.params.doc_type = 'notanalyzed'
            self.params.mapping_body = NOT_ANALLYZED

        def set_mapping(mapping):
            self.params.mapping_body = maybe_dotted(self.params.mapping)()
            _, _, self.params.doc_type = self.params.mapping.rpartition('.')

        def use_or_create_mapping(index, mapping):
            doc_types = ES.get_doc_types(index)
            if doc_types:
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
        use_or_create_mapping(ds.index, self.params.get('mapping'))

        #disable throttling for fast bulk indexing
        ES.api.cluster.put_settings(body={
            'transient':{'indices.store.throttle.type' : 'none'}})

    def process_many(self, dataset):
        super(ESBackend, self).process_many(dataset)

        try:
            if not self.params.dry_run:
                total, errors = helpers.bulk(ES.api, self._buffer, raise_on_error=False)
                if errors:
                    self.job_logger.error('`%s` out of `%s` documents failed to index.\n%s' % (len(errors), len(self._buffer), errors))

            for each in self._buffer:
                msg = '%s with data:\n%s' % (each.extract('_index,_type,_id,_op_type:upper'),
                                                                self.format4logging(data=each))
                if self.params.dry_run:
                    self.job_logger.warning('DRY RUN:\n'+ msg)
                else:
                    self.job_logger.debug(msg)
        finally:
            self._buffer = []

    def build_pk(self, data):
        if not self.params.get('pk'):
            if self.params.op in ['upsert', 'update', 'delete']:
                self.params.pk = self.params.op_params
            else:
                data['id'] = str(ObjectId())
                return data['id']

        if len(self.params.pk) == 1:
            pk = self.params.pk[0]
            return data.flat()[pk]

        pk_data = data.extract(self.params.pk).flat()

        missing = set(self.params.pk) - set(pk_data.keys())
        if missing:
            raise KeyError('missing data for pk `%s`. got `%s`' % (self.params.pk, pk_data))

        pk = []
        for kk in sorted(pk_data.keys()):
            pk.append(str(pk_data[kk]))

        return ':'.join(pk)

    def add_to_buffer(self, data, _id):
        action = slovar({
            '_index': self.klass.index,
            '_type': self.params.doc_type,
            '_id': _id,
            '_op_type': self._ES_OP[self.params.op]
        })


        if self.params.op != 'delete':
            #these fields are old meta fields, pop them before adding to buffer
            data.pop_many(action.keys())
            action['_source'] = data

        self._buffer.append(action)
        return action

    def _save(self, obj, data):

        if self.params.remove_fields:
            data = data.extract(['-%s'%it for it in self.params.remove_fields])

        return self.add_to_buffer(data, self.build_pk(data))

    def delete(self, data):
        self.add_to_buffer({}, getattr(data, self.params.op_params))

    @classmethod
    def index_name(cls, params):
        return '%s.%s' % (params.ns, params.name)

    @classmethod
    def create_mapping(cls, params):
        ds = cls.get_dataset(params)

        if not ES.api.indices.exists(ds.index):
            index_settings = datasets.Settings.extract('es.index.*')
            log.info('Index settings: %s' % index_settings)
            ES.api.indices.create(ds.index, body=index_settings or None)

        exists = ES.api.indices.get_mapping(index=ds.index, doc_type=params.doc_type, ignore_unavailable=True)
        if not exists:
            ES.api.indices.put_mapping(**dict(index = ds.index,
                                          doc_type = params.doc_type,
                                          body = params.mapping_body))

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

