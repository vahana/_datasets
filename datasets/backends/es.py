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
                  "index":"not_analyzed"
              }
           }
        }
      ]
}


class ESBackend(Base):
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
    def get_meta(cls, ns, name):
        return ES(name).get_meta()

    def __init__(self, params, job_log=None):
        self.define_op(params, 'asstr', 'mapping', allow_missing=True)

        super(ESBackend, self).__init__(params, job_log)

        if self.params.op not in self._ES_OP:
            raise ValueError('wrong op %s. Must be one of %s' % (self.params.op, self._ES_OP))

        if self.params.op != 'create' and not self.params.op_params:
            raise ValueError('op params must be supplied')

        self.process_mapping()

    def process_mapping(self):

        def set_default_mapping():
            log.warning('Forgot to pass the mapping param ? `notanalyzed` default mapping will be used')
            self.params.doc_type = 'notanalyzed'
            self.params.mapping_body = NOT_ANALLYZED

        def set_mapping(mapping):
            self.params.mapping_body = maybe_dotted(self.params.mapping)()
            _, _, self.params.doc_type = self.params.mapping.rpartition('.')

        def use_or_create_mapping(index, mapping):
            exists = ES.api.indices.get_mapping(index=index, ignore_unavailable=True)
            if exists:
                self.params.doc_type = list(exists[index]['mappings'].keys())[0]
                if mapping:
                    self.warning('mapping param `%s` will be ignored in favor of pre-existing `%s`',
                                    mapping, self.params.doc_type)
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
        helpers.bulk(ES.api, self._buffer, stats_only=True)
        self._buffer = []

    def build_pk(self, data):
        if len(self.params.pk) == 1:
            pk = self.params.pk[0]
            if pk == '__GEN__':
                pk = str(ObjectId())
                data['id'] = pk
                return pk
            else:
                return data.flat()[pk]

        pk_data = data.extract(self.params.pk).flat()

        missing = set(self.params.pk) - set(pk_data.keys())
        if missing:
            raise KeyError('missing data for pk `%s`. got `%s`' % (self.params.pk, pk_data))

        pk = []
        for kk in sorted(pk_data.keys()):
            pk.append(str(pk_data[kk]))

        return ':'.join(pk)

    def add_to_buffer(self, data):
        action = slovar({
            '_index': self.klass.index,
            '_type': self.params.doc_type,
            '_id': self.build_pk(data),
            '_op_type': self._ES_OP[self.params.op]
        })

        #these fields are old meta fields, pop them before adding to buffer
        data.pop_many(action.keys())

        if self.params.op != 'delete':
            action['_source'] = data

        if not self.params.dry_run:
            self._buffer.append(action)

        return action

    def _save(self, obj, data):

        if self.params.remove_fields:
            data = data.extract(['-%s'%it for it in self.params.remove_fields])

        return self.add_to_buffer(data)

    def delete(self, data):
            item = self.add_to_buffer(data.to_dict())
            msg = 'DELETING %s with data:\n%s' % (item.extract('_index,_type,_id,_op_type'),
                                                            self.log_data_format(data=item))
            if self.params.dry_run:
                log.warning('DRY RUN: %s' % msg)
            else:
                log.debug(msg)


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
