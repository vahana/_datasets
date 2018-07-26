import logging
from elasticsearch import helpers

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
        'upsert': 'update',
        'update': 'update',
        'delete': 'delete',
    })

    def __init__(self, params, job_log=None):
        self.define_op(params, 'asstr', 'mapping', allow_missing=True)
        self.define_op(params, 'asstr', 'pk')

        super(ESBackend, self).__init__(params, job_log)

        doc_type = 'notanalyzed'
        mapping_body = NOT_ANALLYZED

        if params.get('mapping'):
            mapping_body = maybe_dotted(params.mapping)()
            _, _, doc_type = params.mapping.rpartition('.')

        self.params.doc_type = doc_type
        self.params.mapping_body = mapping_body

        #disable throttling for fast bulk indexing
        ES.api.cluster.put_settings(body={
            'transient':{'indices.store.throttle.type' : 'none'}})

        self.create_mapping(self.params)

    def process_many(self, dataset):
        super(ESBackend, self).process_many(dataset)
        helpers.bulk(ES.api, self._buffer, stats_only=True)
        self._buffer = []

    def _save(self, obj, data):
        action = {
            '_index': self.klass.index,
            '_type': self.params.doc_type,
            '_id': data.get(self.params.pk),
            '_op_type': self._ES_OP[self.params.op]
        }

        if self.params.op == 'create':
            action['_source'] = data
        else:
            action['doc'] = data

        if self.params.op == 'upsert':
            action['doc_as_upsert'] = 'true'

        self._buffer.append(action)

    @classmethod
    def create_mapping(cls, params):
        if not ES.api.indices.exists(params.name):
            index_settings = datasets.Settings.extract('es.index.*')
            log.info('Index settings: %s' % index_settings)
            ES.api.indices.create(params.name, body=index_settings or None)

        ES.api.indices.put_mapping(**dict(index = params.name,
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
        ES.api.indices.delete(index=params.name, ignore=[400, 404])
