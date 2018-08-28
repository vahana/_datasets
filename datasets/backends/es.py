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
        'update': 'index',
        'upsert': 'index',
        'delete': 'delete',
    })

    @classmethod
    def get_dataset(cls, ds, define=False):
        ds = cls.process_ds(ds)

        if '.' in ds.name:
            ns, _, ds.name = ds.name.partition('.')
            ds.ns = ns or ds.ns

        name = '%s.%s' % (ds.ns, ds.name) if ds.ns else ds.name
        return ES(name)

    def __init__(self, params, job_log=None):
        self.define_op(params, 'asstr', 'mapping', allow_missing=True)

        super(ESBackend, self).__init__(params, job_log)

        if self.params.op not in self._ES_OP:
            raise ValueError('wrong op %s. Must be one of %s' % (self.params.op, self._ES_OP))

        if self.params.op != 'create' and not self.params.op_params:
            raise ValueError('op params must be supplied')

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

    def build_pk(self, data, pk):
        if len(pk) == 1:
            return data.flat()[pk[0]]

        pk_data = data.extract(pk).flat()
        pk = []

        for kk in sorted(pk_data.keys()):
            pk.append(str(pk_data[kk]))

        return ':'.join(pk)

    def add_to_buffer(self, data):
        action = slovar({
            '_index': self.klass.index,
            '_type': self.params.doc_type,
            '_id': self.build_pk(data, self.params.pk),
            '_op_type': self._ES_OP[self.params.op]
        })

        #these fields are "meta" fields, pop them before adding to buffer
        data.pop_many(action.keys())

        if self.params.op == 'delete':
            action['doc'] = data
        else:
            action['_source'] = data

        if not self.params.dry_run:
            self._buffer.append(action)

        return action

    def _save(self, obj, data):

        if self.params.remove_fields:
            data = data.extract(['-%s'%it for it in self.params.remove_fields])

        return self.add_to_buffer(data)

    def delete(self, data):
        try:
            item = self.add_to_buffer(data)

        finally:
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
