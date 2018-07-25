import logging
from elasticsearch import helpers

from slovar import slovar
from prf.es import ES

from datasets.backends.base import Base

log = logging.getLogger(__name__)

class ESBackend(Base):
    _buffer = []
    _ES_OP = slovar({
        'create': 'index',
        'upsert': 'update',
        'update': 'update',
        'delete': 'delete',
    })

    def __init__(self, params, job_log=None):
        self.define_op(params, 'asstr', 'type', default='notanalyzed')
        self.define_op(params, 'asstr',  'pk')

        super(ESProcessor, self).__init__(params, job_log)

        #disable throttling for fast bulk indexing
        ES.api.cluster.put_settings(body={
            'transient':{'indices.store.throttle.type' : 'none'}})

        self.create_mapping(params)

    def process_many(self, dataset):
        super(ESProcessor, self).process_many(dataset)
        helpers.bulk(ES.api, self._buffer, stats_only=True)
        self._buffer = []

    def _save(self, obj, data):
        action = {
            '_index': self.klass.index,
            '_type': self.params.type,
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
    def create_mapping(cls, target):
        if target.type == 'company':
            cls.create_company_mapping(target)
        elif target.type == 'project':
            cls.create_project_mapping(target)
        elif target.type == 'person':
            cls.create_person_mapping(target)
        elif target.type == 'suggest':
            cls.create_suggest_mapping(target)
        elif target.type == 'notanalyzed':
            cls.create_notanalyzed_mapping(target)
        else:
            raise ValueError('Bad type: %s' % target.type)

    @classmethod
    def target_name(cls, name, mapping):
        return  '_'.join(filter(bool, [mapping, name]))

    @classmethod
    def create_suggest_mapping(cls, target):
        from smurfs.gazelle_es import SUGGEST_MAPPING

        index = cls.target_name(target.name, target.mapping)

        if not ES.api.indices.exists(index):
            index_settings = impala.Settings.extract('es.index.*')
            log.info('Index settings: %s' % index_settings)
            ES.api.indices.create(index, body=index_settings or None)

        ES.api.indices.put_mapping(**dict(
                index = index,
                doc_type = target.type,
                body = SUGGEST_MAPPING
            ))

    @classmethod
    def create_notanalyzed_mapping(cls, target):
        if not ES.api.indices.exists(target.name):
            index_settings = impala.Settings.extract('es.index.*')
            log.info('Index settings: %s' % index_settings)
            ES.api.indices.create(target.name, body=index_settings or None)

        ES.api.indices.put_mapping(**dict(
                index = target.name,
                doc_type = target.type,
                body = NOT_ANALLYZED
            ))

    @classmethod
    def create_company_mapping(cls, target):
        from smurfs.gazelle_es import GAZELLE_COMPANY

        if not ES.api.indices.exists(target.name):
            index_settings = impala.Settings.extract('es.index.*')
            log.info('Index settings: %s' % index_settings)
            ES.api.indices.create(target.name, body=index_settings or None)

        ES.api.indices.put_mapping(**dict(
                index = target.name,
                doc_type = target.type,
                body = GAZELLE_COMPANY
            ))

    @classmethod
    def create_project_mapping(cls, target):
        from smurfs.gazelle_es import GAZELLE_PROJECT
        if not ES.api.indices.exists(target.name):
            index_settings = impala.Settings.extract('es.index.*')
            log.info('Index settings: %s' % index_settings)
            ES.api.indices.create(target.name, body=index_settings or None)

        ES.api.indices.put_mapping(**dict(
                index = target.name,
                doc_type = target.type,
                body = GAZELLE_PROJECT
            ))

    @classmethod
    def create_person_mapping(cls, target):
        from smurfs.gazelle_es import GAZELLE_PERSON
        if not ES.api.indices.exists(target.name):
            index_settings = impala.Settings.extract('es.index.*')
            log.info('Index settings: %s' % index_settings)
            ES.api.indices.create(target.name, body=index_settings or None)

        ES.api.indices.put_mapping(**dict(
                index = target.name,
                doc_type = target.type,
                body = GAZELLE_PERSON
            ))

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
    def drop_index(cls, target):
        ES.api.indices.delete(index=target.name, ignore=[400, 404])
