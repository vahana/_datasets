import logging
from elasticsearch import helpers
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
    _ES_OP = ['create', 'index', 'update', 'upsert','delete']

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

    @classmethod
    def get_meta(cls, ns, name):
        return ES(name).get_meta()

    def __init__(self, params, job_log=None):
        self.define_op(params, 'asstr', 'mapping', allow_missing=True)
        self.define_op(params, 'asint', 'write_buffer_size', default=1000)
        self.define_op(params, 'asint', 'flush_retries', default=1)

        super(ESBackend, self).__init__(params, job_log)

        if self.params.op not in self._ES_OP:
            raise ValueError('wrong op %s. Must be one of %s' % (self.params.op, self._ES_OP))

        if self.params.op in ['update', 'upsert', 'delete'] and not self.params.op_params:
            raise ValueError('op params must be supplied')

        self.process_mapping()
        self._buffer = []

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

        nb_retries = self.params.flush_retries

        def flush(data):
            success, all_errors = helpers.bulk(ES.api, data, raise_on_error=False, refresh=True)
            errors = []
            retries = []
            retry_data = []

            if all_errors:
                #separate retriable errors
                for err in all_errors:
                    if err['index'].get('status') == 429: #too many requests
                        retries.append(err['index']['_id'])
                    else:
                        errors.append(err)

                    if retries:
                        for each in data:
                            if each['_id'] in retries:
                                retry_data.append(each)

            log.debug('BULK FLUSH: total=%s, success=%s, errors=%s, retries=%s',
                                    len(data), success, len(errors), len(retry_data))
            return success, errors, retry_data

        def raise_or_log(data_size, errors):
            msg = '`%s` out of `%s` documents failed to index\n%s' % (len(errors), data_size, errors)
            #if more than 1/4 failed, lets raise. clearly something is up !
            if len(errors) > data_size/4:
                raise ValueError(msg)
            else:
                self.job_logger.error(msg)

        try:
            if self.params.dry_run:
                return

            for chunk in chunks(self._buffer, self.params.write_buffer_size):
                success, errors, retries = flush(chunk)

                while(retries and nb_retries):
                    log.debug('RETRY BULK FLUSH for %s docs', len(retries))
                    success2, errors2, retries = flush(retries)
                    success +=success2
                    errors +=errors2
                    nb_retries -=1

                if errors:
                    raise_or_log(len(chunk), errors)

        finally:
            self._buffer = []

    def build_pk(self, data):

        self.params.pk = self.params.get('pk') or self.params.op_params

        if not self.params.pk:
            data['id'] = str(ObjectId())
            return data['id']

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

        return pk_data.concat_values(sep=':')

    def add_to_buffer(self, data, pk):
        action = slovar({
            '_index': self.klass.index,
            '_type': self.params.doc_type,
            '_id': pk,
        })

        #pop previous action key fields if any
        data.pop_many(action.keys())

        if self.params.op in ['create', 'index']:
            # if its create it will fail if document exists. To use this it should be handled in flush.
            # if its index it will either create or overwrite.
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

        self._buffer.append(action)
        return data

    def save(self, obj, data, pk):
        if self.params.remove_fields:
            data = data.extract(['-%s'%it for it in self.params.remove_fields])

        return self.add_to_buffer(data, pk)

    def delete(self, data):
        log.debug('DELETING %s', self.format4logging(data=data.to_dict()))
        self.add_to_buffer(slovar(), getattr(data, self.params.op_params[0]))

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

