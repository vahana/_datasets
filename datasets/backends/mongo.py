from types import ModuleType
import re
import sys
import logging

import mongoengine as mongo
from datetime import datetime
from bson import ObjectId
import datetime
from pprint import pformat

from slovar import slovar
import prf
from prf.mongodb import DynamicBase, mongo_connect, mongo_disconnect, drop_db
from prf.utils import maybe_dotted, typecast, str2dt, to_dunders

import datasets

log = logging.getLogger(__name__)

import datasets
from datasets.backends.base import Base


class MongoBackend(Base):
    @classmethod
    def get_dataset(cls, ds, define=False):

        ds = cls.process_ds(ds)

        if define:
            _raise = False

        connect_namespace(datasets.Settings, ds.ns)
        kls = define_document(ds.name, namespace=ds.ns, redefine=define)
        set_document(ds.ns, ds.name, kls)

        return kls

    @classmethod
    def get_meta(cls, ns, name):
        return get_dataset_meta(ns, name)

    @classmethod
    def drop_dataset(cls, ds):
        ds = cls.get_dataset(ds)
        if ds:
            ds.drop_collection()

    @classmethod
    def drop_namespace(cls, ns):
        drop_db(ns)

    def log_action(self, data, query, action):
        msg = '%s with query=%s\n%s' % (action.upper(), query, self.format4logging(data=data))
        if self.params.dry_run:
            log.warning('DRY RUN: %s' % msg)
        else:
            log.debug(msg)

    def create(self, data):
        if 'skip_by' in self.params:
            _params = self.build_query_params(data, self.params.skip_by)
            _params['_count'] = 1
            exists = self.klass.get_collection(**_params)
            if exists:
                log.debug('SKIP creation: %s objects exists with: %s', exists, _params)
                return

        data['original_id'] = data.pop('id', None) # pop the id so mongo creates a new document
        data = self.pre_save(data)

        obj = self.klass()
        for name, val in list(data.items()):
            setattr(obj, name, val)

        if not self.params.dry_run:
            if self.params.fail_on_error:
                obj.save()
            else:
                obj.save_safe()

        self.log_action(data, None, 'create')

    def update(self, data):
        params, objects = self.get_objects(self.params.op_params, data)

        if not objects:
            self.log_action({}, params, 'update')
            self.log_not_found(params, data)
            return

        self.update_objects(params, objects, data)

    def upsert(self, data):
        params, objects = self.get_objects(self.params.op_params, data)

        if objects:
            #udpate_fields allows to update only partially if data exists
            if self.params.update_fields:
                data = data.extract(self.params.update_fields)

            self.update_objects(params, objects, data)

        else:
            self.create(data)
            self.log_action(data, None, 'create')

    def delete(self, data):
        params, objects = self.get_objects(self.params.op_params, data)

        if not objects:
            self.log_action({}, params, 'delete')
            self.log_not_found(params, data)
            return

        if not self.params.dry_run:
            objects.delete()

        self.log_action(data, params, 'delete')

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

    def get_objects(self, keys, data):
        _params = self.build_query_params(data, keys)
        if 'query' in self.params:
            _params = _params.update_with(typecast(self.params.query))

        return _params, self.klass.get_collection(**_params.flat())

    def update_objects(self, params, objects, data):
        update_count = len(objects)

        def do_update(obj, data):
            data = self.pre_save(data)
            data.pop('id', None);data.pop('_id', None)
            update = to_dunders(data)

            for rf in self.params.remove_fields:
                update['unset__%s' % rf] = 1

            if not update:
                log.debug('NOTHING TO UPDATE')

            elif not self.params.dry_run:
                updated = obj.update(**update)

            self.log_action(update, params, 'update')

        if update_count > 1:
            msg = 'Multiple (%s) updates for\n%s' % (update_count,
                                                     self.format4logging(query=params))
            self.job_logger.warning(msg)


        actions = self.params.extract(
                    ['overwrite', 'append_to', 'append_to_set', 'flatten'])

        if actions or self.transformer:
            for each in objects:
                each_d = each.to_dict()
                if self.transformer:
                    actions = self.transformer.pre_update(data, each_d) or actions
                    log.debug('ACTIONS:\n%s', pformat(actions))

                do_update(each, each_d.update_with(data, **actions))
        else:
            do_update(objects, data)


def includeme(config):
    datasets.Settings = slovar(config.registry.settings)

class DSDocumentBase(DynamicBase):
    meta = {'abstract': True}
    _ns = None

    @classmethod
    def unregister(cls):
        super(DSDocumentBase, cls).unregister()
        unset_document(cls)

    @classmethod
    def drop_ds(cls):
        cls.drop_collection()
        cls.unregister()

class DatasetStorageModule(ModuleType):
    def __getattribute__(self, attr, *args, **kwargs):
        ns = ModuleType.__getattribute__(self, '__name__')
        cls = ModuleType.__getattribute__(self, attr, *args, **kwargs)
        if isinstance(cls, (type)) and issubclass(cls, DynamicBase):
            cls._meta['db_alias'] = ns
            try:
                from mongoengine.connections_manager import ConnectionManager
                cls._collection = ConnectionManager.get_collection(cls)
            except ImportError:
                cls._collection = None
        return cls


def connect_namespace(settings, namespace):
    connect_settings = settings.update({
        'mongodb.alias': namespace,
        'mongodb.db': namespace
    })
    mongo_connect(connect_settings)


def registered_namespaces(settings):
    ns = settings.aslist('dataset.namespaces', '') \
        or settings.aslist('dataset.ns', '') \

    if ns[0] == '*':
        return mongo.connection.get_connection().database_names()

    else:
        return ns

def connect_dataset_aliases(settings, aliases=None, reconnect=False):

    aliases = aliases or registered_namespaces(settings)
    for alias in aliases:
        if reconnect:
            mongo_disconnect(alias)
        connect_namespace(settings, alias)


def get_dataset_names(match_name="", match_namespace=""):
    """
    Get dataset names, matching `match` pattern if supplied, restricted to `only_namespace` if supplied
    """
    namespaces = get_namespaces()
    names = []
    for namespace in namespaces:
        if match_namespace and match_namespace == namespace or not match_namespace:
            db = mongo.connection.get_db(namespace)
            for name in db.collection_names():
                if match_name in name.lower() and not name.startswith('system.'):
                    names.append([namespace, name, name])
    return names


def get_namespaces():
    # Mongoengine stores connections as a dict {alias: connection}
    # Getting the keys is the list of aliases (or namespaces) we're connected to
    return list(mongo.connection._connections.keys())


def get_dataset_meta(namespace, doc_name):
    db = mongo.connection.get_db(namespace)

    if doc_name not in db.collection_names():
        return slovar()

    meta = slovar(
        _cls=doc_name,
        collection=doc_name,
        db_alias=namespace,
    )

    indexes = []
    for ix_name, index in list(db[doc_name].index_information().items()):
        fields = [
            '%s%s' % (('-' if order == -1 else ''), doc_name)
            for (doc_name, order) in index['key']
        ]

        indexes.append(slovar({
            'name': ix_name,
            'fields':fields,
            'unique': index.get('unique', False)
        }))

    meta['indexes'] = indexes

    return meta


# TODO Check how this method is used and see if it can call set_document
def define_document(name, meta=None, namespace='default', redefine=False,
                    base_class=None):

    if not name:
        raise ValueError('Document class name can not be empty')
    name = str(name)

    if '.' in name:
        namespace, _,name = name.partition('.')

    meta = meta or {}
    meta['collection'] = name
    meta['ordering'] = ['-id']
    meta['db_alias'] = namespace

    base_class = maybe_dotted(base_class or DSDocumentBase)

    if redefine:
        kls = type(name, (base_class,), {'meta': meta})

    else:
        try:
            kls = get_document(namespace, name)
        except AttributeError:
            kls = type(name, (base_class,), {'meta': meta})

    kls._ns = namespace
    return kls


def define_datasets(namespace):
    connect_namespace(datasets.Settings, namespace)
    db = mongo.connection.get_db(namespace)

    dsets = []
    for name in db.collection_names():
        dsets.append(name)

    return dsets


def load_documents():
    names = get_dataset_names()
    _namespaces = set()

    for namespace, _, _cls in names:
        # log.debug('Registering collection %s.%s', namespace, _cls)
        doc = define_document(_cls, namespace=namespace)
        set_document(namespace, _cls, doc)
        _namespaces.add(namespace)

    log.debug('Loaded namespaces: %s', list(_namespaces))

def namespace_storage_module(namespace, _set=False):
    if not namespace:
        raise Exception('A namespace name is required')
    datasets_module = sys.modules[__name__]
    if _set:
        # If we're requesting to set and the target exists but isn't a dataset storage module
        # then we're reasonably sure we're doing something wrong
        if hasattr(datasets_module, namespace):
            if not isinstance(getattr(datasets_module, namespace),
                                                                DatasetStorageModule):
                    raise AttributeError('%s.%s already exists, not overriding.' % (
                                                    __name__, namespace))
        else:
            setattr(datasets_module, namespace, DatasetStorageModule(namespace))
    return getattr(datasets_module, namespace, None)


def set_document(namespace, name, cls):
    namespace_module = namespace_storage_module(namespace, _set=True)
    ns = name or ''
    setattr(namespace_module, ns, cls)


def unset_document(cls):
    namespace_module = namespace_storage_module(cls._ns, _set=True)
    if hasattr(namespace_module, cls.__name__):
        delattr(namespace_module, cls.__name__)


def get_document(namespace, name, _raise=True):
    if namespace is None:
        namespace, _, name = name.rpartition('.')

    namespace_module = namespace_storage_module(namespace)
    doc_class = None
    try:
        doc_class = getattr(namespace_module, name)
    except AttributeError:
        if _raise:
            raise AttributeError('Collection %s.%s doesn\'t exist' % (namespace, name))
    return doc_class


def get_or_define_document(name, define=False):
    namespace, _, name = name.rpartition('.')

    kls = get_document(namespace, name, _raise=False)
    if not kls and define:
        connect_namespace(datasets.Settings, namespace)
        kls = define_document(name, namespace=namespace, redefine=True)
        set_document(namespace, name, kls)

    return kls


def get_mongo_dataset(name, ns=None, define=False):

    if not ns:
        ns, _, name = name.rpartition('.')

    if define:
        _raise = False

    connect_namespace(datasets.Settings, ns)
    kls = define_document(name, namespace=ns, redefine=define)
    set_document(ns, name, kls)

    return kls
