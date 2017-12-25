from types import ModuleType
import re
import sys
import logging

import mongoengine as mongo
from datetime import datetime
from bson import ObjectId
import datetime

import prf
from prf.mongodb import DynamicBase, mongo_connect
from prf.utils import dictset, maybe_dotted
from prf.utils.qs import prep_params

import datasets

log = logging.getLogger(__name__)
DATASET_MODULE_NAME = 'datasets.mongosets'
DATASET_NAMES_MAP = {}


def includeme(config):
    config.add_directive('dataset_namespaces', dataset_namespaces)
    set_dataset_module(config.prf_settings().get('dataset.module'))
    connect_dataset_aliases(config)


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
            cls._meta['db_alias'] = DATASET_NAMES_MAP[ns]
            try:
                from mongoengine.connections_manager import ConnectionManager
                cls._collection = ConnectionManager.get_collection(cls)
            except ImportError:
                cls._collection = None
        return cls


def set_dataset_module(name):
    if not name:
        raise ValueError('Missing configuration option: dataset.module')
    # Just make sure the module gets loaded
    maybe_dotted(name)
    setattr(datasets.mongosets, 'DATASET_MODULE_NAME', name)


def connect_namespace(settings, namespace):
    connect_settings = settings.update({
        'mongodb.alias': namespace,
        'mongodb.db': namespace
    })
    mongo_connect(connect_settings)


def connect_dataset_aliases(config):
    ds = config.dataset_namespaces()
    if len(ds) == 1 and ds[0] == 'auto':
        ds = [str(x) for x in mongo.connection.get_connection().database_names()]
    for namespace in ds:
        connect_namespace(config.prf_settings(), namespace)


def dataset_namespaces(config):
    return (config.prf_settings().aslist('dataset.namespaces', '')
            or config.prf_settings().aslist('dataset.ns', ''))


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


def get_document_meta(namespace, doc_name):
    db = mongo.connection.get_db(namespace)

    if doc_name not in db.collection_names():
        return dictset()

    meta = dictset(
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

        indexes.append(dictset({
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

def load_documents():
    names = get_dataset_names()
    _namespaces = set()

    for namespace, _, _cls in names:
        # log.debug('Registering collection %s.%s', namespace, _cls)
        doc = define_document(_cls, namespace=namespace)
        set_document(namespace, _cls, doc)
        _namespaces.add(namespace)

    log.debug('Loaded namespaces: %s', list(_namespaces))

def safe_name(name):
    # See https://stackoverflow.com/questions/3303312/how-do-i-convert-a-string-to-a-valid-variable-name-in-python
    # Remove invalid characters
    cleaned = re.sub('[^0-9a-zA-Z_]', '', name)
    # Remove leading characters until we find a letter or underscore
    return str(re.sub('^[^a-zA-Z_]+', '', cleaned))


def namespace_storage_module(namespace, _set=False):
    safe_namespace = safe_name(namespace)
    if not safe_namespace:
        raise Exception('A namespace name is required')
    datasets_module = sys.modules[DATASET_MODULE_NAME]
    if _set:
        # If we're requesting to set and the target exists but isn't a dataset storage module
        # then we're reasonably sure we're doing something wrong
        if hasattr(datasets_module, safe_namespace):
            if not isinstance(getattr(datasets_module, safe_namespace),
                                                                DatasetStorageModule):
                    raise AttributeError('%s.%s already exists, not overriding.' % (
                                                    DATASET_MODULE_NAME, safe_namespace))
        else:
            DATASET_NAMES_MAP[safe_namespace] = namespace
            setattr(datasets_module, safe_namespace, DatasetStorageModule(safe_namespace))
    return getattr(datasets_module, safe_namespace, None)


def get_document(namespace, name, _raise=True):
    namespace_module = namespace_storage_module(namespace)
    cls_name = safe_name(name)
    doc_class = None
    try:
        doc_class = getattr(namespace_module, cls_name)
    except AttributeError:
        if _raise:
            raise AttributeError('Collection %s.%s doesn\'t exist' % (namespace, name))
    return doc_class


def set_document(namespace, name, cls):
    namespace_module = namespace_storage_module(namespace, _set=True)
    ns = safe_name(name)
    setattr(namespace_module, ns, cls)


def unset_document(cls):
    namespace_module = namespace_storage_module(cls._ns, _set=True)
    if hasattr(namespace_module, cls.__name__):
        delattr(namespace_module, cls.__name__)


def get_or_define_document(name, _raise=False, define=False):
    try:
        namespace, name = name.split('.')
    except ValueError: #raises this if return is not 2 items - missing namespace
        namespace = None

    kls = get_document(namespace, name, _raise=_raise)
    if not kls and define:
        # Make sure we set the connection if this is a new namespace
        if namespace not in get_dataset_names():
            connect_namespace(prf.Settings, namespace)
        kls = define_document(name, namespace=namespace)
        set_document(namespace, name, kls)

    return kls
