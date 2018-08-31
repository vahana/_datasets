import logging
from pyramid.config import Configurator

from slovar import slovar
from datasets.backends.http import prf_api
from datasets.backends.csv import CSVBackend
from datasets.backends.mongo import MongoBackend
from datasets.backends.es import ESBackend

from datasets.backends import ES_BE_NAME, MONGO_BE_NAME, CSV_BE_NAME


log = logging.getLogger(__name__)
Settings = slovar()

def get_ds(name, backend=MONGO_BE_NAME):
    ns, _, name = name.partition('.')
    return get_dataset(slovar(name=name, ns=ns, backend=backend))

def get_dataset(ds, define=False):
    ds.setdefault('backend', MONGO_BE_NAME)

    if ds.get('backend') == MONGO_BE_NAME:
        return MongoBackend.get_dataset(ds, define=define)

    elif ds.get('backend') == ES_BE_NAME:
        return ESBackend.get_dataset(ds, define=define)

    elif ds.get('backend') in ['http', 'https'] :
        return prf_api(ds)

    elif ds.get('backend') == CSV_BE_NAME:
        return CSVBackend.get_dataset(ds)

    else:
        raise ValueError('Unknown backend in `%s`' % ds)

def get_document_meta(backend, ns, name):
    if backend == MONGO_BE_NAME:
        return MongoBackend.get_meta(ns, name)

    elif backend == ES_BE_NAME:
        return ESBackend.get_meta(ns, name)

    else:
        raise ValueError('Unknown backend in `%s`' % ds)


def main(global_config, **settings):
    global Settings
    Settings = slovar(config.registry.settings)

    config = Configurator(settings=settings)
    return config.make_wsgi_app()
