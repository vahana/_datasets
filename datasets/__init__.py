import logging
from pyramid.config import Configurator

from slovar import slovar
from prf.utils import maybe_dotted

from datasets.backends.http import prf_api
from datasets.backends.csv import CSVBackend
from datasets.backends.mongo import MongoBackend
from datasets.backends.es import ESBackend

from datasets.backends import ES_BE_NAME, MONGO_BE_NAME, CSV_BE_NAME


log = logging.getLogger(__name__)
Settings = slovar()

def get_ds(name, backend=MONGO_BE_NAME):
    if '.' in name:
        ns, _, name = name.partition('.')
    elif '/' in name:
        ns, _, name = name.partition('/')

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

def get_dataset_meta(ds):
    if ds.backend == MONGO_BE_NAME:
        return MongoBackend.get_meta(ds.ns, ds.name)

    elif backend == ES_BE_NAME:
        return ESBackend.get_meta(ds.ns, ds.name)

    else:
        raise ValueError('Backend `%s` is not supported for meta info' % ds)

def drop_dataset(ds):
    if ds.backend == MONGO_BE_NAME:
        return MongoBackend.drop_dataset(ds)

    elif backend == ES_BE_NAME:
        return ESBackend.drop_index(ds)

    else:
        raise ValueError('Backend `%s` is not supported for dropping' % ds)


def drop_namespace(ds):
    if ds.backend == MONGO_BE_NAME:
        return MongoBackend.drop_namespace(ds.ns)

    elif ds.backend == ES_BE_NAME:
        return ESBackend.drop_namespace(ds.ns)

    else:
        raise ValueError('Backend `%s` is not supported for dropping' % ds)


def get_transformer(params, **tr_args):
    if params.get('transformer'):
        trans, _, trans_as = params.transformer.partition('__as__')

        if trans_as:
            tr_args['trans_as'] = trans_as

        tr_args.update(params.get('transformer_args', {}))

        return maybe_dotted(trans)(**tr_args)


def main(global_config, **settings):
    global Settings
    Settings = slovar(config.registry.settings)

    config = Configurator(settings=settings)
    return config.make_wsgi_app()
