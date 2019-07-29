import logging
from pyramid.config import Configurator

from slovar import slovar
from prf.utils import maybe_dotted
from prf.utils import TODAY


log = logging.getLogger(__name__)
Settings = slovar()

def parse_ds(name):
    if not name:
        return {}

    if '%TODAY%' in name:
        name = name.replace('%TODAY%', TODAY())

    if not name:
        return slovar()

    params = slovar()
    parts = name.split('.')

    if parts[0] in ['mongo', 'es']:
        params.backend = parts.pop(0)
    else:
        params.backend = 'mongo'

    if parts:
        params.ns = parts.pop(0)
        if parts:
            params.name = parts[0]
        elif params.backend == 'es':
            params.name = params.ns
            params.ns = ''

    if params.backend == 'es':
        params.name = params.name.lower()

    elif params.backend == 'mongo':
        if not params.ns:
            raise ValueError('missing mongo collection name')

    return params


def get_ds(name):
    return get_dataset(parse_ds(name))

def name2be(name):
    return maybe_dotted('datasets.backends.%s.%sBackend' % (name, name.upper()))

def get_dataset(ds, define=False):
    return name2be(ds.backend).get_dataset(ds)

def get_dataset_meta(ds):
    return name2be(ds.backend).get_meta(ds.ns, ds.name)

def drop_dataset(ds):
    return name2be(ds.backend).drop_dataset(ds)

def drop_namespace(ds):
    return name2be(ds.backend).drop_namespace(ds.ns)

def get_transformers(params, logger=None, **tr_args):
    transformers = []

    for tr in params.aslist('transformer', default=[]):
        trans, _, trans_as = tr.partition('__as__')

        if trans_as:
            tr_args['trans_as'] = trans_as

        tr_args.update(params.get('transformer_args', {}))

        transformers.append(maybe_dotted(trans)(logger=logger, **tr_args))

    return transformers

def main(global_config, **settings):
    global Settings
    Settings = slovar(config.registry.settings)

    config = Configurator(settings=settings)
    return config.make_wsgi_app()
