import logging
from pyramid.config import Configurator

from slovar import slovar
from slovar.strings import split_strip
from prf.utils import maybe_dotted, TODAY


log = logging.getLogger(__name__)
Settings = slovar()

def parse_ds(name):
    if isinstance(name, dict):
        return name

    parts = (name+'...').split('.')
    return slovar(
        backend = parts[0],
        ns = parts[1],
        name = parts[2],
    )

def get_ds(name):
    return get_dataset(parse_ds(name))

def name2be(name):
    return maybe_dotted('datasets.backends.%s.%sBackend' % (name, name.upper()))

def get_dataset(ds, define=False):
    return name2be(ds.backend).get_dataset(ds, define=define)

def get_dataset_meta(ds):
    return name2be(ds.backend).get_meta(ds.ns, ds.name)

def drop_dataset(ds):
    return name2be(ds.backend).drop_dataset(ds)

def drop_namespace(ds):
    return name2be(ds.backend).drop_namespace(ds.ns)

def get_transformers(params, logger=None, **tr_args):
    transformers = {}

    for call, trs in params.get('transformer', {}).items():
        transformers[call] = []
        for tr in split_strip(trs):
            trans, _, trans_as = tr.partition('__as__')

            if trans_as:
                tr_args['trans_as'] = trans_as

            tr_args.update(params.get('transformer_args', {}))
            transformers[call].append(maybe_dotted(trans)(logger=logger, **tr_args))

    return transformers

def main(global_config, **settings):
    global Settings
    Settings = slovar(config.registry.settings)

    config = Configurator(settings=settings)
    return config.make_wsgi_app()
