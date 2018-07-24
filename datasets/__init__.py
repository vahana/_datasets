from pyramid.config import Configurator

from prf.utils.dictset import dictset
from .mongosets import *
from .mongosets import get_dataset as mongo_get_dataset

Settings = dictset()
ES_BE_NAME = '__es'
URL_BE_NAME = '__url'
CSV_BE_NAME = '__csv'

def get_dataset(ds, ns=None, define=False):
    if isinstance(ds, dict):
        if ds.get('backend') == ES_BE_NAME:
            from prf.es import ES
            name = '%s.%s' % (ns,ds.name) if ns else ds.name
            return ES(name)
        else:
            return mongo_get_dataset(ds.name, ns=ns, define=define)
    else:
        return mongo_get_dataset(ds, ns=ns, define=define)


def main(global_config, **settings):
    global Settings
    Settings = dictset(config.registry.settings)

    config = Configurator(settings=settings)
    return config.make_wsgi_app()
