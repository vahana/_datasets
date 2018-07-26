from pyramid.config import Configurator

from prf.utils.dictset import dictset
from datasets.mongosets import get_mongo_dataset
from datasets.backends.http import prf_api

from datasets.backends import ES_BE_NAME

Settings = dictset()

def get_dataset(ds, ns=None, define=False):
    if isinstance(ds, dict):
        if ds.get('backend') == ES_BE_NAME:
            from prf.es import ES
            name = '%s.%s' % (ns,ds.name) if ns else ds.name
            return ES(name)

        elif ds.get('backend') in ['http', 'https'] :
            return prf_api(ds)

        else:
            return get_mongo_dataset(ds.name, ns=ns, define=define)
    else:
        return get_mongo_dataset(ds, ns=ns, define=define)


def main(global_config, **settings):
    global Settings
    Settings = dictset(config.registry.settings)

    config = Configurator(settings=settings)
    return config.make_wsgi_app()
