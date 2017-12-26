from pyramid.config import Configurator

from prf.utils.dictset import dictset
from .mongosets import *

Settings = dictset()

def main(global_config, **settings):
    global Settings
    Settings = dictset(config.registry.settings)

    config = Configurator(settings=settings)
    return config.make_wsgi_app()
