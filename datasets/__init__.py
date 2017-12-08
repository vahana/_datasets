from pyramid.config import Configurator

from .mongosets import *

def main(global_config, **settings):
    config = Configurator(settings=settings)
    return config.make_wsgi_app()
