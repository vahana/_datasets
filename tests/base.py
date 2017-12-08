import os
import unittest
from pyramid.config import Configurator
from pyramid.paster import get_appsettings

import prf
from prf.mongodb import DynamicBase

import datasets
from datasets import define_document, get_namespaces

class BaseTestCase(unittest.TestCase):
    def setUp(self):
        test_ini_file = os.environ.get('INI_FILE', 'test.ini')
        settings = get_appsettings(test_ini_file, name='main')
        self.conf = Configurator(settings=settings)
        prf.includeme(self.conf)
        prf.mongodb.includeme(self.conf)
        datasets.includeme(self.conf)

    def drop_databases(self):
        c = prf.mongodb.mongo.connection.get_connection()
        c.drop_database(self.conf.registry.settings.get('mongodb.db'))
        c.drop_database('prf-test-notthere')
        for namespace in get_namespaces():
            if namespace != 'default':
                c.drop_database(namespace)

    def unload_documents(self):
        for ns in ['default', 'prftest2']:
            if hasattr(datasets, ns):
                delattr(datasets, ns)

    def create_collection(self, namespace, name):
        cls = define_document(name, namespace=namespace)
        # Create a document and delete it to make sure the collection exists
        cls(name='hello').save().delete()
        return cls
