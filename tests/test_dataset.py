import mock
from .base import BaseTestCase
import prf.mongodb

import datasets

from prf.mongodb import get_document_cls

from datasets import (
    get_dataset_names, define_document, load_documents, get_document,
    set_document, get_namespaces, namespace_storage_module, get_document_meta,
    connect_dataset_aliases, get_or_define_document
)


class TestDataset(BaseTestCase):
    def setUp(self):
        super(TestDataset, self).setUp()
        self.drop_databases()
        self.unload_documents()

    def test_get_document_cls(self):
        cls = self.create_collection('default', 'col1')
        cls2 = self.create_collection('prf-test2', 'col2')
        cls3 = self.create_collection('default', 'col3')
        cls4 = self.create_collection('prf-test2', 'col3')

        dcls = get_document_cls('col1')
        dcls2 = get_document_cls('col2')
        dcls3 = get_document_cls('col3')
        assert cls == dcls
        assert cls2 == dcls2
        assert dcls2._meta['db_alias'] == 'prf-test2'
        # This is broken behavior with collision on collection names across dbs,
        # get_document_cls will return the most recently defined class with that name.
        assert dcls3 == cls4

    @mock.patch('prf.mongodb.mongo_connect')
    def test_connect_dataset_aliases_missing_config(self, connect):
        del self.conf.registry.settings['dataset.ns']
        connect_dataset_aliases(self.conf)
        connect.assert_not_called()

    def test_get_namespaces(self):
        assert get_namespaces() == ['default', 'prf-test2', '2prf-test3']

    def test_get_dataset_names(self):
        self.create_collection('default', 'col1')
        self.create_collection('default', 'col2')
        self.create_collection('prf-test2', 'col3')
        assert get_dataset_names() == [
            ['default', 'col1', 'col1'],
            ['default', 'col2', 'col2'],
            ['prf-test2', 'col3', 'col3'],
        ]

    def test_load_documents(self):
        self.create_collection('default', 'col1')
        self.create_collection('prf-test2', 'col2')
        load_documents()
        assert hasattr(datasets, 'default')
        assert hasattr(datasets.default, 'col1')
        assert hasattr(datasets, 'prftest2')
        assert hasattr(datasets.prftest2, 'col2')

    def test_get_document_raises(self):
        self.assertRaises(AttributeError, lambda: get_document('default', 'col1'))
        cls = get_document('default', 'col1', _raise=False)
        assert cls is None

    def test_get_document(self):
        self.create_collection('prf-test2', 'col1')
        load_documents()
        cls = get_document('prf-test2', 'col1')
        assert cls is not None

    def test_namespace_storage_module_raises(self):
        self.assertRaises(
            AttributeError,
            lambda: namespace_storage_module('namespace_storage_module', _set=True)
        )

    def test_getattr_document(self):
        self.create_collection('prf-test2', 'col1')
        load_documents()
        d = datasets.prftest2.col1
        assert d._meta['db_alias'] == 'prf-test2'

    def test_get_document_meta(self):
        assert not get_document_meta('default', 'col1')
        self.create_collection('default', 'col1')
        meta = get_document_meta('default', 'col1')
        assert meta['db_alias'] == 'default'
        assert meta['_cls'] == 'col1'
        assert meta['collection'] == 'col1'

    def test_define_document(self):
        cls = self.create_collection('default', 'col1')
        _d = define_document('col1', namespace='default')
        assert cls != _d
        assert _d._meta['db_alias'] == 'default'

        set_document('default', 'col1', cls)
        _d = define_document('col1', namespace='default')
        assert cls == _d

        _d = define_document('col1', namespace='default', redefine=True)
        assert cls != _d

        set_document('abc', 'xyz',cls)
        _d = define_document('abc.xyz')
        assert cls  == _d

    def test_get_or_define_document(self):
        a = define_document('something', namespace='impala-test2')
        set_document('impala-test2', 'something', a)
        b = get_or_define_document('impala-test2.something')
        self.assertEqual(a.__name__, b.__name__)
        self.assertEqual(a._meta['db_alias'], 'impala-test2')
        self.assertEqual(b._meta['db_alias'], 'impala-test2')

    def test_get_or_define_document_no_namespace(self):
        self.assertRaises(Exception, lambda: get_or_define_document('.something'))

    def test_read_unknown_namespace(self):
        self.assertRaises(AttributeError, lambda: get_document('prf-test-notthere', 'something'))
        self.assertRaises(AttributeError, lambda: get_or_define_document('prf-test-notthere.something', _raise=True))

    def test_write_unknown_namespace(self):
        cls = get_or_define_document('prf-test-notthere.something', define=True)
        self.assertEqual(cls.__name__, 'something')
        cls(a='1').save()
        objs = cls.objects.all()
        self.assertEqual(len(objs), 1)
        self.assertEqual(objs[0].a, '1')
