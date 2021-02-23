import os
from pymongo import IndexModel
import logging

import prf
from slovar.utils import maybe_dotted

from slovar import slovar
from datasets import get_dataset, get_dataset_meta, drop_namespace
from datasets.backends.mongo import define_datasets, registered_namespaces
from datasets.backends import BACKENDS, ESBackend, FSBackend, S3Backend

from prf.utils import urlencode, to_dunders
from prf.es import ES
import prf.exc as prf_exc

from prf.view import BaseView

log = logging.getLogger(__name__)


def includeme(config):
    config.include('datasets.backends.mongo')

    for name in BACKENDS.values():
        config.add_route(name, '%s/*path' % name)
        config.add_view(view='datasets.views.%sView' % (name.upper()),
                                        attr='dispatch', route_name=name, renderer='json')

    config.add_route('es_flat', 'es')
    config.add_view(view='datasets.views.ESFlatView', attr='index',
                    route_name='es_flat', renderer='json')


class BEView(BaseView):

    def dispatch(self, **kw):
        path = kw['path']
        self.be = self.request.matched_route.name
        ns = path[0]
        name = None

        if len(path) > 1:
            name = path[1]

        if self.request.method == 'DELETE':
            if name:
                return self.delete(ns, name)
            elif ns and ns != '_':
                return self.delete_many(ns)

        else:
            if ns == '_':
                result = self.backend()
            elif name is None:
                result = self.namespace(ns)
            else:
                if path[-1] == '_schema':
                    result = self.schema(ns, name)
                elif path[-1] == '_settings':
                    result = self.settings(ns, name)
                else:
                    result = self.collection(ns, name)

            return self._process(result, many=True)

    def schema(self, ns, name):
        raise NotImplementedError()

    def settings(self, ns, name):
        raise NotImplementedError()

    def backend(self):
        raise NotImplementedError()

    def namespace(self, ns):
        raise NotImplementedError()

    def collection(self, ns, name):
        klass = get_dataset(slovar(backend=self.be, ns=ns, name=name))
        return klass.get_collection(**self._params)

    def delete(self, ns, name):
        self._params.pop('_limit', None)
        dry = '_dry' in self._params; self._params.pop('_dry', None)

        klass = get_dataset(slovar(backend=self.be, ns=ns, name=name))

        if not self._params:
            msg = "`%s.%s` collection dropped" % (ns, name)
            if not dry:
                klass.drop_collection()
        else:
            count = klass.get_collection(**self._params, _count=1)
            msg = '`%s` documents deleted from `%s.%s` collection' % (count, ns, name)
            if not dry:
                klass.get_collection(**self._params).delete()

        if dry:
            msg = 'DRY RUN: ' + msg

        return prf.exc.HTTPOk(msg)

    def delete_many(self, ns):
        dry = '_dry' in self._params; self._params.pop('_dry', None)
        be = slovar(backend=self.be, ns=ns)

        if dry:
            return prf.exc.HTTPOk('DRY RUN: `%s` namespace dropped' % (be.ns))
        else:
            self.drop_ns(be)
            return prf.exc.HTTPOk('`%s` namespace dropped' % (be.ns))

    def drop_ns(self, be):
        drop_namespace(be)


class MONGOView(BEView):

    def schema(self, ns, name):
        define_datasets(ns)
        return get_dataset_meta(slovar(backend=self.be, ns=ns, name=name))

    def backend(self):
        base_url = self.request.current_route_url()[:-1]
        return sorted(['%s%s' % (base_url, x)
                        for x in registered_namespaces() if x != 'default'])

    def namespace(self, ns):
        match = self._params.get('match', '')

        base_url = self.request.current_route_url()
        datasets=[]

        for name in define_datasets(ns):
            if name.startswith(match):
                query = self._params.subset('-_limit,-_fields')
                url = '%s/%s' % (base_url, name)
                datasets.append(url)

        return sorted(datasets, key=lambda x: x[1] if query else x[0])


class ESView(BEView):
    def is_root_collection(self, name):
        return name in ESBackend.get_root_collections()

    def is_alias(self, name):
        return name in ESBackend.get_aliases()

    def backend(self):
        base_url = self.request.current_route_url()[:-1]

        nspaces = ['%s%s#' % (base_url, x) for x in ESBackend.get_namespaces()]
        root_collections = ['%s%s' % (base_url, x) for x in ESBackend.get_root_collections()]
        aliases = ['%s%s' % (base_url, x) for x in ESBackend.get_aliases()]

        return sorted(nspaces) + sorted(root_collections) + sorted(aliases)

    def namespace(self, ns):
        match = self._params.get('match', '')
        base_url = self.request.current_route_url()
        datasets=[]

        if self.is_root_collection(ns) or self.is_alias(ns):
            return self.collection('', ns)

        index = '%s.*' % ns
        for name in ESBackend.get_collections(match=index).keys():
            if name.startswith(match):
                name = name.split('.')[1]
                url = '%s/%s' % (base_url, name)
                datasets.append(url)

        return sorted(datasets)

    def index_name(self, ns, name):
        return '%s.%s' % (ns, name) if ns else name

    def schema(self, ns, name):
        index_name = self.index_name(ns, name)

        if self.request.method == 'PUT':
            mapping_body = maybe_dotted(self._params.mapping)()
            ES.put_mapping(
                index=index_name,
                doc_type=self._params.doc_type,
                body=mapping_body)

        return ES.get_meta(index_name, command='get_mapping')

    def settings(self, ns, name):
        return ES.get_meta(self.index_name(ns, name), command='get_settings')


class ESFlatView(BaseView):
    'Show all available indexes combined into single dataset'
    def index(self, **kw):
        klass = get_dataset(
                    slovar(backend='es',
                            ns='',
                            name=self._params.get('_index', '*')))

        results = self._process(klass.get_collection(**self._params), many=True)
        if '_meta' in results:
            results['_meta']['indices'] = sorted(list(results['_meta'].pop('alias', {}).keys())[:50])
        return results


class FSView(BEView):

    def dispatch(self, **kw):
        path = kw['path']
        os_path = os.path.join(*path)

        self.be = self.request.matched_route.name
        ns = path[0]
        name = None

        if len(path) > 1:
            name = path[-1]
            ns = os.path.join(*path[:-1])

        if self.request.method == 'DELETE':
            if name:
                return self.delete(ns, name)
            elif ns and ns != '_':
                return self.delete_many(ns)

        if ns == '_':
            result = self.backend()

        elif FSBackend.is_ns(os_path):
            result = self.namespace(os_path)

        else:
            result = self.collection(ns, name)

        return self._process(result, many=True)

    def backend(self):
        base_url = self.request.current_route_url()[:-1]
        return sorted(['%s%s' % (base_url, x)
                       for x in FSBackend.ls_namespaces()])

    def namespace(self, ns):
        match = self._params.get('match', '')
        _flat = '_flat' in self._params

        base_url = self.request.current_route_url().split('?')[0]

        datasets = []

        for name in FSBackend.ls_ns(ns, _flat):
            if name.startswith(match):
                query = self._params.subset('-_limit,-_fields')
                url = '%s/%s' % (base_url, name)
                datasets.append(url)

        return sorted(datasets,
                      key=lambda x: x[1] if query else x[0])

class CSVView(BEView):

    def dispatch(self, **kw):
        path = kw['path']
        os_path = os.path.join(*path)

        self.be = self.request.matched_route.name
        ns = path[0]
        name = None

        if len(path) > 1:
            name = path[-1]
            ns = os.path.join(*path[:-1])

        if self.request.method == 'DELETE':
            if name:
                return self.delete(ns, name)
            elif ns and ns != '_':
                return self.delete_many(ns)

        if ns == '_':
            result = self.backend()

        elif CSVBackend.is_ns(os_path):
            result = self.namespace(os_path)

        else:
            result = self.collection(ns, name)

        return self._process(result, many=True)

    def backend(self):
        base_url = self.request.current_route_url()[:-1]
        return sorted(['%s%s' % (base_url, x)
                        for x in CSVBackend.ls_namespaces()])

    def namespace(self, ns):
        match = self._params.get('match', '')
        _flat = '_flat' in self._params

        base_url = self.request.current_route_url().split('?')[0]

        datasets=[]

        for name in CSVBackend.ls_ns(ns, _flat):
            if name.startswith(match):
                query = self._params.subset('-_limit,-_fields')
                url = '%s/%s' % (base_url, name)
                datasets.append(url)

        return sorted(datasets,
                        key=lambda x: x[1] if query else x[0])


class S3View(CSVView):
    def dispatch(self, **kw):
        path = kw['path']
        self.be = self.request.matched_route.name

        ns = path[0]
        name = None

        if len(path) > 1:
            name = path[-1]
            ns = os.path.join(*path[:-1])

        if self.request.method == 'DELETE':
            if name:
                return self.delete(ns, name)
            elif ns and ns != '_':
                return self.delete_many(ns)

        self.be = self.request.matched_route.name

        if ns == '_':
            result = self.backend()

        elif S3Backend.is_ns(path):
            result = self.namespace(path)

        else:
            result = self.collection('/'.join(path[:-1]), path[-1])

        return self._process(result, many=True)

    def backend(self):
        base_url = self.request.current_route_url()[:-1]
        return sorted(['%s%s' % (base_url, x)
                        for x in S3Backend.ls_buckets()])

    def namespace(self, ns):
        match = self._params.get('match', '')
        _flat = '_flat' in self._params

        base_url = self.request.current_route_url().split('?')[0]
        datasets=[]

        for name in S3Backend.ls_bucket(ns, _flat):
            if name.startswith(match):
                url = '%s/%s' % (base_url, name)
                datasets.append(url)

        return sorted(datasets, key=lambda x: x[0])


class HTTPView(BEView):
    '''
        In case of http backend, the typical urls are:
        api/http/?_url=http://example.org&other_params=blabla
        api/http/root_name_space?_url=http://example.org&blablabla

        GET `api/http?_url=example.org`
        returns: `{data: [{x:1}, {x:2},..]}`

        GET `api/http/data?_url=example.org`
        returns: `[{x=1}, {x=2},...]`

        the `data` is the root namespace

    '''

    def dispatch(self, **kw):
        # in the format of {be}.{ns}.{name} the name is useless when be=http. so lets assign `NA`.
        # ns could be passed from a client, but if not, lets make sure it too is `NA` so things dont fail downstream.
        kw['path'] = list(kw['path']) + ((2 - len(kw['path']))* ['NA'])
        return super().dispatch(**kw)

    def backend(self):
        raise prf_exc.HTTPBadRequest('HTTP backend requires `_url` param to fetch data from')

    def namespace(self, ns):
        raise prf_exc.HTTPBadRequest('HTTP backend requires `_url` param to fetch data from')

