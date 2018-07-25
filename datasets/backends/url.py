import logging
import os

import prf
from prf.utils.csv import dict2tab
import datasets


class URLBackend(object):
    def __init__(self, params):
        self.params = params
        auth_name = self.params.get('auth')

        if auth_name:
            auth = datasets.Settings.extract('auth.%s.*' % auth_name)
            if not auth:
                raise prf.exc.HTTPBadRequest('auth name `%s` is wrong or missing in config file' % auth_name)

        else:
            auth = slovar()
            auth['login'] = self.params.get('auth.login')
            auth['password'] = self.params.get('auth.password')
            auth['url'] = self.params.get('auth.url')
            auth.pop_by_values([None, ''])

        self.api = Request(self.params.url, _raise=True)

        if auth:
            self.api.login(**auth)

    def process_many(self, dataset):
        payload_wrapper = self.params.get('payload_wrapper')
        if payload_wrapper:
            dataset = dictset({payload_wrapper: dataset}).unflat()

        if isinstance(dataset, dict):
            self.process(dataset)
        else:
            for data in dataset:
                self.process(data)

    def process(self, data):
        if self.params.op == 'create':
            self.api.post(data=data)
        elif self.params.op == 'upsert':
            self.api.put(data=data)
