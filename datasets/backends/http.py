import logging
import os
import urllib3

from slovar import slovar

import prf
from prf.request import PRFRequest

import datasets

def auth_url(params):
    auth = slovar()
    auth_param = params.get('auth')
    if isinstance(auth_param, str):
        if '@' in auth_param:
            return urllib3.util.parse_url(auth_param).url

        auth = datasets.Settings.extract('%s.*' % auth_param)
        if not auth:
            raise prf.exc.HTTPBadRequest('auth name `%s` is wrong or missing in config file' % auth_param)

    else: # passed as separate fields
        auth.login = params.get('auth.login')
        auth.password = params.get('auth.password')
        auth.url = params.get('auth.url')

    auth.pop_by_values([None, ''])

    if auth:
        parts = urllib3.util.parse_url(auth.url)
        return parts._replace(auth='%s:%s' % (auth.login,auth.password)).url


class prf_api(object):
    def __init__(self, params):
        self.api = PRFRequest(params.name, auth=auth_url(params), _raise=True)

    def get_collection(self, **params):
        resp = self.api.get(params=params)
        return self.api.get_data(resp)

    def get_collection_paged(self, page_size, **params):
        for resp in self.api.get_paginated(page_size, params=params):
            yield self.api.get_data(resp)


class HTTPBackend(object):
    def __init__(self, params):
        self.params = params
        self.api = PRFRequest(params.name, auth=auth_url(params), _raise=True)

    def process_many(self, dataset):
        payload_wrapper = self.params.get('payload_wrapper')
        if payload_wrapper:
            dataset = slovar({payload_wrapper: dataset}).unflat()

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
