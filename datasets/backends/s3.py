import logging
import os
import boto3
import botocore
import io

import prf
import prf.exc as prf_exc
from prf.utils import maybe_dotted
from datasets.backends.base import Base
from prf.utils import dict2tab
from prf.s3 import S3

log = logging.getLogger(__name__)


def dict2bucket(params):
    path = params.ns.split('/') + [params.name]
    return path[0], '/'.join(path[1:]) or '/'


def Bucket(name):
    s3 = boto3.resource('s3')
    return s3.Bucket(name)


class S3Backend(Base):

    def __init__(self, params, job_log):
        super().__init__(params, job_log)

        fields = []

        if not self.params.get('fields'):
            fields = maybe_dotted(self.params.get('fields_file'), throw=False)
            if not fields:
                log.warning('Missing fields or fields_file. Will attempt to auto-detect from source')

            if fields and not isinstance(fields, list):
                raise prf.exc.HTTPBadRequest('Expecting list object in fields_file. Got %s' % fields)

            self.params.fields = fields

    @classmethod
    def ls_buckets(cls):
        return sorted([it.name for it in boto3.resource('s3').buckets.all()])

    @classmethod
    def ls_bucket(cls, path, flat=False):
        folders = []
        files = [] # only by extension

        bucket_name = path[0]
        prefix = '/'.join(path[1:])
        if prefix: prefix += '/'

        for obj in Bucket(bucket_name).objects.filter(Prefix=prefix):
            parts = obj.key.split('/')
            if parts[-1] == '': # folder itself
                continue

            if not flat:
                name = parts[len(path[1:])]
            else:
                name = obj.key.replace(prefix, '')

            if not name:
                continue

            if '.' in name:
                files.append(name)
            else:
                folders.append(name)

        return sorted(list(set(folders))) + sorted(list(set(files)))

    @classmethod
    def is_ns(cls, path):
        bucket_name = path[0]
        path = '/'.join(path[1:]) or '/'
        s3 = boto3.resource('s3')
        try:
            s3.Object(bucket_name, path).load()
        except botocore.exceptions.ClientError as e:
            if e.response['Error']['Code'] == "404":
                #its not a file, so it must be a path
                return True
            else:
                raise

        return False

    @classmethod
    def get_dataset(cls, ds, define=False):
        return S3(Base.process_ds(ds), create=define)

    def create(self, data):
        with self._buffer_lock:
            self._buffer.append(data)

    def flush(self, objs):
        s3 = boto3.resource('s3')

        bucket_name, path = dict2bucket(self.params)

        obj = s3.Object(bucket_name, path)

        csv_data = dict2tab(objs, self.params.fields, 'csv')

        try:
            obj.put(Body=csv_data)
        except botocore.exceptions.ClientError as e:
            raise prf_exc.HTTPBadRequest('Error:%r, Bucket:%s, Path:%s' % (e, bucket_name, path))

        success = total = len(objs)
        log.debug('BULK FLUSH: total=%s, success=%s, errors=%s, retries=%s',
                                            total, success, 0, 0)

        return success, 0, 0

