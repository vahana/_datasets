import logging
import os
import boto3
import botocore
import io
import pandas as pd

from slovar import slovar
import prf
import prf.exc as prf_exc
from prf.utils import maybe_dotted, get_dt_unique_name
import datasets
from datasets.backends.base import Base
from datasets.backends.csv import CSV, field_processor, NA_LIST
from prf.utils.csv import dict2tab

log = logging.getLogger(__name__)


def dict2bucket(params):
    sep = '.' if '.' in params.ns else '/'
    path = params.ns.split(sep) + [params.name]
    return path[0], '/'.join(path[1:]) or '/'

def path2bucket(path):
    parts = []

    for it in path:
        if '.' in it:
            parts +=it.split('.')
        else:
            parts.append(it)

    return parts[0], '/'.join(parts[1:]) or None

def Bucket(name):
    s3 = boto3.resource('s3')
    return s3.Bucket(name)


class S3(CSV):

    def __init__(self, ds, create=False):
        path = ds.ns.split('/')
        bucket_name = path[0]

        self.path = '/'.join(path[1:]+[ds.name])
        self.bucket = Bucket(bucket_name)

    def drop_collection(self):
        for it in self.bucket.objects.filter(Prefix=self.path):
            it.delete()

    def read_csv(self, page_size, params):
        obj = boto3.resource('s3').Object(self.bucket.name, self.path)
        return pd.read_csv(
                        io.BytesIO(obj.get()['Body'].read()),
                        infer_datetime_format=True,
                        na_values = NA_LIST,
                        keep_default_na = False,
                        dtype=object,
                        chunksize = page_size,
                        skip_blank_lines=True,
                        engine = 'c',
                        **params)


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
        # path = get_dt_unique_name(path+'_part', only_seconds=True)

        obj = s3.Object(bucket_name, path)

        skip_headers = False
        csv_data = dict2tab(objs, self.params.fields, 'csv', skip_headers,
                            processor=self.params.get('processor', field_processor(self.params.fields)))

        try:
            obj.put(Body=csv_data)
        except botocore.exceptions.ClientError as e:
            raise prf_exc.HTTPBadRequest('Error:%r, Bucket:%s, Path:%s' % (e, bucket_name, path))

        success = total = len(objs)
        log.debug('BULK FLUSH: total=%s, success=%s, errors=%s, retries=%s',
                                            total, success, 0, 0)

        return success, 0, 0

