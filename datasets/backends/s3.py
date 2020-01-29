import logging
import os
import boto3
import botocore

import prf
import datasets
from datasets.backends.base import Base
from datasets.backends.csv import CSV

log = logging.getLogger(__name__)


def Bucket(name):
    s3 = boto3.resource('s3')
    return s3.Bucket(name)


class S3(CSV):

    def __init__(self, ds, create=False):
        path = ds.ns.split('/')
        bucket_name = path[0]
        prefix = '/'.join(path[1:]+[ds.name])

        self.bucket = Bucket(bucket_name)

        self.obj = None

        if create:
            self.file_name = 's3://%s/%s' % (self.bucket.name, ds.name)
        else:
            for it in self.bucket.objects.filter(Prefix=prefix):
                self.obj = it
                break

            if not self.obj:
                raise prf.exc.HTTPBadRequest('File not found `%s`' % ds.name)

            self.file_name = 's3://%s/%s' % (self.bucket.name, self.obj.key)


class S3Backend(object):
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

