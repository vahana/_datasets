import logging
import os
import boto3

import prf
import datasets
from datasets.backends.base import Base
from datasets.backends.csv import CSV

log = logging.getLogger(__name__)


def Bucket(name=None):
    s3 = boto3.resource('s3')
    return s3.Bucket(name or datasets.Settings.get('s3.root'))

class S3(CSV):

    def __init__(self, ds):
        self.bucket = Bucket()
        self.obj = None
        for it in self.bucket.objects.filter(Prefix='%s/%s'%(ds.ns,ds.name)):
            self.obj = it
            break

        if not self.obj:
            raise prf.exc.HTTPBadRequest('File not found `%s`' % ds.name)

        self.file_name = 's3://%s/%s' % (self.bucket.name, self.obj.key)


class S3Backend(object):

    @classmethod
    def ls_namespaces(cls):
        names = []

        for obj in Bucket().objects.all():
            parts = os.path.split(obj.key)
            if parts[0] != '':
                names.append(parts[0])

        return sorted(list(set(names)))

    @classmethod
    def ls_ns(cls, ns):
        names = []

        for obj in Bucket().objects.filter(Prefix=ns):
            parts = os.path.split(obj.key)
            if parts[1] != '':
                names.append(parts[1])

        return sorted(names)

    @classmethod
    def get_dataset(cls, ds):
        return S3(Base.process_ds(ds))

