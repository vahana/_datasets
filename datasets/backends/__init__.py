from slovar import slovar
from datasets import name2be
from datasets.backends.fs import FSBackend
from datasets.backends.mongo import MONGOBackend
from datasets.backends.es import ESBackend
from datasets.backends.s3 import S3Backend
from datasets.backends.http import HTTPBackend

BACKENDS = slovar(
    ES_BE_NAME = 'es',
    MONGO_BE_NAME = 'mongo',
    FS_BE_NAME = 'fs',
    S3_BE_NAME = 's3',
    HTTP_BE_NAME = 'http',
)

class Backend(object):
    def __init__(self, params, job_log):
        self.params = params
        if params.backend in BACKENDS.values():
            self.backend = name2be(params.backend)(params, job_log)
        else:
            raise ValueError('Unknown backend in params: %s' % params )

    def process(self, data):
        return self.backend.process_many(data)