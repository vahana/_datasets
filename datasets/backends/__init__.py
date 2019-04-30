from datasets.backends.csv import CSVBackend
from datasets.backends.es import ESBackend
from datasets.backends.mongo import MongoBackend
from datasets.backends.http import HTTPBackend
from datasets.backends.s3 import S3Backend

ES_BE_NAME = 'es'
MONGO_BE_NAME = 'mongo'
CSV_BE_NAME = 'csv'
S3_BE_NAME = 's3'

class Backend(object):
    def __init__(self, params, job_log):
        self.params = params

        if params.get('backend') in ['http', 'https']:
            self.backend = HTTPBackend(params, job_log)
        elif params.get('backend') == ES_BE_NAME:
            self.backend = ESBackend(params, job_log)
        elif params.get('backend') == CSV_BE_NAME:
            self.backend = CSVBackend(params, job_log)
        elif params.get('backend') == S3_BE_NAME:
            self.backend = S3Backend(params, job_log)
        else:
            self.backend = MongoBackend(params, job_log)

    def process(self, data):
        return self.backend.process_many(data)