from datasets.backends.csv import CSVBackend
from datasets.backends.es import ESBackend
from datasets.backends.mongo import MongoBackend
from datasets.backends.url import URLBackend

ES_BE_NAME = 'es'
MONGO_BE_NAME = 'mongo'
URL_BE_NAME = 'url'
CSV_BE_NAME = 'csv'


class Backend(object):
    def __init__(self, params, job_log):
        self.params = params

        if params.get('backend') == URL_BE_NAME:
            self.backend = URLBackend(params)
        elif params.get('backend') == ES_BE_NAME:
            self.backend = ESBackend(params)
        elif params.get('backend') == CSV_BE_NAME:
            self.backend = CSVBackend(params)
        else:
            self.backend = MongoBackend(params, job_log)

    def process(self, data):
        return self.backend.process_many(data)