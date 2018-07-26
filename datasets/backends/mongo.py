from datasets.backends.base import Base
from datasets.mongosets import get_mongo_dataset

class MongoBackend(Base):
    def _save(self, obj, data):
        #when saving with id, mongoengine will create another object
        #even if you are trying to update it. so lets pop it.
        data.pop('id', None)

        for name, val in list(data.items()):
            setattr(obj, name, val)

        if self.params.fail_on_error:
            return obj.save()
        else:
            return obj.save_safe()
