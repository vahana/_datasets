# datasets

Datasets are abstractions on top of a database layer (mongodb) providing transparent way of working with collections across databases.
It can be used in pyramid applications by calling ```config.include('datasets')``` which will boostrap the database connections (aliases) specified in the configuration file and expose these aliases as namespaces. The datasets then can be access using a dot notation. e.g. `master.companies` is refering to a `companies` collection in `master` database.
It uses `mongoengine` ORM to work with mongodb. It exposes set of convinient methods to define dataset documents, load namespaces, etc.
