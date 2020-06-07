# Datasets

Datasets is an abstraction layer over various data backends (mongodb, elastic, etc) that introduce generic way of accessing, manipulating and filtering data. It introduces common concepts that are mapped to particular terminology in each of cases:

1. backend - root namespace representing the particular instance of database technology. e.g. `mongo`

2. namespace - encapsulates database or folder notions

3. dataset - represents a table/collection/file

4. item - represents row/document/line



Datasets, when run as a standalone server (pyramid app) or as part of another server exposes RESTful resources mapped to configured backend.

e.g. if mongo db connections are configured it will expose:

`api/mongo/{namespace}/{dataset}`  set of nested resources that are mapped one to one to mongo databases and collections.



Datasets are essential part of [ETL](https://github.com/vahana/etl) pipeline server. 

Current implementation supports following data backends:

1. Mongo DB

2. Elastic Search

3. CSV files in local file system

4. CSV files on AWS S3 buckets

5. HTTP backend - any public http resource that returns JSON data.

