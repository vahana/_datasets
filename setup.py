import os

from setuptools import setup, find_packages

here = os.path.abspath(os.path.dirname(__file__))
with open(os.path.join(here, 'README.md')) as f:
    README = f.read()
with open(os.path.join(here, 'VERSION')) as f:
    VERSION = f.read()

setup(name='datasets',
      version=VERSION,
      description='datasets',
      long_description=README,
      classifiers=[
        "Programming Language :: Python",
        ],
      author='vahan',
      author_email='aivosha@gmail.com',
      url='',
      packages=find_packages(),
      include_package_data=True,
      zip_safe=False,
      test_suite="datasets",
      entry_points="""\
      [paste.app_factory]
        main = datasets:main
      """,
      )
