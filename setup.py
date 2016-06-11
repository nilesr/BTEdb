#from distutils.core import setup
from setuptools import setup
setup(name='BTEdb',
      version='7.0',
	  description="Python schemaless JSON/YAML database interface",
	  author="Peter Rogoff",
	  author_email="peter@rogoff.xyz",
	  url="http://github.com/nilesr/BTEdb",
      py_modules=['BTEdb'],
	  install_requires = ["dill"],
      )
