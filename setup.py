from distutils.core import setup
setup(name='BTEdb',
      version='6.2.7',
	  description="Python schemaless JSON/YAML database interface",
	  author="Niles Rogoff",
	  author_email="nilesrogoff@gmail.com",
	  url="http://github.com/nilesr/BTEdb",
      py_modules=['BTEdb'],
	  install_requires = ["dill"],
      )