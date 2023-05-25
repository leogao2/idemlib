
from distutils.core import setup

from setuptools import find_packages

setup(name='cachelib',
      version='0.1.0',
      description='caching utilities',
      author='Leo Gao',
      author_email='leogao31@gmail.com',
      url='https://github.com/leogao2/cachelib',
      packages=find_packages(),
      install_requires=[
          'blobfile'
      ]
)
