
from distutils.core import setup

from setuptools import find_packages

setup(name='idemlib',
      version='0.1.5',
      description='caching utilities',
      author='Leo Gao',
      author_email='leogao31@gmail.com',
      url='https://github.com/leogao2/idemlib',
      packages=find_packages(),
      install_requires=[
          'blobfile'
      ]
)
