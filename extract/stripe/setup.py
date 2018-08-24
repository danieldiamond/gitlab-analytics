# !/usr/bin/env python
from distutils.core import setup

setup(
    name='meltano-extract-stripe',
    version='1.0',
    description='Meltano Stripe extractor.',
    author='Meltano',
    author_email='meltano@gitlab.com',
    url='https://gitlab.com/meltano/meltano',
    packages=['meltano.extract.stripe'],
    package_dir={'meltano.extract.stripe': 'src'},
)
