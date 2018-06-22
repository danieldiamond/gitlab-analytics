#!/usr/bin/env python
from distutils.core import setup

setup(name='meltano-extract-gitlab',
      version='1.0',
      description='Meltano GitLab extractor.',
      author='Jacob Schatz',
      author_email='jschatz@gitlab.com',
      url='https://gitlab.com/meltano/meltano',
      packages=['meltano.gitlab'],
      package_dir={'meltano.gitlab': 'src'}
)
