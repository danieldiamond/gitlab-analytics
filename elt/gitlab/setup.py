#!/usr/bin/env python
from distutils.core import setup

setup(name='bizops-gitlab',
      version='1.0',
      description='BizOps GitLab importer.',
      author='Jacob Schatz',
      author_email='jschatz@gitlab.com',
      url='https://gitlab.com/bizops/bizops',
      packages=['bizops.gitlab'],
      package_dir={'bizops.gitlab': 'src'},
     )
