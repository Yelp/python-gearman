#!/usr/bin/env python

from distutils.core import setup

from gearman import __version__ as version

setup(
    name = 'gearman',
    version = version,
    description = 'Gearman API - Client, worker, and admin client interfaces',
    author = 'Matthew Tai',
    author_email = 'mtai@yelp.com',
    url = 'http://github.com/mtai/py-gearman/tree/master',
    packages = ['gearman'],
    classifiers = [
        'Intended Audience :: Developers',
        'License :: OSI Approved :: Apache Software License',
        'Operating System :: OS Independent',
        'Programming Language :: Python',
        'Topic :: Software Development :: Libraries :: Python Modules',
    ],
)
