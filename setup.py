#!/usr/bin/env python

from distutils.core import setup

from gearman import __version__ as version

setup(
    name = 'gearman',
    version = version,
    description = 'Gearman Library',
    author = 'Samuel Stauffer',
    author_email = 'samuel@descolada.com',
    url = 'http://github.com/samuel/python-gearman/tree/master',
    packages = ['gearman'],
    classifiers = [
        'Intended Audience :: Developers',
        'License :: OSI Approved :: MIT License',
        'Operating System :: OS Independent',
        'Programming Language :: Python',
        'Topic :: Software Development :: Libraries :: Python Modules',
    ],
)
