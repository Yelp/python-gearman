#!/usr/bin/env python

from distutils.core import setup

setup(
    name = 'gearman',
    version = '1.2.1',
    description = 'Gearman Client',
    author = 'Samuel Stauffer',
    author_email = 'samuel@descolada.com',
    url = 'http://github.com/samuel/python-gearman/tree/master',
    packages = ['gearman'],
    classifiers = [
        'Intended Audience :: Developers',
        'License :: OSI Approved :: MIT License',
        'Operating System :: OS Independent',
        'Programming Language :: Python',
    ],
)
