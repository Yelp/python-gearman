#!/usr/bin/env python

from setuptools import setup

from gearman import __version__ as version

setup(
    name = 'gearman',
    version = version,
    author = 'Matthew Tai',
    author_email = 'mtai@yelp.com',
    description = 'Gearman API - Client, worker, and admin client interfaces',
    long_description=open('README.txt').read(),
    url = 'http://github.com/Yelp/python-gearman/',
    packages = ['gearman'],
    license='Apache',
    classifiers = [
        'Development Status :: 5 - Production/Stable',
        'Intended Audience :: Developers',
        'License :: OSI Approved :: Apache Software License',
        'Natural Language :: English',
        'Operating System :: OS Independent',
        'Programming Language :: Python',
        'Programming Language :: Python :: 2.4',
        'Programming Language :: Python :: 2.5',
        'Programming Language :: Python :: 2.6',
        'Programming Language :: Python :: 2.7',
        'Topic :: Software Development :: Libraries :: Python Modules',
    ],
)
