#!/usr/bin/env python

from distutils.core import setup

from gearman import __version__ as version

setup(
    name = 'gearman',
    version = version,
    author = 'Matthew Tai',
    author_email = 'mtai@yelp.com',
    description = 'Gearman API - Client, worker, and admin client interfaces',
    long_description=open('README.rst').read(),
    url = 'http://github.com/Yelp/python-gearman/',
    packages = ['gearman'],
    license='LICENSE.txt',
    classifiers = [
        'Development Status :: 4 - Beta',
        'Intended Audience :: Developers',
        'License :: OSI Approved :: Apache Software License',
        'Natural Language :: English',
        'Operating System :: OS Independent',
        'Programming Language :: Python',
        'Topic :: Software Development :: Libraries :: Python Modules',
    ],
)
