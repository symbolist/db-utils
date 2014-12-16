#!/usr/bin/env python
from setuptools import setup


setup(
    name='db-utils',
    version='0.0.1',
    author='edX',
    description='Helpers for changing MYSQL isolation levels and retrying transactions.',
    url='https://github.com/symbolist/db-utils',
    license='AGPL',
    classifiers=[
        'Development Status :: 3 - Alpha',
        'Intended Audience :: Developers',
        'License :: OSI Approved :: GNU Affero General Public License v3',
        'Operating System :: OS Independent',
        'Programming Language :: Python',
    ],
    packages=['db_utils'],
    install_requires=[],
    tests_require=[],
)
