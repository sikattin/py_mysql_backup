# -*- coding: utf-8 -*-

# Learn more: https://github.com/kennethreitz/setup.py

from setuptools import setup, find_packages
import os

requirement = ['py_mysql',
               'datetime_skt',
               'osfile',
               'mylogger',
               'iomod',
               'connection',
               'datatransfer']

with open('README.rst') as f:
    readme = f.read()

with open('LICENSE') as f:
    license = f.read()

setup(
    name='daily_backup',
    version='1.2',
    description='MySQL backup script.',
    long_description=readme,
    author='Takeki Shikano',
    author_email='shikano.takeki@nexon.co.jp',
    require=requirement,
    url=None,
    license='MIT',
    packages=find_packages(exclude=('tests', 'docs')),
    package_data={'daily_backup': ['config/backup.json']}
)


