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
               'datatransfer',
               's3_transfer']
description = 'MySQL Backup Script.\n' \
              '================================================' \
              'This script is connecting to MySQL server, ' \
              'executes mysqldump and saves dumpfile to specified directory.\n' \
              'then, transfer dumpfiles to specified remote host.\n' \
              'config file path is ' \
              '<python3 lib directory>/site|dist-packages/daily_backup/config/backup.json\n' \
              'run following command to executes this scripts!(must be privileged user)\n' \
              'python3 <python3 lib directory>/dist-packages/daily_backup/local_backup.py'


with open('README.rst') as f:
    readme = f.read()

with open('LICENSE') as f:
    license = f.read()

setup(
    name='daily_backup',
    version='1.6',
    description=description,
    long_description=readme,
    author='Takeki Shikano',
    author_email='shikano.takeki@nexon.co.jp',
    require=requirement,
    url=None,
    license='MIT',
    packages=find_packages(exclude=('tests', 'docs')),
    package_data={'daily_backup': ['config/backup.json', 'README']}
)


