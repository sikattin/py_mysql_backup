# -*- coding: utf-8 -*-

# Learn more: https://github.com/kennethreitz/setup.py

from setuptools import setup, find_packages


with open('README.rst') as f:
    readme = f.read()

with open('LICENSE') as f:
    license = f.read()

setup(
    name='daily_backup',
    version='1.0',
    description='MySQL backup script.',
    long_description=readme,
    author='Takeki Shikano',
    author_email='shikano.takeki@nexon.co.jp',
    url=None,
    license='MIT',
    packages=find_packages(exclude=('tests', 'docs'))
)

