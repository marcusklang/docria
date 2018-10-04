# -*- coding: utf-8 -*-

from setuptools import setup, find_packages


with open('README.rst') as f:
    readme = f.read()

with open('LICENSE') as f:
    license = f.read()

setup(
    name='docria',
    version='0.1.0',
    description='Semi-structured Document Model for Python',
    long_description=readme,
    author='Marcus Klang',
    author_email='marcus.klang@cs.lth.se',
    url='https://github.com/marcusklang/docria',
    license=license,
    packages=find_packages(exclude=('tests', 'docs'))
)