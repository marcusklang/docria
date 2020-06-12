# -*- coding: utf-8 -*-

from setuptools import setup, find_packages


with open('README.rst') as f:
    readme = f.read()

with open('LICENSE') as f:
    license = f.read()

with open('requirements.txt') as f:
    required = f.read().splitlines()

setup(
    name='docria',
    version='0.4.1',
    description='Semi-structured Document Model',
    long_description=readme,
    long_description_content_type="text/x-rst",
    author='Marcus Klang',
    author_email='marcus.klang@cs.lth.se',
    install_requires=required,
    url='https://github.com/marcusklang/docria',
    project_urls={
        'Source': 'https://github.com/marcusklang/docria',
        'Tracker': 'https://github.com/marcusklang/docria/issues',
    },
    license="Apache 2.0",
    packages=find_packages(exclude=('tests', 'docs')),
    classifiers=[
        "Development Status :: 4 - Beta",
        "License :: OSI Approved :: Apache Software License",
        'Programming Language :: Python :: 3',
        'Programming Language :: Python :: 3.5',
        'Programming Language :: Python :: 3.6',
        'Programming Language :: Python :: 3.7',
        'Intended Audience :: Developers',
        "Operating System :: OS Independent",
        "Topic :: Utilities"
    ]
)