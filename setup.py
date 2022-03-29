#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""The setup script."""

from setuptools import setup, find_packages

with open('README.rst') as readme_file:
    readme = readme_file.read()

with open('HISTORY.rst') as history_file:
    history = history_file.read()

requirements = [
    "python-slugify>=1.1.4",
    "psycopg2>=2.6.2",
    "boto==2.45.0",
    "csvkit==1.0.2",
    "slackclient==1.0.4",
    "six==1.11.0",
    "paramiko==2.10.1",
    "pytz==2017.2"
]

setup_requirements = [
]

test_requirements = [
    "pytest",
    "psycopg2>=2.6.2",
    "coverage==4.1",
    "pytest==3.0.7",
    "pytest-cov==2.4.0",
    "mock==2.0.0",
    "moto==0.4.30",
    "testfixtures==4.13.5",
]

setup(
    name='airflow-plugins',
    version='0.1.3',
    description="Airflow plugins.",
    long_description=readme + '\n\n' + history,
    author="Michael Kuty",
    author_email='michael.kuty@stories.bi',
    url='https://github.com/storiesbi/airflow-plugins',
    packages=find_packages(include=['airflow_plugins.*', 'airflow_plugins']),
    include_package_data=True,
    install_requires=requirements,
    license="MIT license",
    zip_safe=False,
    keywords='airflow_plugins',
    classifiers=[
        'Development Status :: 2 - Pre-Alpha',
        'Intended Audience :: Developers',
        'License :: OSI Approved :: MIT License',
        'Natural Language :: English',
        "Programming Language :: Python :: 2",
        'Programming Language :: Python :: 2.6',
        'Programming Language :: Python :: 2.7',
        'Programming Language :: Python :: 3',
        'Programming Language :: Python :: 3.3',
        'Programming Language :: Python :: 3.4',
        'Programming Language :: Python :: 3.5',
    ],
    test_suite='tests',
    tests_require=test_requirements,
    setup_requires=setup_requirements,
)
