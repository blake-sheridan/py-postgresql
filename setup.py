#!/usr/local/bin/python3

from distutils.core import setup, Extension

setup(
    name = 'postgresql',
    version = '0.1',
    description = 'A PostgreSQL front end',
    ext_modules = [
        Extension(
            name = 'postgresql',
            include_dirs = [
                '/usr/include/postgresql',
                'include',
            ],
            language = 'c++',
            libraries = [
                'pq',
            ],
            sources = [
                'src/postgresql.cpp',
            ],
        ),
    ],
)
