#!/usr/bin/env python
# Copyright (c) 2007-2008
# See LICENSE for details.

from distutils.core import setup


def main():
    setup(
        name="paisley",
        version="0.1",
        description=("Paisley is a CouchDB client written in Python to be used "
                     "within a Twisted application."),
        author="Paisley Developpers",
        author_email="",
        license="MIT",
        url="http://github.com/smcq/paisley",
        py_modules=["paisley", "test_paisley"],
    )

if __name__ == "__main__":
    main()
