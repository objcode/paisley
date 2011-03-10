#!/usr/bin/env python
# Copyright (c) 2007-2008
# See LICENSE for details.

from distutils.core import setup


def main():
    setup(
        name="paisley",
        version="0.3",
        description=("Paisley is a CouchDB client written in Python to be used "
                     "within a Twisted application."),
        author="Paisley Developers",
        author_email="",
        license="MIT",
        url="http://github.com/smcq/paisley",
        download_url="http://github.com/smcq/paisley/zipball/v0.3",
        py_modules=["paisley", "paisley.client", "paisley.test_paisley"],
    )

if __name__ == "__main__":
    main()
