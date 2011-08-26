#!/usr/bin/env python
# -*- Mode: Python -*-
# vi:si:et:sw=4:sts=4:ts=4

# Copyright (c) 2007-2008
# See LICENSE for details.

from distutils.core import setup
import setuptools


def main():
    setup(
        name="paisley",
        version="0.3.1",
        description=("Paisley is a CouchDB client written in Python to be used "
                     "within a Twisted application."),
        author="Paisley Developers",
        author_email="",
        license="MIT",
        url="http://github.com/smcq/paisley",
        download_url="http://github.com/smcq/paisley/zipball/v0.3.1",
        packages = setuptools.find_packages (),
        package_data = {
            '': ['*.template'],
            })

if __name__ == "__main__":
    main()
