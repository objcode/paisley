# -*- Mode: Python; test-case-name: paisley.test.test_pjson -*-
# vi:si:et:sw=4:sts=4:ts=4

# Copyright (c) 2011
# See LICENSE for details.

"""
Test for Paisley JSON code.
"""

from paisley import pjson as json

# uncomment to run test with non-strict JSON parsing
# json.set_strict(False)

from twisted.trial import unittest


class JSONTestCase(unittest.TestCase):

    def testStrict(self):
        self.assertEquals(json.STRICT, True)

    def testStrToUnicode(self):
        u = json.loads('"str"')
        self.assertEquals(u, u'str')
        self.assertEquals(type(u), unicode)

    def testUnicodeToUnicode(self):
        u = json.loads(u'"str"')
        self.assertEquals(u, u'str')
        self.assertEquals(type(u), unicode)
