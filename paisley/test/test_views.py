# -*- Mode: Python; test-case-name: paisley.test.test_views -*-
# vi:si:et:sw=4:sts=4:ts=4

# Copyright (c) 2008
# See LICENSE for details.

"""
Tests for the object mapping view API.
"""

from twisted.trial.unittest import TestCase
from twisted.internet.defer import succeed

from paisley.test import test_util

from paisley.views import View


class StubCouch(object):
    """
    A stub couchdb object that will return preset dictionaries
    """

    def __init__(self, views=None):
        self._views = views

    def openView(self, dbName, docId, viewId, **kwargs):
        return succeed(self._views[viewId])

# an object for a view result not including docs


class Tag(object):

    def fromDict(self, dictionary):
        self.name = dictionary['key']
        self.count = dictionary['value']

ROWS = [
                {'key':'foo', 'value':3},
                {'key':'bar', 'value':2},
                {'key':'baz', 'value':1},
]


class CommonTestCase:
    """
    These tests are executed both against the stub couch and the real couch.
    """

    def test_queryView(self):
        """
        Test that querying a view gives us an iterable of our user defined
        objects.
        """
        v = View(self.db, 'test', 'design_doc', 'all_tags', Tag)

        def _checkResults(results):
            results = list(results)
            self.assertEquals(len(results), 3)

            # this used to be not executed because it worked on the empty
            # generator; so guard against that
            looped = False
            for tag in results:
                looped = True
                self.assertIn({'key': tag.name, 'value': tag.count},
                              ROWS)
            self.failUnless(looped)

        d = v.queryView()
        d.addCallback(_checkResults)
        return d


class StubViewTests(CommonTestCase, TestCase):

    def setUp(self):
        self.db = StubCouch(views={'all_tags': {
            'total_rows': 3,
            'offset': 0,
            'rows': ROWS,
            }})


class RealViewTests(CommonTestCase, test_util.CouchDBTestCase):

    def setUp(self):
        test_util.CouchDBTestCase.setUp(self)

        d = self.db.createDB('test')

        for row in ROWS:
            d.addCallback(lambda _, r: self.db.saveDoc('test', r), row)


        viewmapjs = """
function(doc) {
    emit(doc.key, doc.value);
}
"""

        d.addCallback(lambda _: self.db.saveDoc('test',
            {
                'views': {
                    "all_tags": {
                        "map": viewmapjs,
                    },
                },
            },
            '_design/design_doc'))

        return d
