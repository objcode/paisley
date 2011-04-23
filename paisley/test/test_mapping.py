# -*- Mode: Python; test-case-name: paisley.test.test_mapping -*-
# vi:si:et:sw=4:sts=4:ts=4

# Copyright (c) 2008
# See LICENSE for details.

"""
Tests for the object mapping API.
"""

from twisted.trial.unittest import TestCase
from twisted.internet.defer import succeed
from paisley import mapping, views
from test_views import StubCouch

# an object for a view result that includes docs
class Tag(mapping.Document):
    name = mapping.TextField()
    count = mapping.IntegerField()

    def fromDict(self, dictionary):
        self._data = dictionary['doc']

class MappingTests(TestCase):
    def setUp(self):
        # this StubCouch is different than in test_views; it replies to
        # include_docs=true, hence it has an additional key/value pair
        # for doc from which the object can be mapped
        self.fc = StubCouch(views={'all_tags?include_docs=true': {
             'total_rows': 3,
             'offset': 0,
             'rows': [
                {'key':'foo', 'value':3, 'doc': {'name':'foo', 'count':3}},
                {'key':'bar', 'value':2, 'doc': {'name':'foo', 'count':3}},
                {'key':'baz', 'value':1, 'doc': {'name':'foo', 'count':3}},
            ]}})

    def test_queryView(self):
        """
        Test that a querying a view gives us an iterable of our user defined
        objects.
        """
        v = views.View(self.fc, None, None, 'all_tags?include_docs=true', Tag)

        def _checkResults(results):
            results = list(results)
            self.assertEquals(len(results), 3)

            # this used to be not executed because it worked on the empty
            # generator; so guard against that
            looped = False
            for tag in results:
                looped = True
                self.assertIn(tag.name, ['foo', 'bar', 'baz'])
            self.failUnless(looped)

        d = v.queryView()
        d.addCallback(_checkResults)
        return d
