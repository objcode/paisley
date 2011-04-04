# -*- test-case-name: paisley.test_changes -*-
# -*- Mode: Python -*-
# vi:si:et:sw=4:sts=4:ts=4


from twisted.internet import defer
from twisted.trial import unittest

from paisley import client, changes

from paisley import test_util

# ChangeNotifier test lines

TEST_CHANGES = """
{"seq":3934,"id":"cc4fadc922f11ffb5e358d5da2760de2","changes":[{"rev":"1-1e379f46917bc2fc9b9562a58afde75a"}]}
{"changes": [{"rev": "12-7bfdb7016aa8aa0dd0279d3324b524d1"}], "id": "_design/couchdb", "seq": 5823}
{"last_seq":3934}
{"deleted": true, "changes": [{"rev": "2-5e8bd6dae4307ca6f8fcf8afa53e6bc4"}], "id": "27e74762ad0e64d4094f6feea800a826", "seq": 34}
"""

class FakeNotifier(object):
    def __init__(self):
        self.changes = []

    def changed(self, change):
        self.changes.append(change)

class TestStubChangeReceiver(unittest.TestCase):
    def testChanges(self):
        notifier = FakeNotifier()
        receiver = changes.ChangeReceiver(notifier)

        for line in TEST_CHANGES.split("\n"):
            receiver.lineReceived(line)

        self.assertEquals(len(notifier.changes), 3)
        self.assertEquals(notifier.changes[0]["seq"], 3934)
        self.assertEquals(notifier.changes[2]["deleted"], True)

class ChangeReceiverTestCase(test_util.CouchDBTestCase):

    lastChange = None

    def setUp(self):
        test_util.CouchDBTestCase.setUp(self)

        self._deferred = None

    ### ChangeNotifier listener interface
    def changed(self, change):
        self.lastChange = change
        if self._deferred:
            self._deferred.callback(change)
        self._deferred = None

    ### method for subclasses
    def waitForChange(self):
        self._deferred = defer.Deferred()
        return self._deferred

class ListenerChangeReceiverTestCase(ChangeReceiverTestCase):

    def setUp(self):
        ChangeReceiverTestCase.setUp(self)

        self.db = client.CouchDB('localhost', self.port)
        return self.db.createDB('test')

    def testChanges(self):
        notifier = changes.ChangeNotifier(self.db, 'test')
        notifier.addListener(self)


        d = notifier.start()

        def create(_):
            changeD = self.waitForChange()

            saveD = self.db.saveDoc('test', {
                'key': 'value'
            })
            saveD.addCallback(lambda r: setattr(self, 'firstid', r['id']))

            dl = defer.DeferredList([saveD, changeD])

            def check(_):
                c = self.lastChange
                self.assertEquals(c['id'], self.firstid)
                self.assertEquals(len(c['changes']), 1)
                self.assertEquals(c['changes'][0]['rev'][:2], '1-')
            dl.addCallback(check)

            dl.addCallback(lambda _: self.db.openDoc('test', self.firstid))
            dl.addCallback(lambda r: setattr(self, 'first', r))

            return dl
        d.addCallback(create)

        def update(_):
            changeD = self.waitForChange()

            self.first['key'] = 'othervalue'
            saveD = self.db.saveDoc('test', self.first, docId=self.firstid)

            dl = defer.DeferredList([saveD, changeD])

            def check(_):
                c = self.lastChange
                self.assertEquals(c['id'], self.firstid)
                self.assertEquals(len(c['changes']), 1)
                self.assertEquals(c['changes'][0]['rev'][:2], '2-')
            dl.addCallback(check)

            return dl
        d.addCallback(update)

        return d
