# -*- test-case-name: paisley.test_changes -*-
# -*- Mode: Python -*-
# vi:si:et:sw=4:sts=4:ts=4

import os

from twisted.internet import defer, reactor, error
from twisted.trial import unittest

from paisley import client, changes

from paisley import test_util


class FakeNotifier(object):

    def __init__(self):
        self.changes = []

    def changed(self, change):
        self.changes.append(change)


class TestStubChangeReceiver(unittest.TestCase):

    def testChanges(self):
        notifier = FakeNotifier()
        receiver = changes.ChangeReceiver(notifier)

        # ChangeNotifier test lines
        path = os.path.join(os.path.dirname(__file__),
            'test.changes')
        handle = open(path)
        text = handle.read()

        for line in text.split("\n"):
            receiver.lineReceived(line)

        self.assertEquals(len(notifier.changes), 3)
        self.assertEquals(notifier.changes[0]["seq"], 3934)
        self.assertEquals(notifier.changes[2]["deleted"], True)


class BaseTestCase(test_util.CouchDBTestCase):
    tearing = False # set to True during teardown so we can assert

    def setUp(self):
        test_util.CouchDBTestCase.setUp(self)

    def tearDown(self):
        self.tearing = True

    def waitForNextCycle(self):
        # Wait for the reactor to cycle.
        # Useful after telling the notifier to stop, since the actual
        # shutdown is triggered on one of the next cycles
        # 0 is not enough though
        d = defer.Deferred()
        reactor.callLater(0.01, d.callback, None)
        return d


class ChangeReceiverTestCase(BaseTestCase, changes.ChangeListener):

    lastChange = None
    _deferred = None

    ### ChangeListener interface

    def changed(self, change):
        self.lastChange = change
        if self._deferred is not None:
            # reset self._deferred before callback because this can be
            # called recursively
            d = self._deferred
            self._deferred = None
            d.callback(change)

    def connectionLost(self, reason):
        # make sure we lost the connection cleanly
        self.failIf(self.tearing,
            'connectionLost should be called before teardown '
            'through notifier.stop')
        self.failUnless(reason.check(error.ConnectionDone))

    ### method for subclasses

    def waitForChange(self):
        self._deferred = defer.Deferred()
        return self._deferred


class ListenerChangeReceiverTestCase(ChangeReceiverTestCase):

    def setUp(self):
        ChangeReceiverTestCase.setUp(self)

        return self.db.createDB('test')

    def testChanges(self):
        notifier = changes.ChangeNotifier(self.db, 'test')
        notifier.addListener(self)


        d = notifier.start()

        def create(_):
            changeD = self.waitForChange()

            saveD = self.db.saveDoc('test', {'key': 'value'})
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

        d.addCallback(lambda _: notifier.stop())
        d.addCallback(lambda _: self.waitForNextCycle())
        return d

    def testChangesFiltered(self):
        """
        This tests that we can use a filter to only receive notifications
        for documents that interest us.
        """
        notifier = changes.ChangeNotifier(self.db, 'test')
        notifier.addListener(self)

        d = defer.Deferred()

        filterjs = """
function(doc, req) {
    log(req.query);
    var docids = eval('(' + req.query.docids + ')');
    log(docids);
    if (docids.indexOf(doc._id) > -1) {
        return true;
    } else {
        return false;
    }
}
"""

        d.addCallback(lambda _: self.db.saveDoc('test',
            {
                'filters': {
                    "test": filterjs,
                },
            },
            '_design/design_doc'))


        d.addCallback(lambda _: notifier.start(
            filter='design_doc/test',
            docids=client.json.dumps(['one', ])))

        def create(_):
            changeD = self.waitForChange()

            saveD = self.db.saveDoc('test', {'key': 'value'}, docId='one')
            saveD.addCallback(lambda r: setattr(self, 'firstid', r['id']))

            dl = defer.DeferredList([saveD, changeD])

            def check(_):
                c = self.lastChange
                self.assertEquals(c['id'], self.firstid)
                self.assertEquals(len(c['changes']), 1)
                self.assertEquals(c['changes'][0]['rev'][:2], '1-')
                self.assertEquals(c['seq'], 2)
            dl.addCallback(check)

            dl.addCallback(lambda _: self.db.openDoc('test', self.firstid))
            dl.addCallback(lambda r: setattr(self, 'first', r))

            return dl
        d.addCallback(create)

        def update(_):
            changeD = self.waitForChange()

            self.first['key'] = 'othervalue'
            saveD = self.db.saveDoc('test', self.first, docId=self.firstid)
            saveD.addCallback(lambda r: setattr(self, 'firstrev', r['rev']))

            dl = defer.DeferredList([saveD, changeD])

            def check(_):
                c = self.lastChange
                self.assertEquals(c['id'], self.firstid)
                self.assertEquals(len(c['changes']), 1)
                self.assertEquals(c['changes'][0]['rev'][:2], '2-')
                self.assertEquals(c['seq'], 3)
            dl.addCallback(check)

            return dl
        d.addCallback(update)

        def createTwoAndUpdateOne(_):
            # since createTwo is not supposed to trigger a change, we can't
            # assert that it didn't until we make another change that is
            # detected.
            changeD = self.waitForChange()

            saveD = self.db.saveDoc('test', {'key': 'othervalue'}, docId='two')

            def update(_):
                self.first['key'] = 'thirdvalue'
                self.first['_rev'] = self.firstrev
                return self.db.saveDoc('test', self.first, docId=self.firstid)
            saveD.addCallback(update)

            dl = defer.DeferredList([saveD, changeD])
            # FIXME: this failure actually gets swallowed, even though
            # DeferredList should not do that; so force DeferredList to fail
            # to reproduce, remove the line that updates firstrev, and
            # don't add the eb below

            def eb(failure):
                dl.errback(failure)
                # self.fail('Could not update: %r' % failure)
                return failure
            saveD.addErrback(eb)

            def check(_):
                c = self.lastChange
                self.assertEquals(c['id'], self.firstid)
                self.assertEquals(len(c['changes']), 1)
                self.assertEquals(c['changes'][0]['rev'][:2], '3-')
                # Note that we didn't receive change with seq 4,
                # which was the creation of doc two
                self.assertEquals(c['seq'], 5)
            dl.addCallback(check)

            return dl
        d.addCallback(createTwoAndUpdateOne)
        d.addCallback(lambda _: notifier.stop())
        d.addCallback(lambda _: self.waitForNextCycle())

        d.callback(None)
        return d


class ConnectionLostTestCase(BaseTestCase, changes.ChangeListener):

    def setUp(self):
        BaseTestCase.setUp(self)

        notifier = changes.ChangeNotifier(self.db, 'test')
        notifier.addListener(self)

        d = self.db.createDB('test')
        d.addCallback(lambda _: notifier.start())
        return d

    ### ChangeListener interface

    def changed(self, change):
        pass

    def connectionLost(self, reason):
        # make sure we lost the connection before teardown
        self.failIf(self.tearing,
            'connectionLost should be called before teardown')

        self.failIf(reason.check(error.ConnectionDone))

        from twisted.web import _newclient
        self.failUnless(reason.check(_newclient.ResponseFailed))

    def testKill(self):
        self.wrapper.process.terminate()
        return self.waitForNextCycle()
