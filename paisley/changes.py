# -*- test-case-name: paisley.test_changes -*-
# -*- Mode: Python -*-
# vi:si:et:sw=4:sts=4:ts=4

from twisted.internet import reactor
from twisted.web import client
from twisted.protocols import basic

from paisley.client import json

class ChangeReceiver(basic.LineReceiver):
    # figured out by checking the last two characters on actually received lines
    delimiter = '\n'

    def __init__(self, notifier):

        self._notifier = notifier

    def lineReceived(self, line):
        if not line:
            return

        change = json.loads(line)

        if not 'id' in change:
            return

        self._notifier.changed(change)

class ChangeNotifier:
    def __init__(self, db, dbName):
        self._db = db
        self._dbName = dbName

        self._caches = []
        self._listeners = []
        self._prot = ChangeReceiver(self)

        self._since = None

    def addCache(self, cache):
        self._caches.append(cache)

    def addListener(self, listener):
        self._listeners.append(listener)

    def start(self):
        """
        Start listening and notifying of changes.
        Separated from __init__ so you can add caches and listeners.

        @returns: Deferred firing the most recent change id.
        """
        d = self._db.infoDB(self._dbName)
        def infoDBCb(info):
            self._since = info['update_seq']
            url = str(self._db.url_template %
                '/%s/_changes?feed=continuous&since=%d' % (
                    self._dbName, self._since))
            return self._db.client.request('GET', url)
        d.addCallback(infoDBCb)
        def requestCb(response):
            response.deliverBody(self._prot)
        d.addCallback(requestCb)
        def returnCb(_):
            return self._since
        d.addCallback(returnCb)

        return d

    # called by receiver
    def changed(self, change):
        for cache in self._caches:
            cache.delete(change['id'])

        for listener in self._listeners:
            listener.changed(change)
