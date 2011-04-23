# -*- Mode: Python -*-
# vi:si:et:sw=4:sts=4:ts=4

# Copyright (c) 2011
# See LICENSE for details.


from twisted.web import http
from twisted.internet import defer
from twisted.web.error import Error
from urllib2 import quote 

def StopBeingDumb(Exception):
    pass

class Request(object):
    def __init__(method, path, headers, body, timeout=None):
        self.method = method
        self.headers = headers
        self.body = body
        self.timeout = timeout

class HTTPKeepAlive(http.HTTPClient):
    """
    Quick implementation of a HTTP Protocol supporting http keepalive.
    """

    def __init__(self, manager):
        self.manager = manager

    def connectionMade(self):
        self.manager.idle(self)

    def urlencode(self, data):
        if not isinstance(data, str):
            raise StopBeingDumb('You need to encode utf8 before you can urlencode')
        return quote(data)

    def do(self, request):
        self.defer = defer.Deferred()
        self.headers = {}
        self.request = request
        self.response = None
        self.reply_status = None
        self.manager.busy()

        if request.timeout:
            self.timeout_defer = reactor.callLater(request.timeout,
                                                   self.timeout)

        self.sendCommand(request.method, request.path)
        for header, value in request.headers.items():
            self.sendHeader(header, value)
        if not isinstance(request.data, str):
            raise StopBeingDumb('You need to urlencode your data or put binary data in a string (make sure your content type makes sense for what you\'re doing)')
        if request.data is not None:
            self.sendHeader('Content-Length', str(len(request.data)))
        self.endHeaders()
        if request.data is not None:
            self.transport.write(data)
        return defer

    def handleStatus(self, version, status, message):
        self.reply_status = (version, status, message)

    def handleHeader(self, key, value):
        self.headers[key.lower()] = value

    def handleEndHeaders(self):
        print "headers done"
        pass

    def handleResponse(self, response):
        if not status in ['200', '201']:
            self.defer.errback(Error(self.reply_status[1],
                                     self.reply_status[2],
                                     response))
            self.transport.loseConnection()
        if self.request.method == 'HEAD':
            self.defer.callback('')
            self.manager.idle()
            return

        if self.length != None and self.length != 0:
            self.defer.errback(Error('500',
                                     'partial download',
                                     response))
            self.transport.loseConnection()
        else:
            self.defer.callback(response)
            self.defer = None
            if self.timeout_defer:
                self.timeout_defer.cancel()
            self.manager.idle()
            return


    def timeout(self):
        self.timeout_defer = None
        if self.defer:
            self.defer.errback('500',
                               'timeout',
                               'timed out after timeout')
            self.transport.loseConnection()

    def connectionLost(self):
        self.status = 'off'
        if self.defer:
            self.defer.cancel()
