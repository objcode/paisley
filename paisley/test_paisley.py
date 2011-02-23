# Copyright (c) 2007-2008
# See LICENSE for details.

"""
Test for couchdb client.
"""

try:
    import json
except:
    import simplejson as json

import cgi

from twisted.trial.unittest import TestCase
from twisted.internet.defer import Deferred
from twisted.internet import reactor
from twisted.web import resource, server

import paisley



class TestableCouchDB(paisley.CouchDB):
    """
    A couchdb client that can be tested: override the getPage method.
    """

    def __init__(self, *args, **kwargs):
        """
        Initialize the client: forward parameters, and create attributes used in tests.
        """
        paisley.CouchDB.__init__(self, *args, **kwargs)
        self.deferred = Deferred()
        self.uri = None
        self.kwargs = None
        self.called = False


    def _getPage(self, uri, *args, **kwargs):
        """
        Fake getPage that do nothing but saving the arguments.
        """
        if self.called:
            raise RuntimeError("One shot client")
        self.called = True
        self.uri = uri
        self.kwargs = kwargs
        return self.deferred



class CouchDBTestCase(TestCase):
    """
    Test methods against a couchDB.
    """

    def setUp(self):
        """
        Create a fake client to be used in the tests.
        """
        self.client = TestableCouchDB("localhost")
    
    def test_auth_init(self):
        """
        Test setting up client with authentication
        """
        self.client_auth = paisley.CouchDB("localhost",
                                           username="test",
                                           password="testpass")
        
        self.assertEquals(self.client_auth.username, "test")
        self.assertEquals(self.client_auth.password, "testpass")


    def test_get(self):
        """
        Test get method.
        """
        self.client.get("foo")
        self.assertEquals(self.client.uri, "foo")
        self.assertEquals(self.client.kwargs["method"], "GET")


    def test_post(self):
        """
        Test post method.
        """
        self.client.post("bar", "egg")
        self.assertEquals(self.client.uri, "bar")
        self.assertEquals(self.client.kwargs["method"], "POST")
        self.assertEquals(self.client.kwargs["postdata"], "egg")


    def test_put(self):
        """
        Test put method.
        """
        self.client.put("bar", "egg")
        self.assertEquals(self.client.uri, "bar")
        self.assertEquals(self.client.kwargs["method"], "PUT")
        self.assertEquals(self.client.kwargs["postdata"], "egg")


    def test_delete(self):
        """
        Test get method.
        """
        self.client.delete("foo")
        self.assertEquals(self.client.uri, "foo")
        self.assertEquals(self.client.kwargs["method"], "DELETE")


    def _checkParseDeferred(self, d):
        """
        Utility function to test that a Deferred is called with JSON parsing.
        """
        d.callback('["foo"]')
        def cb(res):
            self.assertEquals(res, ["foo"])
        return d.addCallback(cb)


    def test_createDB(self):
        """
        Test createDB: this should C{PUT} the DB name in the uri.
        """
        d = self.client.createDB("mydb")
        self.assertEquals(self.client.uri, "/mydb/")
        self.assertEquals(self.client.kwargs["method"], "PUT")
        return self._checkParseDeferred(d)


    def test_deleteDB(self):
        """
        Test deleteDB: this should C{DELETE} the DB name.
        """
        d = self.client.deleteDB("mydb")
        self.assertEquals(self.client.uri, "/mydb/")
        self.assertEquals(self.client.kwargs["method"], "DELETE")
        return self._checkParseDeferred(d)


    def test_listDB(self):
        """
        Test listDB: this should C{GET} a specific uri.
        """
        d = self.client.listDB()
        self.assertEquals(self.client.uri, "/_all_dbs")
        self.assertEquals(self.client.kwargs["method"], "GET")
        return self._checkParseDeferred(d)


    def test_infoDB(self):
        """
        Test infoDB: this should C{GET} the DB name.
        """
        d = self.client.infoDB("mydb")
        self.assertEquals(self.client.uri, "/mydb/")
        self.assertEquals(self.client.kwargs["method"], "GET")
        return self._checkParseDeferred(d)


    def test_listDoc(self):
        """
        Test listDoc.
        """
        d = self.client.listDoc("mydb")
        self.assertEquals(self.client.uri, "/mydb/_all_docs")
        self.assertEquals(self.client.kwargs["method"], "GET")
        return self._checkParseDeferred(d)


    def test_listDocReversed(self):
        """
        Test listDoc reversed.
        """
        d = self.client.listDoc("mydb", reverse=True)
        self.assertEquals(self.client.uri, "/mydb/_all_docs?reverse=true")
        self.assertEquals(self.client.kwargs["method"], "GET")
        return self._checkParseDeferred(d)


    def test_listDocStartKey(self):
        """
        Test listDoc with a startKey.
        """
        d = self.client.listDoc("mydb", startKey=2)
        self.assertEquals(self.client.uri, "/mydb/_all_docs?startkey=2")
        self.assertEquals(self.client.kwargs["method"], "GET")
        return self._checkParseDeferred(d)


    def test_listDocCount(self):
        """
        Test listDoc with a count.
        """
        d = self.client.listDoc("mydb", count=3)
        self.assertEquals(self.client.uri, "/mydb/_all_docs?count=3")
        self.assertEquals(self.client.kwargs["method"], "GET")
        return self._checkParseDeferred(d)


    def test_listDocMultipleArguments(self):
        """
        Test listDoc with all options activated.
        """
        d = self.client.listDoc("mydb", count=3, startKey=1, reverse=True)
        self.assertEquals(self.client.uri, "/mydb/_all_docs?count=3&startkey=1&reverse=true")
        self.assertEquals(self.client.kwargs["method"], "GET")
        return self._checkParseDeferred(d)


    def test_openDoc(self):
        """
        Test openDoc.
        """
        d = self.client.openDoc("mydb", "mydoc")
        self.assertEquals(self.client.uri, "/mydb/mydoc")
        self.assertEquals(self.client.kwargs["method"], "GET")
        return self._checkParseDeferred(d)


    def test_openDocAtRevision(self):
        """
        Test openDoc with a specific revision.
        """
        d = self.client.openDoc("mydb", "mydoc", revision="ABC")
        self.assertEquals(self.client.uri, "/mydb/mydoc?rev=ABC")
        self.assertEquals(self.client.kwargs["method"], "GET")
        return self._checkParseDeferred(d)


    def test_openDocWithRevisionHistory(self):
        """
        Test openDoc with revision history.
        """
        d = self.client.openDoc("mydb", "mydoc", full=True)
        self.assertEquals(self.client.uri, "/mydb/mydoc?full=true")
        self.assertEquals(self.client.kwargs["method"], "GET")
        return self._checkParseDeferred(d)


    def test_openDocAttachment(self):
        """
        Test openDoc for an attachment.
        """
        d = self.client.openDoc("mydb", "mydoc", attachment="bar")
        self.assertEquals(self.client.uri, "/mydb/mydoc/bar")
        self.assertEquals(self.client.kwargs["method"], "GET")
        # Data is transfered without parsing
        d.callback("test")
        return d.addCallback(self.assertEquals, "test")


    def test_saveDocWithDocId(self):
        """
        Test saveDoc, giving an explicit document ID.
        """
        d = self.client.saveDoc("mydb", "mybody", "mydoc")
        self.assertEquals(self.client.uri, "/mydb/mydoc")
        self.assertEquals(self.client.kwargs["method"], "PUT")
        return self._checkParseDeferred(d)


    def test_saveDocWithoutDocId(self):
        """
        Test saveDoc without a document ID.
        """
        d = self.client.saveDoc("mydb", "mybody")
        self.assertEquals(self.client.uri, "/mydb/")
        self.assertEquals(self.client.kwargs["method"], "POST")
        return self._checkParseDeferred(d)


    def test_saveStructuredDoc(self):
        """
        saveDoc should automatically serialize a structured document.
        """
        d = self.client.saveDoc("mydb", {"value": "mybody", "_id": "foo"}, "mydoc")
        self.assertEquals(self.client.uri, "/mydb/mydoc")
        self.assertEquals(self.client.kwargs["method"], "PUT")
        return self._checkParseDeferred(d)


    def test_deleteDoc(self):
        """
        Test deleteDoc.
        """
        d = self.client.deleteDoc("mydb", "mydoc", "1234567890")
        self.assertEquals(self.client.uri, "/mydb/mydoc?rev=1234567890")
        self.assertEquals(self.client.kwargs["method"], "DELETE")
        return self._checkParseDeferred(d)


    def test_addAttachments(self):
        """
        Test addAttachments.
        """
        doc = {"value": "bar"}
        self.client.addAttachments(doc, {"file1": "value", "file2": "second value"})
        self.assertEquals(doc["_attachments"],
            {'file2': {'data': 'c2Vjb25kIHZhbHVl', 'type': 'base64'},
             'file1': {'data': 'dmFsdWU=', 'type': 'base64'}})


    def test_openView(self):
        """
        Test openView.
        """
        d = self.client.openView("mydb", "viewdoc", "myview")
        self.assertEquals(self.client.uri, "/mydb/_design/viewdoc/_view/myview")
        self.assertEquals(self.client.kwargs["method"], "GET")
        return self._checkParseDeferred(d)


    def test_openViewWithQuery(self):
        """
        Test openView with query arguments.
        """
        d = self.client.openView("mydb",
                                 "viewdoc",
                                 "myview",
                                 startkey="foo",
                                 count=10)
        self.assertEquals(self.client.kwargs["method"], "GET")
        self.failUnless(
            self.client.uri.startswith("/mydb/_design/viewdoc/_view/myview"))
        query = cgi.parse_qs(self.client.uri.split('?', 1)[-1])
        self.assertEquals(query["startkey"], ['"foo"'])
        self.assertEquals(query["count"], ["10"])
        return self._checkParseDeferred(d)

    def test_openViewWithKeysQuery(self):
        """
        Test openView handles couchdb's strange requirements for keys arguments
        """
        d = self.client.openView("mydb2",
                                 "viewdoc2",
                                 "myview2",
                                 keys=[1,3,4, "hello, world", {1: 5}],
                                 count=5)
        self.assertEquals(self.client.kwargs["method"], "POST")
        self.failUnless(
            self.client.uri.startswith('/mydb2/_design/viewdoc2/_view/myview2'))
        query = cgi.parse_qs(self.client.uri.split('?', 1)[-1])
        self.assertEquals(query, dict(count=['5']))
        self.assertEquals(self.client.kwargs['postdata'], 
                          {'keys': '[1, 3, 4, "hello, world", {"1": 5}]'})
        
    def test_tempView(self):
        """
        Test tempView.
        """
        d = self.client.tempView("mydb", "js code")
        self.assertEquals(self.client.uri, "/mydb/_temp_view")
        self.assertEquals(self.client.kwargs["postdata"], "js code")
        self.assertEquals(self.client.kwargs["method"], "POST")
        return self._checkParseDeferred(d)


    def test_addViews(self):
        """
        Test addViews.
        """
        doc = {"value": "bar"}
        self.client.addViews(doc, {"view1": "js code 1", "view2": "js code 2"})
        self.assertEquals(doc["views"], {"view1": "js code 1", "view2": "js code 2"})


    def test_bindToDB(self):
        """
        Test bindToDB, calling a bind method afterwards.
        """
        self.client.bindToDB("mydb")
        d = self.client.listDoc()
        self.assertEquals(self.client.uri, "/mydb/_all_docs")
        self.assertEquals(self.client.kwargs["method"], "GET")
        return self._checkParseDeferred(d)

    def test_escapeId(self):
        d = self.client.openDoc("mydb", "my doc with spaces")
        self.assertEquals(self.client.uri, "/mydb/my%20doc%20with%20spaces")
        self.assertEquals(self.client.kwargs["method"], "GET")
        return self._checkParseDeferred(d)


class FakeCouchDBResource(resource.Resource):
    """
    Fake a couchDB resource.

    @ivar result: value set in tests to be returned by the resource.
    @param result: C{str}
    """
    result = ""

    def getChild(self, path, request):
        """
        Return self as only child.
        """
        return self


    def render(self, request):
        """
        Return C{result}.
        """
        return self.result



class ConnectedCouchDBTestCase(TestCase):
    """
    Test C{CouchDB} with a real web server.
    """

    def setUp(self):
        """
        Create a web server and a client bound to it.
        """
        self.resource = FakeCouchDBResource()
        site = server.Site(self.resource)
        port = reactor.listenTCP(0, site, interface="127.0.0.1")
        self.addCleanup(port.stopListening)
        self.client = paisley.CouchDB("127.0.0.1", port.getHost().port)


    def test_createDB(self):
        """
        Test listDB.
        """
        data = [u"mydb"]
        self.resource.result = json.dumps(data)
        d = self.client.listDB()
        def cb(result):
            self.assertEquals(result, data)
        d.addCallback(cb)
        return d

