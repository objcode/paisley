# -*- Mode: Python; test-case-name: paisley.test.test_client -*-
# vi:si:et:sw=4:sts=4:ts=4

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

from twisted.internet import defer

from twisted.trial.unittest import TestCase
from twisted.internet.defer import Deferred
from twisted.internet import reactor
from twisted.web import resource, server

import paisley

from paisley.test import test_util

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

    def test_disable_log(self):
        client = TestableCouchDB('localhost', disable_log=True)
        import logging
        log = logging.getLogger('paisley')
        self.assertNotEqual(log, client.log)

    def test_enable_log_and_defaults(self):
        client = TestableCouchDB('localhost')
        import logging
        log = logging.getLogger('paisley')
        self.assertEqual(log, client.log)
    
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


    def test_listDocLimit(self):
        """
        Test listDoc with a limit.
        """
        d = self.client.listDoc("mydb", limit=3)
        self.assertEquals(self.client.uri, "/mydb/_all_docs?limit=3")
        self.assertEquals(self.client.kwargs["method"], "GET")
        return self._checkParseDeferred(d)


    def test_listDocMultipleArguments(self):
        """
        Test listDoc with all options activated.
        """
        d = self.client.listDoc("mydb", limit=3, startKey=1, reverse=True)
        self.assertEquals(self.client.uri, "/mydb/_all_docs?startkey=1&limit=3&reverse=true")
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
        self.assertEquals(self.client.uri, "/mydb/_design/viewdoc/_view/myview?")
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
                                 limit=10)
        self.assertEquals(self.client.kwargs["method"], "GET")
        self.failUnless(
            self.client.uri.startswith("/mydb/_design/viewdoc/_view/myview"))
        query = cgi.parse_qs(self.client.uri.split('?', 1)[-1])
        # couchdb expects valid JSON as the query values, so a string of foo
        # should be serialized as "foo" explicitly
        # e.g., ?startkey=A would return
        # {"error":"bad_request","reason":"invalid UTF-8 JSON"}
        self.assertEquals(query["startkey"], ['"foo"'])
        self.assertEquals(query["limit"], ["10"])
        return self._checkParseDeferred(d)

    def test_openViewWithKeysQuery(self):
        """
        Test openView handles couchdb's strange requirements for keys arguments
        """
        d = self.client.openView("mydb2",
                                 "viewdoc2",
                                 "myview2",
                                 keys=[1,3,4, "hello, world", {1: 5}],
                                 limit=5)
        self.assertEquals(self.client.kwargs["method"], "POST")
        self.failUnless(
            self.client.uri.startswith('/mydb2/_design/viewdoc2/_view/myview2'))
        query = cgi.parse_qs(self.client.uri.split('?', 1)[-1])
        self.assertEquals(query, dict(limit=['5']))
        self.assertEquals(self.client.kwargs['postdata'], 
                          '{"keys": [1, 3, 4, "hello, world", {"1": 5}]}')
        
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
    @type result: C{str}
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

class RealCouchDBTestCase(TestCase):
    
    def setUp(self):
        self.wrapper = test_util.CouchDBWrapper()
        self.wrapper.start()
        self.db = self.wrapper.db
        self.bound = False
        self.db_name = 'test'
        return self._resetDatabase()

    def tearDown(self):
        self.wrapper.stop()
        pass
        
    def _resetDatabase(self):
        """
        Helper method to create an empty test database, deleting the existing one if required.
        Used to clean up before running each test.
        """
        d = defer.Deferred()
        d.addCallback(lambda _: self._deleteTestDatabaseIfExists())
        d.addCallback(lambda _: self.db.createDB(self.db_name))
        def createOkCb(result):
            self.assertEquals(result, {'ok': True})
        d.addCallback(createOkCb)
        d.addCallback(lambda _: self.db.infoDB(self.db_name))
        def checkInfoNewDatabase(result):
            self.assertEquals(result['update_seq'], 0)
            self.assertEquals(result['purge_seq'], 0)
            self.assertEquals(result['doc_count'], 0)
            self.assertEquals(result['db_name'], 'test')
            self.assertEquals(result['doc_del_count'], 0)
            self.assertEquals(result['committed_update_seq'], 0)
        d.addCallback(checkInfoNewDatabase)
        # We need to know the version to perform the tests
        #   Ideally the client class would trigger this automatically
        d.addCallback(lambda _: self.db.getVersion())
        d.callback(None)
        return d
            
    def _deleteTestDatabaseIfExists(self):
        """
        Helper method to delete the test database, wether it exists or not.
        Used to clean up before running each test.
        """
        d = defer.Deferred()
        if self.bound :
            d.addCallback(lambda _: self.db.deleteDB())
        else          :
            d.addCallback(lambda _: self.db.deleteDB(self.db_name))
        def deleteCb(result):
            self.assertEquals(result, {'ok': True})
        def deleteFailedCb(failure):
            pass
        d.addCallbacks(deleteCb, deleteFailedCb)
        d.callback(None)
        return d
        
    def _saveDoc(self, body, doc_id):
        """
        Helper method to save a document, and verify that it was successfull.
        """
        d = defer.Deferred()
        if self.bound :
            d.addCallback(lambda _: self.db.saveDoc(body, doc_id))
        else          :
            d.addCallback(lambda _: self.db.saveDoc(self.db_name, body, doc_id))
        def checkDocumentCreated(result):
            self.assertEquals(result['ok'], True)
            if doc_id != None : self.assertEquals(result['id'], doc_id)
            self._rev = result['rev']
        d.addCallback(checkDocumentCreated)
        d.callback(None)
        return d

    def testDB(self):
        d = defer.Deferred()
        d.addCallback(lambda _: self._deleteTestDatabaseIfExists())
        d.addCallback(lambda _: self.db.getVersion())
        d.addCallback(lambda _: self.db.createDB('test'))
        def createCb(result):
            self.assertEquals(result, {'ok': True})
        d.addCallback(createCb)
        d.addCallback(lambda _: self.db.listDB())
        def listCb(result):
            if self.db.version.__ge__((1,1,0)):
                self.assertEquals(len(result), 3)
                self.failUnless('_replicator' in result)
            else:
                self.assertEquals(len(result), 2)
            self.failUnless('test' in result)
            self.failUnless('_users' in result)
        d.addCallback(listCb)
        d.addCallback(lambda _: self.db.saveDoc('test', {'number': 1}, '1'))
        def saveDoc(result):
            self.assertEquals(result[u'ok'], True)
            self.assertEquals(result[u'id'], u'1')
            # save document revision for comparison later
            self.doc_rev = result[u'rev']
        d.addCallback(saveDoc)
        doc = {}
        self.db.addViews(doc, {'test': {'map': 'function (doc) { emit(doc.number, doc) }'}})
        d.addCallback(lambda _: self.db.saveDoc('test', doc, '_design/test'))
        def addViewCb(result):
            self.assertEquals(result[u'ok'], True)
        d.addCallback(addViewCb)
        d.addCallback(lambda _: self.db.openView('test', 'test', 'test'))
        def openView1Cb(result):
            self.assertEquals(result[u'total_rows'], 1)
            self.assertEquals(result[u'offset'], 0)
            self.assertEquals(result[u'rows'][0][u'id'], u'1')
            self.assertEquals(result[u'rows'][0][u'key'], 1)
            self.assertEquals(result[u'rows'][0][u'value'][u'_id'], u'1')
            self.assertEquals(result[u'rows'][0][u'value'][u'number'], 1)
            self.assertEquals(result[u'rows'][0][u'value'][u'_rev'], self.doc_rev)
        d.addCallback(openView1Cb)
        d.addCallback(lambda _: self.db.openView('test', 'test', 'test', keys = [1]))
        def openView2Cb(result):
            self.assertEquals(result[u'total_rows'], 1)
            self.assertEquals(result[u'offset'], 0)
            self.assertEquals(result[u'rows'][0][u'id'], u'1')
            self.assertEquals(result[u'rows'][0][u'key'], 1)
            self.assertEquals(result[u'rows'][0][u'value'][u'_id'], u'1')
            self.assertEquals(result[u'rows'][0][u'value'][u'number'], 1)
            self.assertEquals(result[u'rows'][0][u'value'][u'_rev'], self.doc_rev)
        d.addCallback(openView2Cb)
        d.addCallback(lambda _: self.db.openView('test', 'test', 'test', keys = [0]))
        def openView3Cb(result):
            self.assertEquals(result[u'total_rows'], 1)
            self.assertEquals(result[u'offset'], 0)
            self.assertEquals(result[u'update_seq'], 2)
            self.assertEquals(result[u'rows'], [])
        d.addCallback(openView3Cb)
        d.addCallback(lambda _: self.db.deleteDB('test'))
        def deleteCb(result):
            self.assertEquals(result, {'ok': True})
        d.addCallback(deleteCb)
        d.addCallback(lambda _: self.db.listDB())
        def listCbAgain(result):
            if self.db.version.__ge__((1,1,0)):
                self.assertEquals(len(result), 2)
            else:
                self.assertEquals(len(result), 1)
            self.failUnless('_users' in result)
        d.addCallback(listCbAgain)

        d.callback(None)
        return d

    def test_createDB(self):
        """
        Test createDB: this should C{PUT} the DB name in the uri.
        """
        d = defer.Deferred()
        # Since during setUp we already create the database, and here we are
        #   specifically testing the creation, we need to delete it first
        d.addCallback(lambda _: self._deleteTestDatabaseIfExists())
        d.addCallback(lambda _: self.db.createDB(self.db_name))
        def createCb(result):
            self.assertEquals(result, {'ok': True})
        d.addCallback(createCb)
        d.callback(None)
        return d

    def test_deleteDB(self):
        """
        Test deleteDB: this should C{DELETE} the DB name.
        """
        d = defer.Deferred()
        d.addCallback(lambda _: self.db.deleteDB(self.db_name))
        def deleteCb(result):
            self.assertEquals(result, {'ok': True})
        d.addCallback(deleteCb)
        d.callback(None)
        return d

    def test_listDB(self):
        """
        Test listDB: this should C{GET} a specific uri.
        """
        d = defer.Deferred()
        d.addCallback(lambda _: self.db.listDB())
        def listCb(result):
            if self.db.version.__ge__((1,1,0)):
                self.assertEquals(len(result), 3)
                self.failUnless('_replicator' in result)
            else:
                self.assertEquals(len(result), 2)
            self.failUnless('test' in result)
            self.failUnless('_users' in result)
        d.addCallback(listCb)
        d.callback(None)
        return d

    def test_infoDB(self):
        """
        Test infoDB: this should C{GET} the DB name.
        """
        d = defer.Deferred()
        # Get info about newly created database
        d.addCallback(lambda _: self.db.infoDB(self.db_name))
        def checkInfoNewDatabase(result):
            self.assertEquals(result['update_seq'], 0)
            self.assertEquals(result['purge_seq'], 0)
            self.assertEquals(result['doc_count'], 0)
            self.assertEquals(result['db_name'], 'test')
            self.assertEquals(result['doc_del_count'], 0)
            self.assertEquals(result['committed_update_seq'], 0)
        d.addCallback(checkInfoNewDatabase)
        d.callback(None)
        return d

    def test_listDoc(self):
        """
        Test listDoc.
        """
        d = defer.Deferred()
        # List documents in newly created database
        d.addCallback(lambda _: self.db.listDoc(self.db_name))
        def checkDatabaseEmpty(result):
            self.assertEquals(result['rows'], [])
            self.assertEquals(result['total_rows'], 0)
            self.assertEquals(result['offset'], 0)
        d.addCallback(checkDatabaseEmpty)
        d.callback(None)
        return d

    def test_listDocReversed(self):
        """
        Test listDoc reversed.
        """
        d = defer.Deferred()
        # List documents in newly created database
        d.addCallback(lambda _: self.db.listDoc(self.db_name, reverse=True))
        def checkDatabaseEmpty(result):
            self.assertEquals(result['rows'], [])
            self.assertEquals(result['total_rows'], 0)
            self.assertEquals(result['offset'], 0)
        d.addCallback(checkDatabaseEmpty)
        d.callback(None)
        return d

    def test_listDocStartKey(self):
        """
        Test listDoc with a startKey.
        """
        d = defer.Deferred()
        # List documents in newly created database
        d.addCallback(lambda _: self.db.listDoc(self.db_name, startKey=2))
        def checkDatabaseEmpty(result):
            self.assertEquals(result['rows'], [])
            self.assertEquals(result['total_rows'], 0)
            self.assertEquals(result['offset'], 0)
        d.addCallback(checkDatabaseEmpty)
        d.callback(None)
        return d

    def test_listDocLimit(self):
        """
        Test listDoc with a limit.
        """
        d = defer.Deferred()
        # List documents in newly created database
        d.addCallback(lambda _: self.db.listDoc(self.db_name, limit=3))
        def checkDatabaseEmpty(result):
            self.assertEquals(result['rows'], [])
            self.assertEquals(result['total_rows'], 0)
            self.assertEquals(result['offset'], 0)
        d.addCallback(checkDatabaseEmpty)
        d.callback(None)
        return d

    def test_listDocMultipleArguments(self):
        """
        Test listDoc with all options activated.
        """
        d = defer.Deferred()
        # List documents in newly created database
        d.addCallback(lambda _: self.db.listDoc(self.db_name, limit=3, startKey=1, reverse=True))
        def checkDatabaseEmpty(result):
            self.assertEquals(result['rows'], [])
            self.assertEquals(result['total_rows'], 0)
            self.assertEquals(result['offset'], 0)
        d.addCallback(checkDatabaseEmpty)
        d.callback(None)
        return d

    def test_openDoc(self):
        """
        Test openDoc.
        """
        d = defer.Deferred()
        doc_id = 'foo'
        body = {"value": "mybody"}
        d.addCallback(lambda _: self._saveDoc(body, doc_id))
        d.addCallback(lambda _: self.db.openDoc(self.db_name, doc_id))
        def checkDoc(result):
            self.assertEquals(result['_id'], doc_id)
            self.assertEquals(result['value'], 'mybody')
        d.addCallback(checkDoc)
        d.callback(None)
        return d

    def test_saveDocWithDocId(self):
        """
        Test saveDoc, giving an explicit document ID.
        """
        d = defer.Deferred()
        # Save simple document and check the result
        doc_id = 'foo'
        body = { }
        d.addCallback(lambda _: self._saveDoc(body, doc_id))
        d.callback(None)
        return d

    def test_saveDocWithoutDocId(self):
        """
        Test saveDoc without a document ID.
        """
        d = defer.Deferred()
        doc_id = None
        body = { }
        d.addCallback(lambda _: self._saveDoc(body, doc_id))
        d.callback(None)
        return d

    def test_saveStructuredDoc(self):
        """
        saveDoc should automatically serialize a structured document.
        """
        d = defer.Deferred()
        doc_id = 'foo'
        body = {"value": "mybody", "_id": doc_id}
        d.addCallback(lambda _: self._saveDoc(body, doc_id))
        d.addCallback(lambda _: self.db.openDoc(self.db_name, doc_id))
        def checkDocumentContent(result):
            #self.assertEquals(result['_id'], "AAA")
            self.assertEquals(result['_id'], doc_id)
            self.assertEquals(result['value'], 'mybody')
        d.addCallback(checkDocumentContent)
        d.callback(None)
        return d

    def test_deleteDoc(self):
        """
        Test deleteDoc.
        """
        d = defer.Deferred()
        doc_id = 'foo'
        body = {"value": "mybody", "_id": doc_id}
        d.addCallback(lambda _: self._saveDoc(body, doc_id))
        d.addCallback(lambda _: self.db.deleteDoc(self.db_name, doc_id, self._rev))
        def checkDocumentDeleted(result):
            self.assertEquals(result['id'], doc_id)
            self.assertEquals(result['ok'], True)
        d.addCallback(checkDocumentDeleted)
        d.callback(None)
        return d
    
    def test_addAttachments(self):
        """
        Test addAttachments.
        """
        doc_id = 'foo'
        d = defer.Deferred()
        body = {"value": "mybody", "_id": doc_id}
        attachments = {"file1": "value", "file2": "second value"}
        d.addCallback(lambda _: self.db.addAttachments(body, attachments))
        d.addCallback(lambda _: self._saveDoc(body, doc_id))
        d.addCallback(lambda _: self.db.openDoc(self.db_name, doc_id))
        def checkAttachments(result):
            self.failUnless('file1' in result["_attachments"])
            self.failUnless('file2' in result["_attachments"])
            self.assertEquals(result['_id'], doc_id)
            self.assertEquals(result['value'], 'mybody')
        d.addCallback(checkAttachments)
        d.callback(None)
        return d

    #def test_openView(self):
    # This is already covered by test_addViews
    
    def test_openViewWithKeysQuery(self):
        """
        Test openView handles couchdb's strange requirements for keys arguments
        """
        d = defer.Deferred()
        #d = Deferred()
        doc_id = 'foo'
        body = {"value": "bar"}
        view1_id = 'view1'
        view1 = ''' function(doc) {
        emit(doc._id, doc);
        }'''
        views = {view1_id: { 'map': view1 } }
        d.addCallback(lambda _: self.db.addViews(body, views))
        d.addCallback(lambda _: self._saveDoc(body, '_design/' + doc_id))
        keys=[
            {
                'startkey': [ "a", "b", "c" ],
                'endkey' : ["x", "y", "z" ]
            },
            {
                'startkey': [ "a", "b", "c" ],
                'endkey' : ["x", "y", "z" ]
            }
        ]
        d.addCallback(lambda _: self.db.openView(self.db_name, doc_id, view1_id, keys=keys, limit=5))
        def checkOpenView(result):
            self.assertEquals(result["rows"], [])
            self.assertEquals(result["total_rows"], 0)
            self.assertEquals(result["offset"], 0)
        d.addCallback(checkOpenView)
        d.callback(None)
        return d
        
    def test_tempView(self):
        """
        Test tempView.
        """
        d = defer.Deferred()
        view1 = ''' function(doc) { emit(doc._id, doc); } '''
        view1 = ''' function(doc) {
        emit(doc._id, doc);
        }'''
        doc = { 'map': view1 }
        d.addCallback(lambda _: self.db.tempView(self.db_name, doc))
        def checkView(result):
            self.assertEquals(result['rows'], [])
            self.assertEquals(result['total_rows'], 0)
            self.assertEquals(result['offset'], 0)
        d.addCallback(checkView)
        d.callback(None)
        return d

    def test_addViews(self):
        """
        Test addViews.
        """
        d = defer.Deferred()
        doc_id = 'foo'
        #d = Deferred()
        body = {"value": "bar"}
        view1 = ''' function(doc) {
        emit(doc._id, doc);
        }'''
        view2 = ''' function(doc) {
        emit(doc._id, doc);
        }'''
        views = {"view1": { 'map': view1 } , "view2": { 'map' : view2 }}
        d.addCallback(lambda _: self.db.addViews(body, views))
        d.addCallback(lambda _: self._saveDoc(body, '_design/' + doc_id))
        d.addCallback(lambda _: self.db.openDoc(self.db_name, '_design/' + doc_id))
        def checkViews(result):
            self.failUnless(result["views"]['view1']['map'] == view1 )
            self.failUnless(result["views"]['view2']['map'] == view2 )
            self.assertEquals(result['_id'], '_design/' + doc_id)
            self.assertEquals(result['value'], 'bar')
        d.addCallback(checkViews)
        d.addCallback(lambda _: self.db.openView(self.db_name, doc_id, 'view1'))
        def checkOpenView(result):
            self.assertEquals(result["rows"], [ ])
            self.assertEquals(result["total_rows"], 0)
            self.assertEquals(result["offset"], 0)
        d.addCallback(checkOpenView)
        d.addCallback(lambda _: self.db.openView(self.db_name, doc_id, 'view2'))
        d.addCallback(checkOpenView)
        d.callback(None)
        return d

    def test_bindToDB(self):
        """
        Test bindToDB, calling a bind method afterwards.
        """
        d = defer.Deferred()
        doc_id = 'foo'
        body = {"value": "bar"}
        self.db.bindToDB(self.db_name)
        self.bound = True
        d.addCallback(lambda _: self._saveDoc(body, '_design/' + doc_id))
        d.addCallback(lambda _: self.db.listDoc(self.db_name))
        def checkViews(result):
            self.assertEquals(result['total_rows'], 1)
            self.assertEquals(result['offset'], 0)
        d.addCallback(checkViews)
        d.callback(None)
        return d

    def test_escapeId(self):
        d = defer.Deferred()
        doc_id = 'my doc with spaces'
        body = {"value": "bar"}
        d.addCallback(lambda _: self._saveDoc(body, doc_id))
        d.addCallback(lambda _: self.db.openDoc(self.db_name, doc_id))
        def checkDoc(result):
            self.assertEquals(result['_id'], doc_id)
            self.assertEquals(result['value'], 'bar')
        d.addCallback(checkDoc)
        d.callback(None)
        return d
    
class UnicodeTestCase(test_util.CouchDBTestCase):

    def setUp(self):
        test_util.CouchDBTestCase.setUp(self)
        d = self.db.createDB('test')
        def createCb(result):
            self.assertEquals(result, {'ok': True})
        d.addCallback(createCb)
        return d

    def tearDown(self):
        d = self.db.deleteDB('test')
        def deleteCb(result):
            self.assertEquals(result, {'ok': True})
        d.addCallback(deleteCb)
        d.addCallback(lambda _: test_util.CouchDBTestCase.tearDown(self))
        return d

    def testUnicodeContents(self):
        name = u'\xc3\xa9preuve'

        d = defer.Deferred()

        d.addCallback(lambda _: self.db.saveDoc('test', {
            'name': name,
            name: 'name',
            }))
        d.addCallback(lambda r: self.db.openDoc('test', r['id']))
        def check(r):
            self.assertEquals(r['name'], name)
            self.assertEquals(r[name], u'name')
            self.assertEquals(type(r['name']), unicode)
            self.assertEquals(type(r[name]), unicode)
        d.addCallback(check)
        d.callback(None)
        return d

    def testUnicodeId(self):
        docId = u'\xc3\xa9preuve'

        d = defer.Deferred()

        d.addCallback(lambda _: self.db.saveDoc('test', {
            'name': 'name',
            }, docId=docId))

        def saveDocCb(r):
            self.assertEquals(r['id'], docId)
            return self.db.openDoc('test', r['id'])
        d.addCallback(saveDocCb)

        def check(r):
            self.assertEquals(r[u'name'], u'name')
            self.assertEquals(type(r['name']), unicode)
            self.assertEquals(r[u'_id'], docId)
            self.assertEquals(type(r[u'_id']), unicode)
            self.assertEquals(type(r[u'_rev']), unicode)

            # open again, with revision
            return self.db.openDoc('test', r['_id'], revision=r['_rev'])
        d.addCallback(check)

        def checkRevisioned(r):
            self.assertEquals(r[u'name'], u'name')
            self.assertEquals(type(r['name']), unicode)
            self.assertEquals(r[u'_id'], docId)
            self.assertEquals(type(r[u'_id']), unicode)
            self.assertEquals(type(r[u'_rev']), unicode)
            return r
        d.addCallback(checkRevisioned)

        d.addCallback(lambda r: self.db.deleteDoc(
            'test', r[u'_id'], r[u'_rev']))

        d.callback(None)
        return d
