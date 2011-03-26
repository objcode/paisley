import paisley

import sys
from twisted.internet import reactor, defer
from twisted.python import log
from twisted.web import client, error, http

client.HTTPClientFactory.noisy = False



def test():
    foo = paisley.CouchDB('localhost')

    print "\nCreate database 'mydb':"
    d = foo.createDB('mydb')
    wfd = defer.waitForDeferred(d)
    yield wfd

    try:
        print wfd.getResult()
    except error.Error, e:
        # FIXME: not sure why Error.status is a str compared to http constants
        if e.status == str(http.UNAUTHORIZED):
            print "\nError: not allowed to create databases"
            reactor.stop()
            return
        else:
            raise

    print "\nList databases on server:"
    d = foo.listDB()
    wfd = defer.waitForDeferred(d)
    yield wfd
    print wfd.getResult()

    print "\nCreate a document 'mydoc' in database 'mydb':"
    doc = """
    {
        "value":
        {
            "Subject":"I like Planktion",
            "Author":"Rusty",
            "PostedDate":"2006-08-15T17:30:12-04:00",
            "Tags":["plankton", "baseball", "decisions"],
            "Body":"I decided today that I don't like baseball. I like plankton."
        }
    }
    """
    d = foo.saveDoc('mydb', doc, 'mydoc')
    wfd = defer.waitForDeferred(d)
    yield wfd
    mydoc = wfd.getResult()
    print mydoc

    print "\nCreate a document, using an assigned docId:"
    d = foo.saveDoc('mydb', doc)
    wfd = defer.waitForDeferred(d)
    yield wfd
    print wfd.getResult()

    print "\nList all documents in database 'mydb'"
    d = foo.listDoc('mydb')
    wfd = defer.waitForDeferred(d)
    yield wfd
    print wfd.getResult()

    print "\nRetrieve document 'mydoc' in database 'mydb':"
    d = foo.openDoc('mydb', 'mydoc')
    wfd = defer.waitForDeferred(d)
    yield wfd
    print wfd.getResult()

    print "\nDelete document 'mydoc' in database 'mydb':"
    d = foo.deleteDoc('mydb', 'mydoc', mydoc['rev'])
    wfd = defer.waitForDeferred(d)
    yield wfd
    print wfd.getResult()

    print "\nList all documents in database 'mydb'"
    d = foo.listDoc('mydb')
    wfd = defer.waitForDeferred(d)
    yield wfd
    print wfd.getResult()

    print "\nList info about database 'mydb':"
    d = foo.infoDB('mydb')
    wfd = defer.waitForDeferred(d)
    yield wfd
    print wfd.getResult()

    print "\nDelete database 'mydb':"
    d = foo.deleteDB('mydb')
    wfd = defer.waitForDeferred(d)
    yield wfd
    print wfd.getResult()

    print "\nList databases on server:"
    d = foo.listDB()
    wfd = defer.waitForDeferred(d)
    yield wfd
    print wfd.getResult()

    reactor.stop()
test = defer.deferredGenerator(test)


if __name__ == "__main__":
    log.startLogging(sys.stdout)
    reactor.callWhenRunning(test)
    reactor.run()
