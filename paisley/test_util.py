# Copyright (c) 2007-2008
# See LICENSE for details.

import signal
import re
import os
import tempfile
import subprocess

from twisted.internet import defer
from twisted.trial import unittest

from paisley import client


class CouchDBTestCase(unittest.TestCase):
    """
    I am a TestCase base class for tests against a real CouchDB server.
    I start a server during setup and stop it during teardown.

    @ivar  db: the CouchDB client
    @type  db: L{paisley.client.CouchDB}
    """

    def setUp(self):
        self.tempdir = tempfile.mkdtemp(suffix='.paisley.test')

        path = os.path.join(os.path.dirname(__file__),
            'test.ini.template')
        handle = open(path)

        conf = handle.read() % {
            'tempdir': self.tempdir,
        }

        confPath = os.path.join(self.tempdir, 'test.ini')
        handle = open(confPath, 'w')
        handle.write(conf)
        handle.close()

        # create the dirs from the template
        os.mkdir(os.path.join(self.tempdir, 'lib'))
        os.mkdir(os.path.join(self.tempdir, 'log'))

        args = ['couchdb', '-n', '-a', confPath]
        null = open('/dev/null', 'w')
        process = subprocess.Popen(args, env=None, stdout=null, stderr=null)
        self._pid = process.pid

        # find port
        logPath = os.path.join(self.tempdir, 'log', 'couch.log')
        while not os.path.exists(logPath):
            pass

        while os.stat(logPath).st_size == 0:
            pass

        PORT_RE = re.compile(
            'Apache CouchDB has started on http://127.0.0.1:(?P<port>\d+)')

        handle = open(logPath)
        line = handle.read()
        m = PORT_RE.search(line)
        if not m:
            raise Exception("Cannot find port in line %s" % line)

        self.port = m.group('port')
        self.db = client.CouchDB(host='localhost', port=self.port)

    def tearDown(self):
        # kill
        os.kill(self._pid, signal.SIGTERM)

        # clean up
        os.system("rm -rf %s" % self.tempdir)
