# -*- Mode: Python -*-
# vi:si:et:sw=4:sts=4:ts=4

# Copyright (c) 2007-2008
# See LICENSE for details.

import re
import os
import tempfile
import subprocess
import time

from twisted.trial import unittest

from paisley import client


class CouchDBWrapper(object):
    """
    I wrap an external CouchDB instance started and stopped for testing.

    @ivar tempdir: the temporary directory used for logging and running
    @ivar process: the CouchDB process
    @type process: L{subprocess.Popen}
    @ivar port:    the randomly assigned port on which CouchDB listens
    @type port:    str
    @ivar db:      the CouchDB client to this server
    @type db:      L{client.CouchDB}
    """

    def start(self):
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

        args = ['couchdb', '-a', confPath]
        null = open('/dev/null', 'w')
        self.process = subprocess.Popen(
            args, env=None, stdout=subprocess.PIPE, stderr=subprocess.PIPE)

        # find port
        logPath = os.path.join(self.tempdir, 'log', 'couch.log')
        while not os.path.exists(logPath):
            if self.process.poll() is not None:
                raise Exception("""
couchdb exited with code %d.
stdout:
%s
stderr:
%s""" % (
                    self.process.returncode, self.process.stdout.read(),
                    self.process.stderr.read()))
            time.sleep(0.01)

        while os.stat(logPath).st_size == 0:
            time.sleep(0.01)

        PORT_RE = re.compile(
            'Apache CouchDB has started on http://127.0.0.1:(?P<port>\d+)')

        handle = open(logPath)
        line = handle.read()
        m = PORT_RE.search(line)
        if not m:
            self.stop()
            raise Exception("Cannot find port in line %s" % line)

        self.port = int(m.group('port'))
        self.db = client.CouchDB(host='localhost', port=self.port, username='testpaisley', password='testpaisley')

    def stop(self):
        self.process.terminate()

        os.system("rm -rf %s" % self.tempdir)


class CouchDBTestCase(unittest.TestCase):
    """
    I am a TestCase base class for tests against a real CouchDB server.
    I start a server during setup and stop it during teardown.

    @ivar  db: the CouchDB client
    @type  db: L{client.CouchDB}
    """

    def setUp(self):
        self.wrapper = CouchDBWrapper()
        self.wrapper.start()
        self.db = self.wrapper.db

    def tearDown(self):
        self.wrapper.stop()
