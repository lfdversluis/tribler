import time
import random

from Tribler.Test.test_as_server import AbstractServer
from Tribler.Core.Session import Session
from Tribler.Core.SessionConfig import SessionStartupConfig
from Tribler.Core.CacheDB.sqlitecachedb import SQLiteCacheDB

from twisted.internet.defer import Deferred
from twisted.internet import threads


class TestDatabaseDeferreds(AbstractServer):

    BEGIN_INSTANCE_SIZE = 1000
    END_INSTANCE_SIZE = 10000
    STEP_SIZE = 1000

    MAX_ID = 1000
    NUM_QUERIES = 1000

    def setUp(self):
        super(TestDatabaseDeferreds, self).setUp()
        self.config = SessionStartupConfig()
        self.config.set_state_dir(self.getStateDir())
        self.config.set_torrent_checking(False)
        self.config.set_multicast_local_peer_discovery(False)
        self.config.set_megacache(False)
        self.config.set_dispersy(False)
        self.config.set_mainline_dht(False)
        self.config.set_torrent_collecting(False)
        self.config.set_libtorrent(False)
        self.config.set_dht_torrent_collecting(False)
        self.config.set_videoplayer(False)
        self.session = Session(self.config, ignore_singleton=True)

        self.sqlite_test = SQLiteCacheDB(self.session)
        self.db_path = u":deferred:"
        self.sqlite_test.initialize(self.db_path)

    def tearDown(self):
        self.delete_table()
        super(TestDatabaseDeferreds, self).tearDown()
        self.sqlite_test.close()
        self.sqlite_test = None
        self.session.del_instance()
        self.session = None

    def test_create_db(self):
        sql = u"CREATE TABLE IF NOT EXISTS unit_test(id INTEGER NOT NULL);"
        self.sqlite_test.execute(sql)

    def test_synchronous_calls(self):
        self.counter = 0
        self.global_time = time.time()
        for s in range(self.BEGIN_INSTANCE_SIZE, self.END_INSTANCE_SIZE +1, self.STEP_SIZE):
            self.test_create_db()
            random.seed(1337)

            for i in range(0, self.STEP_SIZE):
                id = random.randint(0, self.MAX_ID)
                self.add_id(id)

            def query_time(self):
                for i in range(0, self.NUM_QUERIES):
                    if i % 2 == 0:
                        self.print_item(self.get_count())
                    else:
                        self.print_item(self.get_unique_count())

            start_time = time.time()
            query_time(self)

            print "synchronous %s %s" % (s, (time.time() - start_time))
        print self.counter

    def test_asynchrnous_calls(self):
        self.global_time = time.time()
        self.counter = 0
        for s in range(self.BEGIN_INSTANCE_SIZE, self.END_INSTANCE_SIZE + 1, self.STEP_SIZE):
            self.test_create_db()
            random.seed(1337)

            for i in range(0, self.STEP_SIZE):
                id = random.randint(0, self.MAX_ID)
                self.add_id(id)

            def query_time(self):
                for i in range(0, self.NUM_QUERIES):
                    if i % 2 == 0:
                        d = Deferred(self.get_count)
                        d.addCallback(self.print_item)
                    else:
                        d = Deferred(self.get_unique_count)
                        d.addCallback(self.print_item)

            start_time = time.time()
            query_time(self)

            print "asynchronous %s %s" % (s, (time.time() - start_time))

    def test_async_defertoThread(self):
        self.global_time = time.time()
        self.counter = 0
        for s in range(self.BEGIN_INSTANCE_SIZE, self.END_INSTANCE_SIZE + 1, self.STEP_SIZE):
            self.test_create_db()
            random.seed(1337)

            for i in range(0, self.STEP_SIZE):
                id = random.randint(0, self.MAX_ID)
                self.add_id(id)

            def query_time(self):
                for i in range(0, self.NUM_QUERIES):
                    if i % 2 == 0:
                        d = threads.deferToThread(self.get_count)
                        d.addCallback(self.print_item)
                    else:
                        d = threads.deferToThread(self.get_unique_count)
                        d.addCallback(self.print_item)

            start_time = time.time()
            query_time(self)

            print "defertothread %s %s" % (s, (time.time() - start_time))

    def print_item(self, item):
        self.counter += 1
        if self.counter == (self.END_INSTANCE_SIZE / self.STEP_SIZE) * self.NUM_QUERIES:
            print "done %s" % ((time.time() - self.global_time))

    def add_id(self, id):
        self.sqlite_test.insert('unit_test', id=id)

    def get_count(self):
        count = self.sqlite_test.fetchone(u"SELECT COUNT(id) FROM unit_test")
        return count

    def get_unique_count(self):
        count = self.sqlite_test.fetchone(u"SELECT COUNT(DISTINCT id) FROM unit_test")
        return count

    def delete_table(self):
        self.sqlite_test.execute(u"DROP TABLE unit_test")

