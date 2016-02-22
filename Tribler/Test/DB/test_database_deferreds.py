import time
from random import randint

from Tribler.Test.test_as_server import AbstractServer
from Tribler.Core.Session import Session
from Tribler.Core.SessionConfig import SessionStartupConfig
from Tribler.Core.CacheDB.sqlitecachedb import SQLiteCacheDB

from twisted.internet.defer import Deferred


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
        super(TestDatabaseDeferreds, self).tearDown()
        self.sqlite_test.close()
        self.sqlite_test = None
        self.session.del_instance()
        self.session = None

    def test_create_db(self):
        sql = u"CREATE TABLE IF NOT EXISTS unit_test(id INTEGER NOT NULL);"
        self.sqlite_test.execute(sql)

    def test_synchrnous_calls(self):
        for s in range(self.BEGIN_INSTANCE_SIZE, self.END_INSTANCE_SIZE, self.STEP_SIZE):
            self.test_create_db()
            start_time = time.time()

            for i in range(0, s):
                id = randint(0, self.MAX_ID)
                self.add_id(id)

            for i in range(0, self.NUM_QUERIES):
                if i % 2 == 0:
                    ignored = self.get_count()
                else:
                    ignored = self.get_unique_count()

            print "synchronous %s %s\n" % (s, (time.time() - start_time))
            self.delete_table()

    def test_asynchrnous_calls(self):
        for s in range(self.BEGIN_INSTANCE_SIZE, self.END_INSTANCE_SIZE, self.STEP_SIZE):
            self.test_create_db()
            start_time = time.time()

            for i in range(0, s):
                id = randint(0, self.MAX_ID)
                self.add_id(id)

            for i in range(0, self.NUM_QUERIES):
                if i % 2 == 0:
                    d = Deferred(self.get_count)
                    d.addCallback(self.print_item)
                else:
                    d = Deferred(self.get_unique_count())
                    d.addCallback(self.print_item)

            print "asynchronous %s %s\n" % (s, (time.time() - start_time))
            self.delete_table()

    def print_item(self, item):
        pass

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

