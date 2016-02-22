import os
from os import path
from random import randint
import time

from Tribler.Test.test_as_server import BaseTestCase
from Tribler.dispersy.database import Database

DATABASE_DIRECTORY = path.join(u"sqlite")
""" Path to the database location + dispersy._workingdirectory"""
DATABASE_PATH = path.join(DATABASE_DIRECTORY, u"unittest.db")
""" Version to keep track if the db schema needs to be updated."""
LATEST_DB_VERSION = 1
""" Schema for the MultiChain DB."""
schema = u"""
CREATE TABLE IF NOT EXISTS unit_test(
 id                         INTEGER NOT NULL
 );
"""

TESTS_DIR = os.path.abspath(os.path.dirname(os.path.realpath(__file__)))
TESTS_DATA_DIR = os.path.abspath(os.path.join(TESTS_DIR, u"data"))

class TestDatabaseDispersyDeferreds(BaseTestCase):

    BEGIN_INSTANCE_SIZE = 1000
    END_INSTANCE_SIZE = 1000000
    STEP_SIZE = 1000

    MAX_ID = 1000
    NUM_QUERIES = 1000

    def test_synchrnous_calls(self):
        db = TestDB(TESTS_DATA_DIR)

        for s in range(self.BEGIN_INSTANCE_SIZE, self.END_INSTANCE_SIZE, self.STEP_SIZE):
            start_time = time.time()

            for i in range(0, s):
                id = randint(0, self.MAX_ID)
                db.add_id(id)

            for i in range(0, self.NUM_QUERIES):
                if i % 2 == 0:
                    print db.get_count()
                else:
                    print db.get_unique_count()

            print "Instance size %s time %s\n" % (s, (time.time() - start_time))
            db.truncate_table()





class TestDB(Database):
    """
    This class implements a simple database to use for testing purposes.
    """

    def __init__(self, working_directory):
        """
        Opens the DB to test.
        """

        super(TestDB, self).__init__(path.join(working_directory, DATABASE_PATH))
        self.open()

    def add_id(self, id):
        """
        Adds a single row to the DB.
        """

        self.execute(
            u"INSERT INTO unit_test (id) "
            u"VALUES(?)",
            id)

    def get_count(self):
        """
        Returns the number of rows.
        """

        db_query = u"SELECT COUNT(*) FROM unit_test"
        db_result = self.execute(db_query).fetchone()[0]

        return int(db_result) if db_result else 0

    def get_unique_count(self):
        """
        Returns the amount of unique ids
        """

        db_query = u"SELECT COUNT(*) FROM unit_test GROUP BY id"
        db_result = self.execute(db_query).fetchone()[0]

        return int(db_result) if db_result else 0

    def truncate_table(self):
        db_query = u"TRUNCATE unit_test"
        self.execute(db_query)

    def open(self, initial_statements=True, prepare_visioning=True):
        return super(TestDB, self).open(initial_statements, prepare_visioning)

    def close(self, commit=True):
        return super(TestDB, self).close(commit)

    def check_database(self, database_version):
        """
        Ensure the proper schema is used by the database.
        :param database_version: Current version of the database.
        :return:
        """
        assert isinstance(database_version, unicode)
        assert database_version.isdigit()
        assert int(database_version) >= 0
        database_version = int(database_version)

        if database_version < 1:
            self.executescript(schema)
            self.commit()

        return LATEST_DB_VERSION