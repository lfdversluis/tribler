import datetime
import os
from math import pow
from twisted.internet.defer import inlineCallbacks

from nose.twistedtools import deferred

from Tribler.Test.Community.Multichain.test_multichain_utilities import TestBlock, MultiChainTestCase
from Tribler.community.multichain.database import MultiChainDB
from Tribler.community.multichain.database import DATABASE_DIRECTORY


class TestDatabase(MultiChainTestCase):
    """
    Tests the Database for MultiChain community.
    Also tests integration with Dispersy.
    This integration slows down the tests,
    but can probably be removed and a Mock Dispersy could be used.
    """

    def __init__(self, *args, **kwargs):
        super(TestDatabase, self).__init__(*args, **kwargs)

    @inlineCallbacks
    def setUp(self, **kwargs):
        super(TestDatabase, self).setUp()
        path = os.path.join(self.getStateDir(), DATABASE_DIRECTORY)
        if not os.path.exists(path):
            os.makedirs(path)
        self.db = MultiChainDB(None, self.getStateDir())
        yield self.db.initialize()
        self.block1 = TestBlock()
        self.block2 = TestBlock()

    @deferred(timeout=10)
    @inlineCallbacks
    def test_add_block(self):
        # Act
        yield self.db.add_block(self.block1)
        # Assert
        result = yield self.db.get_by_hash_requester(self.block1.hash_requester)
        self.assertEqual_block(self.block1, result)

    @deferred(timeout=10)
    @inlineCallbacks
    def test_get_by_hash(self):
        # Act
        yield self.db.add_block(self.block1)
        # Assert
        result1 = yield self.db.get_by_hash_requester(self.block1.hash_requester)
        result2 = yield self.db.get_by_hash(self.block1.hash_requester)
        result3 = yield self.db.get_by_hash(self.block1.hash_responder)
        self.assertEqual_block(self.block1, result1)
        self.assertEqual_block(self.block1, result2)
        self.assertEqual_block(self.block1, result3)

    @deferred(timeout=10)
    @inlineCallbacks
    def test_add_two_blocks(self):
        # Act
        yield self.db.add_block(self.block1)
        yield self.db.add_block(self.block2)
        # Assert
        result = yield self.db.get_by_hash_requester(self.block2.hash_requester)
        self.assertEqual_block(self.block2, result)

    @deferred(timeout=10)
    @inlineCallbacks
    def test_get_block_non_existing(self):
        # Act
        result = yield self.db.get_by_hash_requester(self.block1.hash_requester)
        # Assert
        self.assertEqual(None, result)

    @deferred(timeout=10)
    @inlineCallbacks
    def test_contains_block_id_positive(self):
        # Act
        yield self.db.add_block(self.block1)
        # Assert
        res = yield self.db.contains(self.block1.hash_requester)
        self.assertTrue(res)

    @deferred(timeout=10)
    @inlineCallbacks
    def test_contains_block_id_negative(self):
        # Act & Assert
        res = yield self.db.contains("NON EXISTING ID")
        self.assertFalse(res)

    @deferred(timeout=10)
    @inlineCallbacks
    def test_get_latest_sequence_number_not_existing(self):
        # Act & Assert
        res = yield self.db.get_latest_sequence_number("NON EXISTING KEY")
        self.assertEquals(res, -1)

    @deferred(timeout=10)
    @inlineCallbacks
    def test_get_latest_sequence_number_public_key_requester(self):
        # Arrange
        # Make sure that there is a responder block with a lower sequence number.
        # To test that it will look for both responder and requester.
        yield self.db.add_block(self.block1)
        self.block2.public_key_responder = self.block1.public_key_requester
        self.block2.sequence_number_responder = self.block1.sequence_number_requester - 5
        yield self.db.add_block(self.block2)
        # Act & Assert
        res = yield self.db.get_latest_sequence_number(self.block1.public_key_requester)
        self.assertEquals(res, self.block1.sequence_number_requester)

    @deferred(timeout=10)
    @inlineCallbacks
    def test_get_latest_sequence_number_public_key_responder(self):
        # Arrange
        # Make sure that there is a requester block with a lower sequence number.
        # To test that it will look for both responder and requester.
        yield self.db.add_block(self.block1)
        self.block2.public_key_requester = self.block1.public_key_responder
        self.block2.sequence_number_requester = self.block1.sequence_number_responder - 5
        yield self.db.add_block(self.block2)
        # Act & Assert
        res = yield self.db.get_latest_sequence_number(self.block1.public_key_responder)
        self.assertEquals(res, self.block1.sequence_number_responder)

    @deferred(timeout=10)
    @inlineCallbacks
    def test_get_previous_id_not_existing(self):
        # Act & Assert
        latest_hash = yield self.db.get_latest_hash("NON EXISTING KEY")
        self.assertEquals(latest_hash, None)

    @deferred(timeout=10)
    @inlineCallbacks
    def test_get_previous_hash_of_requester(self):
        # Arrange
        # Make sure that there is a responder block with a lower sequence number.
        # To test that it will look for both responder and requester.
        yield self.db.add_block(self.block1)
        self.block2.public_key_responder = self.block1.public_key_requester
        self.block2.sequence_number_responder = self.block1.sequence_number_requester + 1
        yield self.db.add_block(self.block2)
        # Act & Assert
        latest_hash = yield self.db.get_latest_hash(self.block2.public_key_responder)
        self.assertEquals(latest_hash, self.block2.hash_responder)

    @deferred(timeout=10)
    @inlineCallbacks
    def test_get_previous_hash_of_responder(self):
        # Arrange
        # Make sure that there is a requester block with a lower sequence number.
        # To test that it will look for both responder and requester.
        yield self.db.add_block(self.block1)
        self.block2.public_key_requester = self.block1.public_key_responder
        self.block2.sequence_number_requester = self.block1.sequence_number_responder + 1
        yield self.db.add_block(self.block2)
        # Act & Assert
        latest_hash = yield self.db.get_latest_hash(self.block2.public_key_requester)
        self.assertEquals(latest_hash, self.block2.hash_requester)

    @deferred(timeout=10)
    @inlineCallbacks
    def test_get_by_sequence_number_by_mid_not_existing(self):
        # Act & Assert
        res = yield self.db.get_by_public_key_and_sequence_number("NON EXISTING KEY", 0)
        self.assertEquals(res, None)

    @deferred(timeout=10)
    @inlineCallbacks
    def test_get_by_public_key_and_sequence_number_requester(self):
        # Arrange
        # Make sure that there is a responder block with a lower sequence number.
        # To test that it will look for both responder and requester.
        yield self.db.add_block(self.block1)
        # Act & Assert
        res = yield self.db.get_by_public_key_and_sequence_number(
            self.block1.public_key_requester, self.block1.sequence_number_requester)
        self.assertEqual_block(self.block1, res)

    @deferred(timeout=10)
    @inlineCallbacks
    def test_get_by_public_key_and_sequence_number_responder(self):
        # Arrange
        # Make sure that there is a responder block with a lower sequence number.
        # To test that it will look for both responder and requester.
        yield self.db.add_block(self.block1)

        # Act & Assert
        res = yield self.db.get_by_public_key_and_sequence_number(
            self.block1.public_key_responder, self.block1.sequence_number_responder)
        self.assertEqual_block(self.block1, res)

    @deferred(timeout=10)
    @inlineCallbacks
    def test_get_total(self):
        # Arrange
        yield self.db.add_block(self.block1)
        self.block2.public_key_requester = self.block1.public_key_responder
        self.block2.sequence_number_requester = self.block1.sequence_number_responder + 1
        self.block2.total_up_requester = self.block1.total_up_responder + self.block2.up
        self.block2.total_down_requester = self.block1.total_down_responder + self.block2.down
        yield self.db.add_block(self.block2)
        # Act
        (result_up, result_down) = yield self.db.get_total(self.block2.public_key_requester)
        # Assert
        self.assertEqual(self.block2.total_up_requester, result_up)
        self.assertEqual(self.block2.total_down_requester, result_down)

    @deferred(timeout=10)
    @inlineCallbacks
    def test_get_total_not_existing(self):
        # Arrange
        yield self.db.add_block(self.block1)
        # Act
        (result_up, result_down) = yield self.db.get_total(self.block2.public_key_requester)
        # Assert
        self.assertEqual(-1, result_up)
        self.assertEqual(-1, result_down)

    @deferred(timeout=10)
    @inlineCallbacks
    def test_save_large_upload_download_block(self):
        """
        Test if the block can save very large numbers.
        """
        # Arrange
        self.block1.total_up_requester = long(pow(2, 62))
        self.block1.total_down_requester = long(pow(2, 62))
        self.block1.total_up_responder = long(pow(2, 61))
        self.block1.total_down_responder = pow(2, 60)
        # Act
        yield self.db.add_block(self.block1)
        # Assert
        result = yield self.db.get_by_hash(self.block1.hash_requester)
        self.assertEqual_block(self.block1, result)

    @deferred(timeout=10)
    @inlineCallbacks
    def test_get_insert_time(self):
        # Arrange
        # Upon adding the block to the database, the timestamp will get added.
        yield self.db.add_block(self.block1)

        # Act
        # Retrieving the block from the database will result in a block with a
        # timestamp
        result = yield self.db.get_by_hash(self.block1.hash_requester)

        insert_time = datetime.datetime.strptime(result.insert_time,
                                                 "%Y-%m-%d %H:%M:%S")

        # We store UTC timestamp
        time_difference = datetime.datetime.utcnow() - insert_time

        # Assert
        self.assertEquals(time_difference.days, 0)
        self.assertLess(time_difference.seconds, 10,
                        "Difference in stored and retrieved time is too large.")

    @deferred(timeout=10)
    @inlineCallbacks
    def set_db_version(self, version):
        yield self.db.executescript(u"UPDATE option SET value = '%d' WHERE key = 'database_version';" % version)
        yield self.db.close(commit=True)
        self.db = MultiChainDB(None, self.getStateDir())
        yield self.db.initialize()

    @deferred(timeout=10)
    @inlineCallbacks
    def test_database_upgrade(self):
        yield self.set_db_version(1)
        version, = yield self.db.fetchone(u"SELECT value FROM option WHERE key = 'database_version' LIMIT 1")
        self.assertEqual(version, u"2")

    @deferred(timeout=10)
    @inlineCallbacks
    def test_database_create(self):
        yield self.set_db_version(0)
        version, = yield self.db.fetchone(u"SELECT value FROM option WHERE key = 'database_version' LIMIT 1")
        self.assertEqual(version, u"2")

    @deferred(timeout=10)
    @inlineCallbacks
    def test_database_no_downgrade(self):
        yield self.set_db_version(200000)
        version, = yield self.db.execute(u"SELECT value FROM option WHERE key = 'database_version' LIMIT 1")
        self.assertEqual(version, u"200000")
