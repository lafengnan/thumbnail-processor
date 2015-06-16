import unittest
from mock import Mock, patch
from uuid import uuid4
from julep.tiers.queue import HashQueue, ERedisDataMissing, ERedisQueueFull,\
    ERedisKeyNotFound, ERedisReleaseError

from julep.tiers.tier import RedisStore, SwiftStore, PolarisStageStore,\
    TIER_0, TIER_1
from swiftclient_secretkey.client import ClientException

from tests.tiers import redis_driver_version

try:
    from redis import ConnectionError, TimeoutError
except ImportError:
    pass

class TestRedisStore(unittest.TestCase):

    # Redis connection options
    def setUp(self):
        self.user_id = '123432abcdefeacdef'
        self.uuid = uuid4().hex
        self.queue_cls = Mock(spec=HashQueue)
        self.data = Mock()

    @patch('julep.wsgi.HPCStorage.redis_conn')
    def test_upload(self, mock_redis):
        store = RedisStore(mock_redis,
                           TIER_0,
                           self.queue_cls,
                           True,
                           None)
        store.queue.enqueue.return_value = None
        r = store.upload(self.user_id, self.uuid, self.data)
        self.assertIsNone(r)

    @patch('julep.wsgi.HPCStorage.redis_conn')
    def test_upload_key_error(self, mock_redis):
        store = RedisStore(mock_redis,
                           TIER_0,
                           self.queue_cls,
                           True,
                           None)
        store.queue.enqueue.return_value= ValueError
        with self.assertRaises(ValueError) as ar:
            store.upload(None, self.uuid, self.data)
        self.assertIsInstance(ar.exception, ValueError)

    @patch('julep.wsgi.HPCStorage.redis_conn')
    def test_upload_connection_error(self, mock_redis):
        mock_queue = Mock(spec=HashQueue)
        store = RedisStore(mock_redis,
                           TIER_0,
                           mock_queue,
                           True,
                           None)
        store.queue.enqueue.side_effect = ConnectionError
        with self.assertRaises(ConnectionError) as ar:
            store.upload(self.user_id, self.uuid, self.data)
        self.assertIsInstance(ar.exception, ConnectionError)

    @redis_driver_version(major=2, mid=10, minor=3)
    @patch('julep.wsgi.HPCStorage.redis_conn')
    def test_upload_timeout_error(self, mock_redis):
        mock_queue = Mock(spec=HashQueue)
        store = RedisStore(mock_redis,
                           TIER_0,
                           mock_queue,
                           True,
                           None)
        store.queue.enqueue.side_effect = TimeoutError
        with self.assertRaises(TimeoutError) as ar:
            store.upload(self.user_id, self.uuid, self.data)
        self.assertIsInstance(ar.exception, TimeoutError)

    @patch('julep.wsgi.HPCStorage.redis_conn')
    def test_download(self, mock_redis):
        store = RedisStore(mock_redis,
                           TIER_0,
                           self.queue_cls,
                           True,
                           None)
        store.queue.dequeue.return_value = self.uuid, self.data
        k, data = store.download(self.user_id, self.uuid)
        self.assertEqual(data, self.data)
        self.assertEqual(k, self.uuid)

    @patch('julep.wsgi.HPCStorage.redis_conn')
    def test_download_key_error(self, mock_redis):
        store = RedisStore(mock_redis,
                           TIER_0,
                           self.queue_cls,
                           True,
                           None)
        k = data = None
        with self.assertRaises(ValueError) as ar:
            k, data = store.download(None, self.uuid)
        self.assertIsNone(k)
        self.assertIsNone(data)
        self.assertIsInstance(ar.exception, ValueError)

    @patch('julep.wsgi.HPCStorage.redis_conn')
    def test_download_connection_error(self, mock_redis):
        store = RedisStore(mock_redis,
                           TIER_0,
                           self.queue_cls,
                           True,
                           None)
        store.queue.dequeue.side_effect = ConnectionError
        k = data = None
        with self.assertRaises(ConnectionError) as ar:
            k, data = store.download(self.user_id, self.uuid)
        self.assertIsNone(k)
        self.assertIsNone(data)
        self.assertIsInstance(ar.exception, ConnectionError)

    @redis_driver_version(major=2, mid=10, minor=3)
    @patch('julep.wsgi.HPCStorage.redis_conn')
    def test_download_timeout_error(self, mock_redis):
        store = RedisStore(mock_redis,
                           TIER_0,
                           self.queue_cls,
                           True,
                           None)
        store.queue.dequeue.side_effect = TimeoutError
        k = data = None
        with self.assertRaises(TimeoutError) as ar:
            k, data = store.download(self.user_id, self.uuid)
        self.assertIsNone(k)
        self.assertIsNone(data)
        self.assertIsInstance(ar.exception, TimeoutError)

    @patch('julep.wsgi.HPCStorage.redis_conn')
    def test_delete(self, mock_redis):
        store = RedisStore(mock_redis,
                           TIER_0,
                           self.queue_cls,
                           True,
                           None)
        store.queue.release.return_value = 10, 20
        user_len, total_len = store.delete(self.user_id, self.uuid)
        self.assertEqual(10, user_len)
        self.assertEqual(20, total_len)

    @patch('julep.wsgi.HPCStorage.redis_conn')
    def test_delete_key_error(self, mock_redis):
        store = RedisStore(mock_redis,
                           TIER_0,
                           self.queue_cls,
                           True,
                           None)
        len = total_len = -1
        with self.assertRaises(ValueError) as ar:
            len, total_len = store.delete(None, self.uuid)
        self.assertEqual(-1, len)
        self.assertEqual(-1, total_len)
        self.assertIsInstance(ar.exception, ValueError)

    @patch('julep.wsgi.HPCStorage.redis_conn')
    def test_delete_release_error(self, mock_redis):
        store = RedisStore(mock_redis,
                           TIER_0,
                           self.queue_cls,
                           True,
                           None)
        len = total_len = -1
        store.queue.release.side_effect = ERedisReleaseError(self.uuid)
        with self.assertRaises(ERedisReleaseError) as ar:
            len, total_len = store.delete(self.user_id, self.uuid)
        self.assertEqual(-1, len)
        self.assertEqual(-1, total_len)
        self.assertIsInstance(ar.exception, ERedisReleaseError)

    @patch('julep.wsgi.HPCStorage.redis_conn')
    def test_delete_connection_error(self, mock_redis):
        store = RedisStore(mock_redis,
                           TIER_0,
                           self.queue_cls,
                           True,
                           None)
        store.queue.release.side_effect = ConnectionError
        len = total_len = -1
        with self.assertRaises(ConnectionError) as ar:
            len, total_len = store.delete(self.user_id, self.uuid)
        self.assertEqual(-1, len)
        self.assertEqual(-1, total_len)
        self.assertIsInstance(ar.exception, ConnectionError)

    @redis_driver_version(major=2, mid=10, minor=3)
    @patch('julep.wsgi.HPCStorage.redis_conn')
    def test_delete_timeout_error(self, mock_redis):
        store = RedisStore(mock_redis,
                           TIER_0,
                           self.queue_cls,
                           True,
                           None)
        store.queue.release.side_effect = TimeoutError
        len = total_len = -1
        with self.assertRaises(TimeoutError) as ar:
            len, total_len = store.delete(self.user_id, self.uuid)
        self.assertEqual(-1, len)
        self.assertEqual(-1, total_len)
        self.assertIsInstance(ar.exception, TimeoutError)

class TestSwiftStore(unittest.TestCase):

    def setUp(self):
        self.user_id = '123432abcdefeacdef'
        self.uuid = uuid4().hex
        self.data = Mock()
        self.path_format = "{}/appdata/thumbnail/{}.jpg"\
            .format(self.user_id, self.uuid)

    @patch('julep.thumbnail_handlers.get_swift_connection')
    def test_upload(self, mock_swift):
        store = SwiftStore(mock_swift, TIER_1, None)
        container = store.upload(self.user_id, self.uuid, self.data,
                                 container=None, ctype="image/jpeg",
                                 chunk_size=512, path_format=self.path_format)
        self.assertIsNotNone(container)

    @patch('julep.wsgi.HPCStorage.logger')
    @patch('julep.thumbnail_handlers.get_swift_connection')
    def test_upload_exception(self, mock_swift, mock_logger):
        store = SwiftStore(mock_swift, TIER_1, mock_logger)
        with self.assertRaises(Exception) as ar:
            store.upload(self.user_id, self.uuid, self.data,
                         container=None,
                         ctype=None,
                         chunk_size=512,
                         path_format=self.path_format)
        self.assertIsInstance(ar.exception, Exception)

    @patch('julep.wsgi.HPCStorage.logger')
    @patch('julep.thumbnail_handlers.get_swift_connection')
    def test_upload_client_exception(self, mock_swift, mock_logger):
        store = SwiftStore(mock_swift, TIER_1, mock_logger)
        mock_swift.put_object.side_effect = ClientException("upload failed")
        with self.assertRaises(ClientException) as ar:
            store.upload(self.user_id, self.uuid, self.data,
                         container=None,
                         ctype="image/jpeg",
                         chunk_size=512,
                         path_format=self.path_format)
        self.assertIsInstance(ar.exception, ClientException)

    @patch('julep.wsgi.HPCStorage.logger')
    @patch('julep.thumbnail_handlers.get_swift_connection')
    def test_download(self, mock_swift, mock_logger):
        store = SwiftStore(mock_swift, TIER_1, mock_logger)
        mock_swift.get_object.return_value = None, "123"
        data = store.download(self.user_id,
                              self.uuid,
                              container="anan",
                              chunk_size=512,
                              path_format=self.path_format)
        self.assertEqual(data, "123")

    @patch('julep.wsgi.HPCStorage.logger')
    @patch('julep.thumbnail_handlers.get_swift_connection')
    def test_download_exception(self, mock_swift, mock_logger):
        store = SwiftStore(mock_swift, TIER_1, mock_logger)
        mock_swift.get_object.side_effect = ClientException("get failed!")
        with self.assertRaises(ClientException) as ar:
            store.download(self.user_id,
                                  self.uuid,
                                  container="anan",
                                  chunk_size=512,
                                  path_format=self.path_format)
        self.assertIsInstance(ar.exception, ClientException)

    @patch('julep.wsgi.HPCStorage.logger')
    @patch('julep.thumbnail_handlers.get_swift_connection')
    def test_delete(self, mock_swift, mock_logger):
        store = SwiftStore(mock_swift, TIER_1, mock_logger)
        mock_swift.delete_object.return_value = None
        store.delete(self.user_id,
                     self.uuid,
                     container="anan",
                     path_format=self.path_format)
        self.assertRaises(None)

    @patch('julep.wsgi.HPCStorage.logger')
    @patch('julep.thumbnail_handlers.get_swift_connection')
    def test_delete_client_exception(self, mock_swift, mock_logger):
        store = SwiftStore(mock_swift, TIER_1, mock_logger)
        mock_swift.delete_object.side_effect = ClientException("delete failed")
        with self.assertRaises(ClientException) as ar:
            store.delete(self.user_id,
                         self.uuid,
                         container="anan",
                         path_format=self.path_format)
        self.assertIsInstance(ar.exception, ClientException)

class TestPolarisStageStore(unittest.TestCase):

    def setUp(self):
        self.user_id = '123432abcdefeacdef'
        self.uuid = uuid4().hex
        self.data = Mock()
        self.path_format = "{}/appdata/thumbnail{}.jpg"\
            .format(self.user_id, self.uuid)

    @patch('julep.wsgi.HPCStorage.logger')
    @patch('julep.thumbnail_handlers.get_swift_connection')
    @patch('julep.wsgi.HPCStorage.redis_conn')
    def test_upload_to_redis(self, mock_redis, mock_swift, mock_logger):
        store = PolarisStageStore(mock_redis,
                                  mock_swift,
                                  Mock(spec=HashQueue),
                                  has_proxy=True,
                                  logge=mock_logger
                                  )
        store.tier_0.queue.enqueue.return_value = 1
        container = store.upload(TIER_0,
                                 self.user_id,
                                 self.uuid,
                                 self.data
                                 )
        self.assertIsNone(container)

    @patch('julep.wsgi.HPCStorage.logger')
    @patch('julep.thumbnail_handlers.get_swift_connection')
    @patch('julep.wsgi.HPCStorage.redis_conn')
    def test_upload_to_swift(self, mock_redis, mock_swift, mock_logger):
        store = PolarisStageStore(mock_redis,
                                  mock_swift,
                                  HashQueue,
                                  has_proxy=True,
                                  logger=mock_logger
                                  )
        container = store.upload(TIER_1,
                                 self.user_id,
                                 self.uuid,
                                 self.data,
                                 ctype="image/jpeg",
                                 container="anan",
                                 chunk_size=512,
                                 path_format=self.path_format
                                 )
        self.assertIsNotNone(container)
        self.assertEqual(container, 'anan')

    @patch('julep.wsgi.HPCStorage.logger')
    @patch('julep.thumbnail_handlers.get_swift_connection')
    @patch('julep.wsgi.HPCStorage.redis_conn')
    def test_upload_failover(self, mock_redis, mock_swift, mock_logger):
        store = PolarisStageStore(mock_redis,
                                  mock_swift,
                                  Mock(sepc=HashQueue),
                                  has_proxy=True,
                                  logger=mock_logger
                                  )
        exceptions = (ConnectionError, ERedisQueueFull)
        for ex in exceptions:
            store.tier_0.queue.enqueue.side_effect = ex
            # w/o specified container
            container_no = store.upload(TIER_0,
                                 self.user_id,
                                 self.uuid,
                                 self.data,
                                 ctype="image/jpeg",
                                 chunk_size=512,
                                 path_format=self.path_format
                                 )
            # w/ specified container
            container_yes = store.upload(TIER_0,
                                     self.user_id,
                                     self.uuid,
                                     self.data,
                                     container="anan",
                                     ctype="image/jpeg",
                                     chunk_size=512,
                                     path_format=self.path_format
                                     )
            self.assertIsNotNone(container_no)
            self.assertIsNotNone(container_yes)
            self.assertEqual(container_yes, "anan")

    @redis_driver_version(major=2, mid=10, minor=3)
    @patch('julep.wsgi.HPCStorage.logger')
    @patch('julep.thumbnail_handlers.get_swift_connection')
    @patch('julep.wsgi.HPCStorage.redis_conn')
    def test_upload_failover_new_redis_driver(self, mock_redis,
                                              mock_swift, mock_logger):
        store = PolarisStageStore(mock_redis,
                                  mock_swift,
                                  Mock(sepc=HashQueue),
                                  has_proxy=True,
                                  logger=mock_logger
                                  )
        exceptions = (ConnectionError, TimeoutError)
        for ex in exceptions:
            store.tier_0.queue.enqueue.side_effect = ex
            # w/o specified container
            container_no = store.upload(TIER_0,
                                 self.user_id,
                                 self.uuid,
                                 self.data,
                                 ctype="image/jpeg",
                                 chunk_size=512,
                                 path_format=self.path_format
                                 )
            # w/ specified container
            container_yes = store.upload(TIER_0,
                                     self.user_id,
                                     self.uuid,
                                     self.data,
                                     container="anan",
                                     ctype="image/jpeg",
                                     chunk_size=512,
                                     path_format=self.path_format
                                     )
            self.assertIsNotNone(container_no)
            self.assertIsNotNone(container_yes)
            self.assertEqual(container_yes, "anan")

    @patch('julep.wsgi.HPCStorage.logger')
    @patch('julep.thumbnail_handlers.get_swift_connection')
    @patch('julep.wsgi.HPCStorage.redis_conn')
    def test_upload_to_unkown_tier(self, mock_redis, mock_swift, mock_logger):
        store = PolarisStageStore(mock_redis,
                                  mock_swift,
                                  HashQueue,
                                  has_proxy=True,
                                  logger=mock_logger
                                  )
        fake_tiers = (-1, 3, "tier", 2.0, None)
        for tier in fake_tiers:
            with self.assertRaises(ValueError) as ar:
                container_no = store.upload(tier,
                                            self.user_id,
                                            self.uuid,
                                            self.data,
                                            ctype="image/jpeg",
                                            chunk_size=512,
                                            path_format=self.path_format
                                            )
            self.assertIsInstance(ar.exception, ValueError)
            self.assertEqual("tier '{}' is not supported!".format(tier),
                             str(ar.exception))

    @patch('julep.wsgi.HPCStorage.logger')
    @patch('julep.thumbnail_handlers.get_swift_connection')
    @patch('julep.wsgi.HPCStorage.redis_conn')
    def test_upload_swift_client_exception(self, mock_redis,
                                           mock_swift, mock_logger):
        store = PolarisStageStore(mock_redis,
                                  mock_swift,
                                  HashQueue,
                                  has_proxy=True,
                                  logger=mock_logger
                                  )
        mock_swift.put_object.side_effect = ClientException("fail to upload")
        with self.assertRaises(ClientException) as ar:
            container_no = store.upload(TIER_1,
                                        self.user_id,
                                        self.uuid,
                                        self.data,
                                        ctype="image/jpeg",
                                        chunk_size=512,
                                        path_format=self.path_format
                                        )
            container_yes = store.upload(TIER_1,
                                         self.user_id,
                                         self.uuid,
                                         self.data,
                                         container="anan",
                                         ctype="image/jpeg",
                                         chunk_size=512,
                                         path_format=self.path_format
                                         )
            self.assertIsNotNone(container_no)
            self.assertIsNotNone(container_yes)
        self.assertIsInstance(ar.exception, ClientException)
        self.assertEqual("fail to upload", str(ar.exception))

    @patch('julep.wsgi.HPCStorage.logger')
    @patch('julep.thumbnail_handlers.get_swift_connection')
    @patch('julep.wsgi.HPCStorage.redis_conn')
    def test_download_from_redis(self, mock_redis, mock_swift, mock_logger):
        store = PolarisStageStore(mock_redis,
                                  mock_swift,
                                  Mock(spec=HashQueue),
                                  has_proxy=True,
                                  logge=mock_logger
                                  )
        store.tier_0.queue.dequeue.return_value = 1, self.data
        k, data = store.download(TIER_0,
                                 self.user_id,
                                 self.uuid,
                                 )
        self.assertEqual(k, 1)
        self.assertEqual(data, self.data)

    @patch('julep.wsgi.HPCStorage.logger')
    @patch('julep.thumbnail_handlers.get_swift_connection')
    @patch('julep.wsgi.HPCStorage.redis_conn')
    def test_download_from_swift(self, mock_redis, mock_swift, mock_logger):
        store = PolarisStageStore(mock_redis,
                                  mock_swift,
                                  HashQueue,
                                  has_proxy=True,
                                  logger=mock_logger
                                  )
        mock_swift.get_object.return_value = Mock(), self.data
        data = store.download(TIER_1,
                                 self.user_id,
                                 self.uuid,
                                 ctype="image/jpeg",
                                 container="anan",
                                 chunk_size=512,
                                 path_format=self.path_format
                                 )
        self.assertEqual(data, self.data)

    @patch('julep.wsgi.HPCStorage.logger')
    @patch('julep.thumbnail_handlers.get_swift_connection')
    @patch('julep.wsgi.HPCStorage.redis_conn')
    def test_download_failover(self, mock_redis, mock_swift, mock_logger):
        store = PolarisStageStore(mock_redis,
                                  mock_swift,
                                  Mock(sepc=HashQueue),
                                  has_proxy=True,
                                  logger=mock_logger
                                  )
        exceptions = (ConnectionError, ERedisDataMissing, ERedisKeyNotFound)
        mock_swift.get_object.return_value = 1, self.data
        for ex in exceptions:
            store.tier_0.queue.dequeue.side_effect = ex
            # w/o specified container
            data_no = store.download(TIER_0,
                                 self.user_id,
                                 self.uuid,
                                 chunk_size=512,
                                 path_format=self.path_format
                                 )
            # w/ specified container
            data_yes = store.download(TIER_0,
                                     self.user_id,
                                     self.uuid,
                                     container="anan",
                                     chunk_size=512,
                                     path_format=self.path_format
                                     )
            self.assertEqual(data_no, self.data)
            self.assertEqual(data_yes, self.data)

    @redis_driver_version(major=2, mid=10, minor=3)
    @patch('julep.wsgi.HPCStorage.logger')
    @patch('julep.thumbnail_handlers.get_swift_connection')
    @patch('julep.wsgi.HPCStorage.redis_conn')
    def test_download_failover_new_redis_driver(self, mock_redis,
                                                mock_swift, mock_logger):
        store = PolarisStageStore(mock_redis,
                                  mock_swift,
                                  Mock(sepc=HashQueue),
                                  has_proxy=True,
                                  logger=mock_logger
                                  )
        exceptions = (ConnectionError, TimeoutError)
        mock_swift.get_object.return_value = 1, self.data
        for ex in exceptions:
            store.tier_0.queue.dequeue.side_effect = ex
            # w/o specified container
            data_no = store.download(TIER_0,
                                 self.user_id,
                                 self.uuid,
                                 chunk_size=512,
                                 path_format=self.path_format
                                 )
            # w/ specified container
            data_yes = store.download(TIER_0,
                                     self.user_id,
                                     self.uuid,
                                     container="anan",
                                     chunk_size=512,
                                     path_format=self.path_format
                                     )
            self.assertEqual(data_no, self.data)
            self.assertEqual(data_yes, self.data)

    @patch('julep.wsgi.HPCStorage.logger')
    @patch('julep.thumbnail_handlers.get_swift_connection')
    @patch('julep.wsgi.HPCStorage.redis_conn')
    def test_download_from_unkown_tier(self, mock_redis,
                                       mock_swift, mock_logger):
        store = PolarisStageStore(mock_redis,
                                  mock_swift,
                                  HashQueue,
                                  has_proxy=True,
                                  logger=mock_logger
                                  )
        fake_tiers = (-1, 3, "tier", 2.0, None)
        for tier in fake_tiers:
            with self.assertRaises(ValueError) as ar:
                data = store.download(tier,
                                      self.user_id,
                                      self.uuid,
                                      chunk_size=512,
                                      path_format=self.path_format
                                      )
            self.assertIsInstance(ar.exception, ValueError)
            self.assertEqual("tier '{}' is not supported!".format(tier),
                             str(ar.exception))

    @patch('julep.wsgi.HPCStorage.logger')
    @patch('julep.thumbnail_handlers.get_swift_connection')
    @patch('julep.wsgi.HPCStorage.redis_conn')
    def test_download_swift_client_exception(self, mock_redis,
                                           mock_swift, mock_logger):
        store = PolarisStageStore(mock_redis,
                                  mock_swift,
                                  HashQueue,
                                  has_proxy=True,
                                  logger=mock_logger
                                  )
        mock_swift.get_object.side_effect = ClientException("fail to download")
        with self.assertRaises(ClientException) as ar:
            data_no = store.download(TIER_1,
                                 self.user_id,
                                 self.uuid,
                                 chunk_size=512,
                                 path_format=self.path_format
                                 )

            data_yes = store.download(TIER_1,
                                 self.user_id,
                                 self.uuid,
                                 container="anan",
                                 chunk_size=512,
                                 path_format=self.path_format
                                 )
        self.assertIsInstance(ar.exception, ClientException)
        self.assertEqual("fail to download", str(ar.exception))

    @patch('julep.wsgi.HPCStorage.logger')
    @patch('julep.thumbnail_handlers.get_swift_connection')
    @patch('julep.wsgi.HPCStorage.redis_conn')
    def test_delete_from_redis(self, mock_redis, mock_swift, mock_logger):
        store = PolarisStageStore(mock_redis,
                                  mock_swift,
                                  Mock(spec=HashQueue),
                                  has_proxy=True,
                                  logge=mock_logger
                                  )
        store.tier_0.queue.release.return_value = 1, 10
        len, total_len = store.delete(TIER_0,
                                 self.user_id,
                                 self.uuid,
                                 )
        self.assertEqual(len, 1)
        self.assertEqual(total_len, 10)

    @patch('julep.wsgi.HPCStorage.logger')
    @patch('julep.thumbnail_handlers.get_swift_connection')
    @patch('julep.wsgi.HPCStorage.redis_conn')
    def test_delete_from_swift(self, mock_redis, mock_swift, mock_logger):
        store = PolarisStageStore(mock_redis,
                                  mock_swift,
                                  HashQueue,
                                  has_proxy=True,
                                  logger=mock_logger
                                  )
        mock_swift.delete_object.return_value = None
        rt = store.delete(TIER_1,
                          self.user_id,
                          self.uuid,
                          container="anan",
                          path_format=self.path_format
                          )
        self.assertIsNone(rt)

    @patch('julep.wsgi.HPCStorage.logger')
    @patch('julep.thumbnail_handlers.get_swift_connection')
    @patch('julep.wsgi.HPCStorage.redis_conn')
    def test_delete_failover(self, mock_redis, mock_swift, mock_logger):
        store = PolarisStageStore(mock_redis,
                                  mock_swift,
                                  Mock(sepc=HashQueue),
                                  has_proxy=True,
                                  logger=mock_logger
                                  )
        exceptions = (ConnectionError, ERedisReleaseError, ERedisKeyNotFound)
        mock_swift.delete_object.return_value = None
        for ex in exceptions:
            store.tier_0.queue.release.side_effect = ex
            # w/o specified container
            rt_no = store.delete(TIER_0,
                                 self.user_id,
                                 self.uuid,
                                 path_format=self.path_format
                                 )
            # w/ specified container
            rt_yes = store.delete(TIER_0,
                                    self.user_id,
                                    self.uuid,
                                    container="anan",
                                    path_format=self.path_format
                                    )
            self.assertIsNone(rt_no)
            self.assertIsNone(rt_yes)

    @redis_driver_version(major=2, mid=10, minor=3)
    @patch('julep.wsgi.HPCStorage.logger')
    @patch('julep.thumbnail_handlers.get_swift_connection')
    @patch('julep.wsgi.HPCStorage.redis_conn')
    def test_download_failover_new_redis_driver(self, mock_redis,
                                                mock_swift, mock_logger):
        store = PolarisStageStore(mock_redis,
                                  mock_swift,
                                  Mock(sepc=HashQueue),
                                  has_proxy=True,
                                  logger=mock_logger
                                  )
        exceptions = (ConnectionError, TimeoutError)
        mock_swift.delete_object.return_value = None
        for ex in exceptions:
            store.tier_0.queue.release.side_effect = ex
            # w/o specified container
            rt_no = store.delete(TIER_0,
                                 self.user_id,
                                 self.uuid,
                                 path_format=self.path_format
                                 )
            # w/ specified container
            rt_yes = store.delete(TIER_0,
                                  self.user_id,
                                  self.uuid,
                                  container="anan",
                                  path_format=self.path_format
                                  )
            self.assertIsNone(rt_no)
            self.assertIsNone(rt_yes)

    @patch('julep.wsgi.HPCStorage.logger')
    @patch('julep.thumbnail_handlers.get_swift_connection')
    @patch('julep.wsgi.HPCStorage.redis_conn')
    def test_delete_from_unkown_tier(self, mock_redis,
                                       mock_swift, mock_logger):
        store = PolarisStageStore(mock_redis,
                                  mock_swift,
                                  HashQueue,
                                  has_proxy=True,
                                  logger=mock_logger
                                  )
        fake_tiers = (-1, 3, "tier", 2.0, None)
        for tier in fake_tiers:
            with self.assertRaises(ValueError) as ar:
                store.delete(tier,
                             self.user_id,
                             self.uuid,
                             path_format=self.path_format
                             )
            self.assertIsInstance(ar.exception, ValueError)
            self.assertEqual("tier '{}' is not supported!".format(tier),
                             str(ar.exception))

    @patch('julep.wsgi.HPCStorage.logger')
    @patch('julep.thumbnail_handlers.get_swift_connection')
    @patch('julep.wsgi.HPCStorage.redis_conn')
    def test_delete_release_error(self, mock_redis,
                                       mock_swift, mock_logger):
        store = PolarisStageStore(mock_redis,
                                  mock_swift,
                                  Mock(spec=HashQueue),
                                  has_proxy=True,
                                  logger=mock_logger
                                  )

        store.tier_0.queue.release.side_effect = ERedisReleaseError(self.uuid)
        with self.assertRaises(ERedisReleaseError) as ar:
            store.delete(TIER_0,
                         self.user_id,
                         self.uuid,
                         path_format=self.path_format
                         )
        self.assertIsInstance(ar.exception, ERedisReleaseError)
        self.assertEqual("Redis Error: Key {} has not been dequeued!"
                         .format(self.uuid), str(ar.exception))

    @patch('julep.wsgi.HPCStorage.logger')
    @patch('julep.thumbnail_handlers.get_swift_connection')
    @patch('julep.wsgi.HPCStorage.redis_conn')
    def test_delete_swift_client_exception(self, mock_redis,
                                           mock_swift, mock_logger):
        store = PolarisStageStore(mock_redis,
                                  mock_swift,
                                  HashQueue,
                                  has_proxy=True,
                                  logger=mock_logger
                                  )
        mock_swift.delete_object.side_effect = ClientException("fail to delete")
        with self.assertRaises(ClientException) as ar:
            store.delete(TIER_1,
                         self.user_id,
                         self.uuid,
                         path_format=self.path_format
                         )

            store.delete(TIER_1,
                         self.user_id,
                         self.uuid,
                         container="anan",
                         path_format=self.path_format
                         )
        self.assertIsInstance(ar.exception, ClientException)
        self.assertEqual("fail to delete", str(ar.exception))
