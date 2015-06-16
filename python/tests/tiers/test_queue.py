import unittest
from mock import Mock, patch
from julep.tiers.queue import Job, JOB_ENQUEUED, JOB_DEQUEUED, JOB_RESERVED,\
    HashQueue, ReliableQueue, ERedisQueueFull, ERedisKeyNotFound,\
    ERedisDataMissing, ERedisReleaseError, ERedisEmptyQueue
import pickle
from uuid import uuid4
from hashlib import md5
from tests.tiers import redis_driver_version

try:
    from redis import ConnectionError, TimeoutError
except ImportError:
    pass

class TestJob(unittest.TestCase):

    def test_change_state(self):
        job = Job('change-state')
        states = [JOB_ENQUEUED, JOB_DEQUEUED, JOB_RESERVED]
        for s in states:
            job.change_state(s)
            self.assertEqual(s, job.state)
            self.assertNotEqual(0, job.state)
            if s == JOB_ENQUEUED:
                self.assertIsNotNone(job.enqueue_ts)
            if s == JOB_DEQUEUED:
                self.assertIsNotNone(job.dequeue_ts)

    def test_change_state_to_reserved(self):
        job = Job('test-known')
        job.change_state(JOB_RESERVED)
        self.assertIsNone(job.enqueue_ts)
        self.assertIsNone(job.dequeue_ts)
        self.assertEqual(JOB_RESERVED, job.state)

    def test_change_state_exception(self):
        job = Job('test-except')
        with self.assertRaises(ValueError) as ar:
            job.change_state(-1)
        self.assertIsInstance(ar.exception, ValueError)

        try:
            with self.assertRaises(ValueError) as ar:
                job.change_state(JOB_DEQUEUED)
        except AssertionError:
            pass

class TestHashQueue(unittest.TestCase):

    # Redis connection options

    def setUp(self):
        self.user_id = '123432abcdefeacdef'
        self.uuid = uuid4().hex
        self.mock_redis_conn = Mock()
        self.data = Mock()

    def tearDown(self):
        pass

    def test_queue_length(self):
        queue = HashQueue(self.mock_redis_conn)
        self.mock_redis_conn.get.return_value = None
        self.assertEqual(0, len(queue))
        self.mock_redis_conn.reset_mock()
        self.mock_redis_conn.get.return_value = '1024'
        self.assertEqual(1024, len(queue))

    def test_queue_length_connection_error(self):
        queue = ReliableQueue(self.mock_redis_conn)
        self.mock_redis_conn.get.side_effect = ConnectionError
        with self.assertRaises(ConnectionError) as ar:
            l = len(queue)
        self.assertIsInstance(ar.exception, ConnectionError)

    @redis_driver_version(major=2, mid=10, minor=3)
    def test_queue_length_timeout_error(self):
        queue = ReliableQueue(self.mock_redis_conn)
        self.mock_redis_conn.get.side_effect = TimeoutError
        with self.assertRaises(TimeoutError) as ar:
            l = len(queue)
        self.assertIsInstance(ar.exception, TimeoutError)

    def test_proxy(self):
        queue = HashQueue(self.mock_redis_conn, has_proxy=False)
        self.mock_redis_conn.get.return_value = 10
        queue.redis.pipeline.return_value.execute.side_effect = ConnectionError
        qname = md5(self.user_id + "_queue").hexdigest()
        try:
            queue.enqueue(qname, self.uuid, self.data)
        except Exception as e:
            self.assertIsInstance(e, ConnectionError)

    def test_queue_stats(self):
        self.mock_redis_conn.get.return_value = '20'
        self.mock_redis_conn.hget.return_value = '10'
        queue = HashQueue(self.mock_redis_conn)
        qname = md5(self.user_id + "_queue").hexdigest()
        stat = queue.stats(qname)
        self.assertEqual(1024, stat['size'])
        self.assertEqual(20, stat['length'])
        self.assertEqual('10', stat[qname])
        self.mock_redis_conn.hget.return_value = None
        stat = queue.stats(qname)
        self.assertEqual(None, stat[qname])

    def test_enqueue_queue_full(self):
        queue = HashQueue(self.mock_redis_conn)
        self.mock_redis_conn.get.return_value = '1024'
        self.assertEqual(len(queue), 1024)
        qname = md5(self.user_id + "_queue").hexdigest()
        with self.assertRaises(ERedisQueueFull) as ar:
            queue.enqueue(qname, self.uuid, self.data)
        self.assertIsInstance(ar.exception, ERedisQueueFull)

    def test_enqueue_pipeline_connection_error(self):
        self.mock_redis_conn.get.return_value = None
        queue = HashQueue(self.mock_redis_conn)
        self.mock_redis_conn.pipeline.side_effect = ConnectionError
        qname = md5(self.user_id + "_queue").hexdigest()
        with self.assertRaises(ConnectionError) as ar:
            queue.enqueue(qname, self.uuid, self.data)
        self.assertIsInstance(ar.exception, ConnectionError)

    @redis_driver_version(major=2, mid=10, minor=3)
    def test_enqueue_pipeline_timeout_error(self):
        self.mock_redis_conn.get.return_value = None
        queue = HashQueue(self.mock_redis_conn)
        self.mock_redis_conn.pipeline.side_effect = TimeoutError
        qname = md5(self.user_id + "_queue").hexdigest()
        with self.assertRaises(TimeoutError) as ar:
            queue.enqueue(qname, self.uuid, self.data)
        self.assertIsInstance(ar.exception, TimeoutError)

    def test_enqueue(self):
        queue = HashQueue(self.mock_redis_conn, has_proxy=True)
        qname = md5(self.user_id + "_queue").hexdigest()
        self.mock_redis_conn.get.return_value = 10
        self.mock_redis_conn.pipeline.return_value.execute.return_value =\
            1, 1, 11, 11
        r = queue.enqueue(qname, self.uuid, self.data)
        self.assertIsNone(r)

    def test_dequeue(self):
        queue = HashQueue(self.mock_redis_conn, has_proxy=True)
        qname = md5(self.user_id + "_queue").hexdigest()
        job = pickle.dumps(Job(self.uuid))
        self.mock_redis_conn.pipeline.return_value.execute.return_value =\
            self.data, job
        k, data = queue.dequeue(qname, self.uuid)
        self.assertEqual(k, self.uuid)
        self.assertEqual(data, self.data)

    def test_dequeue_not_found(self):
        queue = HashQueue(self.mock_redis_conn, has_proxy=True)
        qname = md5("None" + "_queue").hexdigest()
        self.mock_redis_conn.pipeline.return_value.execute.return_value =\
            None, None
        with self.assertRaises(ERedisKeyNotFound) as ar:
            k, data = queue.dequeue(qname, self.uuid)
        self.assertIsInstance(ar.exception, ERedisKeyNotFound)

    def test_dequeue_data_missing(self):
        queue = HashQueue(self.mock_redis_conn, has_proxy=True)
        qname = md5(self.user_id + "_queue").hexdigest()
        job = pickle.dumps(Job(self.uuid))
        self.mock_redis_conn.pipeline.return_value.execute.return_value =\
            None, job
        with self.assertRaises(ERedisDataMissing) as ar:
            k, data = queue.dequeue(qname, self.uuid)
        self.assertIsInstance(ar.exception, ERedisDataMissing)

    def test_dequeue_pipeline_connection_error(self):
        queue = HashQueue(self.mock_redis_conn, has_proxy=True)
        self.mock_redis_conn.pipeline.return_value.execute.side_effect =\
            ConnectionError
        qname = md5(self.user_id + "_queue").hexdigest()
        with self.assertRaises(ConnectionError) as ar:
            queue.dequeue(qname, self.uuid)
        self.assertIsInstance(ar.exception, ConnectionError)

    @redis_driver_version(major=2, mid=10, minor=3)
    def test_dequeue_pipeline_timeout_error(self):
        queue = HashQueue(self.mock_redis_conn)
        self.mock_redis_conn.pipeline.return_value.execute.side_effect =\
            TimeoutError
        qname = md5(self.user_id + "_queue").hexdigest()
        with self.assertRaises(TimeoutError) as ar:
            queue.dequeue(qname, self.uuid)
        self.assertIsInstance(ar.exception, TimeoutError)

    def test_release(self):
        queue = HashQueue(self.mock_redis_conn, has_proxy=True)
        qname = md5(self.user_id + "_queue").hexdigest()
        job = pickle.dumps(Job(self.uuid))
        job = pickle.loads(job)
        job.change_state(JOB_DEQUEUED)
        job = pickle.dumps(job)
        self.mock_redis_conn.get.return_value = 10
        self.mock_redis_conn.hget.return_value = job
        self.mock_redis_conn.pipeline.return_value.execute.return_value =\
            1, 1, 9, 9
        l1, l2 = queue.release(qname, self.uuid)
        self.assertEqual(9, l1)
        self.assertEqual(9, l2)

    def test_release_empty_queue(self):
        queue = HashQueue(self.mock_redis_conn, has_proxy=True)
        qname = md5(self.user_id + "_queue").hexdigest()
        self.mock_redis_conn.get.return_value = 0
        l1, l2 = queue.release(qname, self.uuid)
        self.assertEqual(0, l1)
        self.assertEqual(0, l2)

    def test_release_undequeued_job(self):
        queue = HashQueue(self.mock_redis_conn, has_proxy=True)
        qname = md5(self.user_id + "_queue").hexdigest()
        job = pickle.dumps(Job(self.uuid))
        self.mock_redis_conn.get.return_value = 10
        self.mock_redis_conn.hget.return_value = job
        with self.assertRaises(ERedisReleaseError) as ar:
            queue.release(qname, self.uuid)
        self.assertIsInstance(ar.exception, ERedisReleaseError)

    def test_release_pipeline_connection_error(self):
        self.mock_redis_conn.get.return_value = 10
        self.mock_redis_conn.hget.side_effect = ConnectionError
        queue = HashQueue(self.mock_redis_conn, has_proxy=True)
        qname = md5(self.user_id + "_queue").hexdigest()
        with self.assertRaises(ConnectionError) as ar:
            queue.release(qname, self.uuid)
        self.assertIsInstance(ar.exception, ConnectionError)

    @redis_driver_version(major=2, mid=10, minor=3)
    def test_release_pipeline_timeout_error(self):
        self.mock_redis_conn.get.return_value = 10
        self.mock_redis_conn.hget.side_effect = TimeoutError
        queue = HashQueue(self.mock_redis_conn)
        qname = md5(self.user_id + "_queue").hexdigest()
        with self.assertRaises(TimeoutError) as ar:
            queue.release(qname, self.uuid)
        self.assertIsInstance(ar.exception, TimeoutError)

        self.mock_redis_conn.reset_mock()
        self.mock_redis_conn.get.side_effect = TimeoutError
        with self.assertRaises(TimeoutError) as ar:
            queue.release(qname, self.uuid)
        self.assertIsInstance(ar.exception, TimeoutError)

        self.mock_redis_conn.reset_mock()
        self.mock_redis_conn.get.return_value = 10
        self.mock_redis_conn.hget.return_value = pickle.dumps(Job(self.uuid))
        self.mock_redis_conn.pipeline.return_value.execute.side_effect =\
            TimeoutError
        with self.assertRaises(TimeoutError) as ar:
            queue.release(qname, self.uuid)
        self.assertIsInstance(ar.exception, TimeoutError)

class TestReliableQueue(unittest.TestCase):
    def setUp(self):
        self.user_id = '123432abcdefeacdef'
        self.uuid = uuid4().hex
        self.data = Mock()

    @patch('julep.wsgi.HPCStorage.redis_conn')
    def test_queue_length(self, mock_redis):
        queue = ReliableQueue(mock_redis)
        mock_redis.get.return_value = None
        self.assertEqual(0, len(queue))
        mock_redis.get.return_value = 1024
        self.assertEqual(1024, len(queue))

    @patch('julep.wsgi.HPCStorage.redis_conn')
    def test_queue_length_connection_error(self, mock_redis):
        queue = ReliableQueue(mock_redis)
        mock_redis.get.side_effect = ConnectionError
        l = -1
        with self.assertRaises(ConnectionError) as ar:
            l = len(queue)
        self.assertEqual(l, -1)
        self.assertIsInstance(ar.exception, ConnectionError)

    @redis_driver_version(major=2, mid=10, minor=3)
    @patch('julep.wsgi.HPCStorage.redis_conn')
    def test_queue_length_timeout_error(self, mock_redis):
        queue = ReliableQueue(mock_redis)
        mock_redis.get.side_effect = TimeoutError
        l = -1
        with self.assertRaises(TimeoutError) as ar:
            l = len(queue)
        self.assertEqual(l, -1)
        self.assertIsInstance(ar.exception, TimeoutError)

    @patch('julep.wsgi.HPCStorage.redis_conn')
    def test_enqueue(self, mock_redis):
        queue = ReliableQueue(mock_redis)
        qname = md5(self.user_id + "_queue").hexdigest()
        mock_redis.pipeline.return_value.execute.return_value =\
            10, 1, 1, 10, 11
        l = queue.enqueue(qname, self.uuid, self.data)
        self.assertEqual(l, 10)

    @patch('julep.wsgi.HPCStorage.redis_conn')
    def test_enqueue_connection_error(self, mock_redis):
        queue = ReliableQueue(mock_redis)
        qname = md5(self.user_id + "_queue").hexdigest()
        mock_redis.pipeline.return_value.execute.side_effect = ConnectionError
        l = -1
        with self.assertRaises(ConnectionError) as ar:
            l = queue.enqueue(qname, self.uuid, self.data)
        self.assertEqual(l, -1)
        self.assertIsInstance(ar.exception, ConnectionError)

    @redis_driver_version(major=2, mid=10, minor=3)
    @patch('julep.wsgi.HPCStorage.redis_conn')
    def test_enqueue_timeout_error(self, mock_redis):
        queue = ReliableQueue(mock_redis)
        qname = md5(self.user_id + "_queue").hexdigest()
        mock_redis.pipeline.return_value.execute.side_effect = TimeoutError
        l = -1
        with self.assertRaises(TimeoutError) as ar:
            l = queue.enqueue(qname, self.uuid, self.data)
        self.assertEqual(l, -1)
        self.assertIsInstance(ar.exception, TimeoutError)

    @patch('julep.wsgi.HPCStorage.redis_conn')
    def test_enqueue_queue_full(self, mock_redis):
        queue = ReliableQueue(mock_redis)
        qname = md5(self.user_id + "_queue").hexdigest()
        mock_redis.get.return_value = 1024
        l = -1
        with self.assertRaises(ERedisQueueFull) as ar:
            l = queue.enqueue(qname, self.uuid, self.data)
        self.assertEqual(l, -1)
        self.assertIsInstance(ar.exception, ERedisQueueFull)

    @patch('julep.wsgi.HPCStorage.redis_conn')
    def test_dequeue(self, mock_redis):
        queue = ReliableQueue(mock_redis)
        qname = md5(self.user_id + "_queue").hexdigest()
        mock_redis.rpoplpush.return_value = self.uuid
        mock_redis.pipeline.return_value.execute.return_value = \
            self.data, pickle.dumps(Job(self.uuid))
        k, data = queue.dequeue(qname, self.uuid)
        self.assertEqual(k, self.uuid)
        self.assertEqual(data, self.data)

    @patch('julep.wsgi.HPCStorage.redis_conn')
    def test_dequeue_empty_queue(self, mock_redis):
        queue = ReliableQueue(mock_redis)
        qname = md5(self.user_id + "_queue").hexdigest()
        mock_redis.rpoplpush.return_value = None
        k = data = None
        with self.assertRaises(ERedisEmptyQueue) as ar:
            k, data = queue.dequeue(qname, self.uuid)
        self.assertIsNone(k)
        self.assertIsNone(data)
        self.assertIsInstance(ar.exception, ERedisEmptyQueue)
        self.assertEqual("Redis Warn: {} queue is empty!".format(qname),
                         str(ar.exception))

    @patch('julep.wsgi.HPCStorage.redis_conn')
    def test_dequeue_data_missing(self, mock_redis):
        queue = ReliableQueue(mock_redis)
        qname = md5(self.user_id + "_queue").hexdigest()
        mock_redis.rpoplpush.return_value = "anan"
        k = data = None
        mock_redis.pipeline.return_value.execute.return_value = \
            None, pickle.dumps(Job(self.uuid))
        with self.assertRaises(ERedisDataMissing) as ar:
            k, data = queue.dequeue(qname, "anan")
        self.assertIsNone(k)
        self.assertIsNone(data)
        self.assertIsInstance(ar.exception, ERedisDataMissing)

    @patch('julep.wsgi.HPCStorage.redis_conn')
    def test_dequeue_key_not_found(self, mock_redis):
        queue = ReliableQueue(mock_redis)
        qname = md5(self.user_id + "_queue").hexdigest()
        mock_redis.rpoplpush.return_value = "anan"
        k = data = None
        mock_redis.pipeline.return_value.execute.return_value = \
            None, None
        with self.assertRaises(ERedisKeyNotFound) as ar:
           k, data = queue.dequeue(qname, self.uuid)
        self.assertIsNone(k)
        self.assertIsNone(data)
        self.assertIsInstance(ar.exception, ERedisKeyNotFound)

    @patch('julep.wsgi.HPCStorage.redis_conn')
    def test_dequeue_connection_error(self, mock_redis):
        queue = ReliableQueue(mock_redis)
        qname = md5(self.user_id + "_queue").hexdigest()
        mock_redis.rpoplpush.side_effect = ConnectionError
        data = None
        with self.assertRaises(ConnectionError) as ar:
            data = queue.dequeue(qname, self.uuid)
        self.assertIsNone(data)
        self.assertIsInstance(ar.exception, ConnectionError)
        mock_redis.reset_mock()
        mock_redis.rpoplpush.return_value = "anan"
        mock_redis.pipeline.return_value.execute.side_effect= ConnectionError
        with self.assertRaises(ConnectionError) as ar:
            data = queue.dequeue(qname, self.uuid)
        self.assertIsNone(data)
        self.assertIsInstance(ar.exception, ConnectionError)

    @redis_driver_version(major=2, mid=10, minor=3)
    @patch('julep.wsgi.HPCStorage.redis_conn')
    def test_dequeue_timeout_error(self, mock_redis):
        queue = ReliableQueue(mock_redis)
        qname = md5(self.user_id + "_queue").hexdigest()
        mock_redis.rpoplpush.side_effect = TimeoutError
        k = data = None
        with self.assertRaises(TimeoutError) as ar:
            k, data = queue.dequeue(qname, self.uuid)
        self.assertIsNone(k)
        self.assertIsNone(data)
        self.assertIsInstance(ar.exception, TimeoutError)

        mock_redis.reset_mock()
        mock_redis.rpoplpush.return_value = "anan"
        mock_redis.pipeline.return_value.execute.side_effect= TimeoutError
        with self.assertRaises(TimeoutError) as ar:
            k, data = queue.dequeue(qname, self.uuid)
        self.assertIsNone(k)
        self.assertIsNone(data)
        self.assertIsInstance(ar.exception, TimeoutError)

    @patch('julep.wsgi.HPCStorage.redis_conn')
    def test_release(self, mock_redis):
        queue = ReliableQueue(mock_redis)
        qname = md5(self.user_id + "_queue").hexdigest()
        job = Job(self.uuid)
        job.change_state(JOB_DEQUEUED)
        mock_redis.hget.return_value = pickle.dumps(job)
        mock_redis.pipeline.return_value.execute.return_value = \
            10, 1, 1, 1, 10
        q_len = total_len = -1
        q_len, total_len  = queue.release(qname, self.uuid)
        self.assertEqual(1, q_len)
        self.assertEqual(10, total_len)

    @patch('julep.wsgi.HPCStorage.redis_conn')
    def test_release_undequeued_job(self, mock_redis):
        queue = ReliableQueue(mock_redis)
        qname = md5(self.user_id + "_queue").hexdigest()
        job = Job(self.uuid)
        mock_redis.hget.return_value = pickle.dumps(job)
        mock_redis.pipeline.return_value.execute.return_value = \
            10, 1, 1, 1, 10
        q_len = total_len = -1
        with self.assertRaises(ERedisReleaseError) as ar:
            q_len, total_len = queue.release(qname, self.uuid)
        self.assertEqual(-1, q_len)
        self.assertEqual(-1, total_len)
        self.assertIsInstance(ar.exception, ERedisReleaseError)

    @patch('julep.wsgi.HPCStorage.redis_conn')
    def test_release_connection_error(self, mock_redis):
        queue = ReliableQueue(mock_redis)
        qname = md5(self.user_id + "_queue").hexdigest()
        mock_redis.hget.side_effect = ConnectionError
        q_len = total_len = -1
        with self.assertRaises(ConnectionError) as ar:
            q_len, total_len= queue.release(qname, self.uuid)
        self.assertEqual(-1, q_len)
        self.assertEqual(-1, total_len)
        self.assertIsInstance(ar.exception, ConnectionError)

        mock_redis.reset_mock()
        mock_redis.hget.return_value= "anan"
        mock_redis.pipeline.return_value.execute.side_effect= ConnectionError
        with self.assertRaises(ConnectionError) as ar:
            q_len, total_len = queue.release(qname, self.uuid)
        self.assertEqual(-1, q_len)
        self.assertEqual(-1, total_len)
        self.assertIsInstance(ar.exception, ConnectionError)

    @redis_driver_version(major=2, mid=10, minor=3)
    @patch('julep.wsgi.HPCStorage.redis_conn')
    def test_release_timeout_error(self, mock_redis):
        queue = ReliableQueue(mock_redis)
        qname = md5(self.user_id + "_queue").hexdigest()
        mock_redis.hget.side_effect = TimeoutError
        q_len = total_len = -1
        with self.assertRaises(TimeoutError) as ar:
            q_len, total_len= queue.release(qname, self.uuid)
        self.assertEqual(-1, q_len)
        self.assertEqual(-1, total_len)
        self.assertIsInstance(ar.exception, TimeoutError)

        mock_redis.reset_mock()
        mock_redis.hget.return_value= "anan"
        mock_redis.pipeline.return_value.execute.side_effect= TimeoutError
        with self.assertRaises(TimeoutError) as ar:
            q_len, total_len = queue.release(qname, self.uuid)
        self.assertEqual(-1, q_len)
        self.assertEqual(-1, total_len)
        self.assertIsInstance(ar.exception, TimeoutError)
