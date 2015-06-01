"""
julep.tiers.queue
~~~~~~~~~~~~~~~~~

This module contains the interfaces responsible for Reliable Redis Queues
implementations. To make Redis work as a store more reliable, a reliable queue
is used to protect data.

"""

import time
import pickle
from .exceptions import ERedisDataMissing, ERedisKeyNotFound, \
    EReliableQueueFull

JOB_ENQUEUED = 1
JOB_DEQUEUED = 2
JOB_UNKNOWN_STATE = 3

class QueueBase(object):
    """
    QueueBase represents the abstract queue interface which will be used in
    Redis storage. One queue could be implemented via comprising lists, hash
    set or sorted set.

    param conn: connection to redis
    param size: the maximum elements one queue could hold meanwhile
    param values: the hash set name to hold the real data elements
    param has_proxy: identify if a proxy server will resident infront of redis
    param logger: the logger for logging

    """
    redis = None

    class JobStat(object):
        """
        Stat will be used to monitoring the job stats.
        """

        def __int__(self, key):
            self._key = key
            self._state = 0
            self._enqueue_time = None
            self._dequeue_time = None

        @property
        def key(self):
            return self._key

        @property
        def state(self):
            return self._state

        @state.setter
        def state(self, new_state):
            self._state = new_state

        def change_state(self,
                         new_state,
                         enqueue_ts=None,
                         dequeue_ts=None,
                         ):
            self.state = new_state
            if enqueue_ts:
                self.enqueue_time = enqueue_ts
            if dequeue_ts:
                self.dequeue_time = dequeue_ts

        @property
        def enqueue_time(self):
            return self._enqueue_time

        @enqueue_time.setter
        def enqueue_time(self, ts):
            self._enqueue_time = ts

        @property
        def dequeue_time(self):
            return self._dequeue_time

        @_dequeue_time.setter
        def last_dequeue_time(self, ts):
            self._dequeue_time = ts

    def __init__(self, conn, size, values, stats, has_proxy,
                 logger=None, retries=3):
        self.redis = conn
        self._size = size
        self.values = values
        self.stats = stats
        self.has_proxy = has_proxy
        self.logger = logger
        self.retries = retries

    def __str__(self): return self.__class__.__name__

    def __len__(self):
        try:
            return self.redis.hlen(self.values)
        except:
            raise

    @property
    def queue_is_full(self):
        return self.size == len(self)

    @property
    def size(self): return self._size

    @size.setter
    def size(self, new_size):
        self._size = new_size

    def enqueue(self, k, data):
        raise NotImplemented

    def dequeue(self, k):
        raise NotImplemented

    def requeue(self, k):
        raise NotImplemented

    def release(self, k):
        raise NotImplemented

class HashQueue(QueueBase):
    """
    HashQueue will store data into a hash set. Uploading and downloading will
    be implemented via enqueue and dequeue operations. To protect data safety,
    the data should not be deleted until the data process completes.
    """
    def enqueue(self, k, data):
        try:
            job_stat = self.JobStat(k)
            job_stat.change_state(JOB_ENQUEUED, enqueue_ts=time.time())
            job_stat = pickle.dumps(job_stat)

            pipe = self.redis.pipeline(transaction=not self.hash_proxy)
            pipe.hset(self.values, k, data).hset(self.stats, k, job_stat)
            ok, _ = pipe.execute()
            if not ok:
                # The k has already been set in hash set
                if self.logger:
                    self.logger.debug("{} was rewritten by new data".format(k))
        except:
            raise

    def dequeue(self, k):
        try:
            data = self.redis.hget(self.values, k)
            if not data:
                # 1. k was not existing
                # 2. data missing
                job_stat = self.redis.hget(self.stats, k)
                if not job_stat:
                    # Oops! k and its data were dropped into blackhole.
                    # either been processed or missing.
                    raise ERedisKeyNotFound(k)
                else:
                    if pickle.loads(job_stat).state == JOB_DEQUEUED:
                        # job has not been dequeued but never released
                        # seems data missing happened.
                        raise ERedisDataMissing(k)
                    else:
                        # job has been processed, cleaning the orphan stats
                        self.redis.hdel(self.stats, k)
                        raise ERedisKeyNotFound(k)
            job_stat = self.redis.hget(self.stats, k)
            if not job_stat:
                # stats missing, rebuilding one new stat
                job_stat = self.JobStat(k)
                job_stat.change_state(JOB_DEQUEUED, dequeue_ts=time.time())
            else:
                job_stat = pickle.loads(job_stat)
                job_stat.change_state(JOB_DEQUEUED, dequeue_ts=time.time())
            job_stat = pickle.dumps(job_stat)
            self.redis.hset(self.stats, k, job_stat)
            return data
        except:
            raise

    def release(self, k):
        try:
            pipe = self.redis.pipeline(transaction=not self.has_prox)
            pipe.hdel(self.values, k).hdel(self.stats, k)
            ok, _ = pipe.execute()
            if not ok and self.logger:
                self.logger.debug("release a nonexisting key:{}".format(k))
        except:
            raise

class ReliableQueue(QueueBase):
    """
    ReliableQueue represents the queues resident in Redis.A ReliableQueue is
    consist of two separate Redis lists:
        pending_queue [userid:uuid, ...]
        working_queue [userid:uuid, ...]
    The data access in Redis will look like following format:
        LPUSH pending_queue userid:uuid
        RPOPLPUSH pending_queue, working_queue, userid:uuid

    And a hash set stores real binary data:
        values: {userid:uuid => data, ...}

    param pending_queue: the pending queue name
    param working_queue: the working queue name

    """
    def __init__(self, conn, size, values, stats, has_proxy=False,
                 logger=None, retries=3, **kwargs):
        super(ReliableQueue, self).__init__(conn,
                                            size,
                                            values,
                                            stats,
                                            has_proxy,
                                            logger,
                                            retries
                                            )
        self.pending_queue = kwargs.get("pending_queue", "pending")
        self.working_queue = kwargs.get("working_queue", "working")

    def __len__(self):
        """
        Set the queue length to the maximum value of the three data structures:
            pending_queue
            working_queue
            values
        to make sure elements in the ReliableQueue SHOULD not over than user
        settings.
        """
        try:
            pipe = self.redis.pipeline(transaction=not self.has_proxy)
            pipe.llen(self.pending_queue)\
                .llen(self.working_queue)\
                .hlen(self.values)
            return max(pipe.execute())
        except:
            raise

    def enqueue(self, k, data):
        """
        enqueue will push data into pending queue.
        This mehtod comprises two subsequent commands:lpush and hset. In order
        to improve performance as much as possible, the requests would be sent
        via pipelining. While initializing pipe object please make sure set
        transaction=False, since Twemproxy does not support Redis transactions
        now.

        """
        try:
            if self.queue_is_full:
                raise EReliableQueueFull(self)
            job_stat = self.JobStat(k)
            job_stat.state = 'enqueued'
            job_stat.enqueue_time = time.time()

            pipe = self.redis.pipeline(transaction=not self.has_proxy)
            pipe.lpush(self.pending_queue, k)\
                .hset(self.values, k, data)\
                .hset(self.stats, k, pickle.dumps(job_stat))
            len, ok, _ = pipe.execute()
            if not ok: # {k:data} is already in hash set
                self.logger.warn("Key:{} is already existing in hash set!"\
                                 .format(k))
            return len
        except:
            raise

    def dequeue(self, k):
        """
        dequeue uses Redis RPOPLPUSH command to pop an element and clone it
        into working queue atomically. After get the element key, invoking
        HGET command to retrieve real data from hash set. Compared to enqueue
        operation pipelining is unfeasible here, since Reliable queue uses
        Redis lists to store elements, and element is retrieved via
        RPOPLPUSH operation. This mechanism makes the key we retrieve from
        queue is different from the passed parameter: k.
        """

        data = None
        try:
            temp_k = self.redis.rpoplpush(self.pending_queue, self.working_queue)
            if temp_k:
                data = self.redis.hget(self.values, temp_k)
                if not data:
                    raise ERedisDataMissing(k)
            else:
                # To protect error cases that in last dequeue operation an
                # error was triggered while the element was removed from
                # pending queue. We should make addtional check to fix the
                # data consistence between pending queue and workign queue.
                pass
            return data
        except:
            raise

    def release(self, k):
        """
        After data is processed, the original data coulde be cleaned up. Since
        Reliable queue comprises three components:
            pending queue
            working queue
            data set
        The original data has already been released from pending queue, but
        the key still reside in working queue and the real data also hide in
        data set.
        """
        try:
            pipe = self.redis.pipeline(transaction=not self.has_proxy)
            pipe.lrem(self.working_queue, 0, k).hdel(self.values, k)
            pipe.execute()
        except:
            raise

    def requeue(self, k, data):
        try:
            if not self.redis.exists(k) and not self.queue_is_full:
                self.redis.lpush(self.pending_queue, k)
        except:
            raise
