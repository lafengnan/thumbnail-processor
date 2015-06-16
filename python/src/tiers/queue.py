"""
julep.tiers.queue
~~~~~~~~~~~~~~~~~

This module contains the interfaces responsible for Reliable Redis Queues
implementations. To make Redis work as a store more reliable, a reliable queue
is used to protect data.

"""

import time
import pickle
from tier_exceptions import ERedisDataMissing, ERedisKeyNotFound, \
    ERedisQueueFull, ERedisReleaseError, ERedisEmptyQueue

JOB_ENQUEUED = 1
JOB_DEQUEUED = 2
JOB_RESERVED = 3

class Job(object):
    """
    Job represents a task that identify an file operation.
    param key: the key in redis
    param timeout: If the job state not change for timeout seconds,
                   it will be revoked. 0 means never timeout.
    """
    def __init__(self, key, timeout=0):
        self.key = key
        self.state = 0
        self.enqueue_ts = None
        self.dequeue_ts = None
        self.timeout = timeout

    def change_state(self, new_state):
        if new_state not in [JOB_ENQUEUED, JOB_DEQUEUED, JOB_RESERVED]:
            raise ValueError(new_state)
        self.state = new_state
        if self.state == JOB_ENQUEUED:
            self.enqueue_ts  = time.time()
        elif self.state == JOB_DEQUEUED:
            self.dequeue_ts = time.time()
        else:
            pass

class QueueBase(object):
    """
    QueueBase represents the abstract queue interface which will be used in
    Redis storage. One queue could be implemented via comprising lists, hash
    set or sorted set.

    param conn: connection to redis
    param size: the maximum elements one queue could hold meanwhile
    param lists: the hash set name to hold each userid's uuid counts:
        lists hash_queue: {'xxxxx':10, 'yyyyy':11}
        counter queue_length: interge
    param has_proxy: identify if a proxy server will resident infront of redis
    param logger: the logger for logging

    """
    redis = None

    def __init__(self, conn, size=1024, list="hash_queue", jobs="jobs",
                 has_proxy=False, logger=None):
        self.redis = conn
        self._size = size
        self.list = list
        self.jobs = jobs
        self.queue_len = "queue_length"
        self.has_proxy = has_proxy
        self.logger = logger

    def __str__(self): return self.__class__.__name__

    def __len__(self):
        try:
            len = self.redis.get(self.queue_len)
            return 0 if not len else int(len)
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

    def stats(self, qname=None):
        stats = dict()
        stats['size'] = self.size
        stats['length'] = len(self)
        if qname:
            try:
                stats[qname] = self.redis.hget(self.list, qname)
            except:
                raise
        return stats

    def enqueue(self, qname, k, data):
        raise NotImplemented

    def dequeue(self, qname, k=None):
        raise NotImplemented

    def requeue(self, qname, k):
        raise NotImplemented

    def release(self, qname, k):
        raise NotImplemented

class HashQueue(QueueBase):
    """
    HashQueue will store data into a hash set. Uploading and downloading will
    be implemented via enqueue and dequeue operations. To protect data safety,
    the data should not be deleted until the data process completes.
    The hash queue comprises two seprate hash sets:
        md5(userid_queue): {uuid => data}
        jobs:   {uuid => job}
    and a list recording each userid's uuid counts, a counter recording all the
    uuid counts:
        counter: queue_length: interge
        list:   user_ids: {'userid1_queue':interge, 'userid2_queue':interge...}
    """
    def enqueue(self, qname, k, data):
        try:
            if not self.queue_is_full:
                job = Job(k)
                job.change_state(JOB_ENQUEUED)
                job = pickle.dumps(job)

                pipe = self.redis.pipeline(transaction=not self.has_proxy)
                pipe.hset(qname, k, data)\
                    .hset(self.jobs, k, job)\
                    .hincrby(self.list, qname, 1)\
                    .incr(self.queue_len)
                ok, job_ok, _, _ = pipe.execute()
                if not ok or not job_ok:
                    # The key has already been set in hash set
                    if self.logger:
                        self.logger.debug("{} was rewritten by new data".format(k))
            else:
                raise ERedisQueueFull(self)
        except:
            raise

    def dequeue(self, qname, k=None):
        try:
            pipe = self.redis.pipeline(transaction=not self.has_proxy)
            pipe.hget(qname, k).hget(self.jobs, k)
            data, job = pipe.execute()
            if not data:
                # case 1. data has been removed
                # case 2. data missing happened
                if not job:
                    raise ERedisKeyNotFound(k)
                else:
                    raise ERedisDataMissing(k)
            job = pickle.loads(job)
            job.change_state(JOB_DEQUEUED)
            self.redis.hset(self.jobs, k, pickle.dumps(job))
            return k, data
        except:
            raise

    def release(self, qname, k):
        try:
            if len(self) == 0:
                return 0, 0

            job = self.redis.hget(self.jobs, k)
            if job and pickle.loads(job).state != JOB_DEQUEUED:
                raise ERedisReleaseError(k)
            if not job:
                raise ERedisKeyNotFound(k)
            pipe = self.redis.pipeline(transaction=not self.has_proxy)
            pipe.hdel(qname, k)\
                .hdel(self.jobs, k)\
                .hincrby(self.list, qname, -1)\
                .decr(self.queue_len)
            _, _, qname_len, q_len = pipe.execute()
            return qname_len, q_len
        except:
            raise

class ReliableQueue(QueueBase):
    """
    ReliableQueue represents the queues resident in Redis.A ReliableQueue is
    consist of two separate Redis lists:
        pending_queue: pending_md5(userid_queue) [uuid, ...]
        working_queue: working_md5(userid_queue) [uuid, ...]
    the pending queue is used for storing data transient. While consumer starts
    processing data, the data is popped from ready queue w/ a copy cloned into
    working queue. That means the working queue always stores data which is
    under processing. This mechanism defend data loss once Redis crashes during
    data processing. After the data processed and persistented into slower tier
    the data in working queue could be removed forever.

    The data access in Redis will look like following format:
        LPUSH pending_queue uuid
        RPOPLPUSH pending_queue, working_queue, uuid

    And two hash sets store data and job status:
        values: md5(uerid_queue): {uuid => data, ...}
        jobs: {uuid => job, ...}
    And one total counter, one specific counter for one userid:
        queue_length: interge
        list: {md5(userid_queue):integer, ...}

    param pending_queue: the pending queue name
    param working_queue: the working queue name

    """
    def __init__(self, conn, size=1024, list="reliable_queue", jobs="jobs",
                 has_proxy=False, logger=None, **kwargs):
        super(ReliableQueue, self).__init__(conn,
                                            size,
                                            list,
                                            jobs,
                                            has_proxy,
                                            logger,
                                            )
        self.pending_prefix = kwargs.get("pending_queue", "pending")
        self.working_prefix = kwargs.get("working_queue", "working")

    def enqueue(self, qname, k, data):
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
                raise ERedisQueueFull(self)
            job = Job(k)
            job.change_state(JOB_ENQUEUED)

            pipe = self.redis.pipeline(transaction=not self.has_proxy)
            pipe.lpush(self.pending_prefix+"_"+qname, k)\
                .hset(qname, k, data)\
                .hset(self.jobs, k, pickle.dumps(job))\
                .hincrby(self.list, qname, 1)\
                .incr(self.queue_len)
            len, ok, _, _, _ = pipe.execute()
            if not ok: # {k:data} is already in hash set
                self.logger.warn("Key:{} is already existing in hash set!"\
                                 .format(k))
            return len
        except:
            raise

    def dequeue(self, qname, k=None):
        """
        dequeue uses Redis RPOPLPUSH command to pop an element and clone it
        into working queue atomically. After get the element key, invoking
        HGET command to retrieve real data from hash set. Compared to enqueue
        operation pipelining is unfeasible here, since Reliable queue uses
        Redis lists to store elements, and element is retrieved via
        RPOPLPUSH operation. This mechanism makes the key we retrieve from
        queue is different from passed in parameter: k.
        """

        data = job = None
        try:
            tmp_k = self.redis.rpoplpush(self.pending_prefix+"_"+qname,
                                         self.working_prefix+"_"+qname)
            if tmp_k:
                pipe = self.redis.pipeline(transaction=not self.has_proxy)
                pipe.hget(qname, tmp_k).hget(self.jobs, tmp_k)
                data, job = pipe.execute()
                if not data and job:
                    raise ERedisDataMissing(tmp_k)
                elif not data and not job:
                    raise ERedisKeyNotFound(tmp_k)

                job = pickle.loads(job)
                job.change_state(JOB_DEQUEUED)
                job = pickle.dumps(job)
                pipe.hset(self.jobs, tmp_k, job)
            else:
                raise ERedisEmptyQueue(qname)
            return tmp_k, data
        except:
            raise

    def release(self, qname, k):
        """
        After data is processed, the original data could be cleaned up. Since
        Reliable queue comprises six components:
            pending queue
            working queue
            data set
            job set
            total counter
            specific user counter
        Although the original data has already been released from pending
        queue, but the key still reside in working queue and the real data also
        hide in data set.
        """
        try:
            job = self.redis.hget(self.jobs, k)
            if job and pickle.loads(job).state != JOB_DEQUEUED:
                raise ERedisReleaseError(k)

            pipe = self.redis.pipeline(transaction=not self.has_proxy)
            pipe.lrem(self.working_prefix+"_"+qname, 0, k)\
                .hdel(qname, k)\
                .hdel(self.jobs, k)\
                .hincrby(self.list, qname, -1)\
                .decr(self.queue_len)
            _, _, _, qname_len, q_len = pipe.execute()
            return qname_len, q_len
        except:
            raise
