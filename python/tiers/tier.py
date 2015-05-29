"""
julep.tiers.tier
~~~~~~~~~~~~~~~~

This module contains the interfaces responsible for storing data into a
concrete backend storage. Suppose user can choose where to store data, such as
local file system, a cache system, or an object storage. Currently plan to
support Redis cache firstly. In the future new storage maybe introduced for
data staging or persistence.

"""
import logging
import sys, time
from functools import wraps
from exceptions import ERedisDataMissing,ERedisKeyNotFound,EReliableQueueFull,\
    ERedisKeyError

# Only support three tiers and the third tier is reserved for future usage.
TIER_0 = 0
TIER_1 = 1
TIER_2 = 2

def timing(logger):
    def deco(f, *args, **kwargs):
        @wraps(f)
        def wrapper(*args, **kwargs):
            b = time.time()
            rt = f(*args, **kwargs)
            e = time.time()
            logger.info("function: {} spends {} seconds"\
                         .format(f.__name__, e-b))
            return rt
        return wrapper
    return deco

class ReliableQueue(object):
    """
    ReliableQueue represents the queues resident in Redis.A ReliableQueue is
    consist of two separate Redis lists:
        pending_queue [userid:uuid, ...]
        working_queue [userid:uuid, ...]
    The data access in Redis will look like following format:
        LPUSH pending_queue userid:uuid
        RPOPLPUSH pending_queue, working_queue, userid:uuid

    And a hash set stores real binary data:
        data_set: {userid:uuid => data, ...}

    param redis: the redis connection
    param size: the maximum length of a ReliableQueue, default value 1024
    param has_proxy: identify if a proxy is used infront of Redis
    param pending_queue: the pending queue name
    param working_queue: the working queue name
    param data_set: the internal redis hash table name which stores real data

    """
    redis = None

    def __init__(self, redis, size=1024, has_proxy=False, *args, **kwargs):
        self._size = size
        self._len = 0
        self.redis = redis
        self.has_proxy = has_proxy
        self.pending_queue = kwargs.get("pending_queue", "pending")
        self.working_queue = kwargs.get("working_queue", "working")
        self.data_set = kwargs.get("data_set", "thumbnail")
        self.logger = kwargs.get("logger")

    @property
    def size(self):
        return self._size

    @size.setter
    def size(self, new_size):
        self._size = new_size

    def __str__(self):return str(id(self))

    def queue_is_full(self):
        try:
            return self.redis.llen(self.pending_queue) >= self.size
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
            if self.queue_is_full():
                raise EReliableQueueFull(self)
            pipe = self.redis.pipeline(transaction=not self.has_proxy)
            pipe.lpush(self.pending_queue, k).hset(self.data_set, k, data)
            len, ok = pipe.execute()
            if not ok: # {k:data} is already in hash set
                self.logger.warn("Key:{} is already existing in hash set!"\
                                 .format(k))
            return len
        except:
            raise

    def dequeue(self):
        """
        dequeue uses Redis RPOPLPUSH command to pop an element and clone it
        into working queue atomically. After get the element key, invoking
        HGET command to retrieve real data from hash set. Compared to enqueue
        operation pipelining is unfeasible here, since we are not aware of the
        popped key.
        """
        data = None
        try:
            k = self.redis.rpoplpush(self.pending_queue, self.working_queue)
            if k:
                data = self.redis.hget(self.data_set, k)
                if not data:
                    raise ERedisDataMissing(k)
            else:
                raise ERedisKeyNotFound(k)
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
            pipe.lrem(self.working_queue, 0, k).hdel(self.data_set, k)
            pipe.execute()
        except:
            raise

    def requeue(self, k, data):
        try:
            if not self.redis.exists(k) and not self.queue_is_full():
                self.redis.lpush(self.pending_queue, k)
        except:
            raise

class StoreBase(object):
    """
    StoreBase repsents the abstract interface base class. The concrete class
    should inherit from TierBase to implement funcationalities. By now try to
    support two tiers, with the third tier reserved for future usage.
    The first tier should be used for the fastest backend storage in the whole
    infrastructure, the second one works  as a slower storge to failover,
    backup, or persistent data for the first tier.

    .--------.   failover    .--------.    failover    .--------.
    | tier 0 | ------------> | tier 1 | -------------> | tier 2 |
    *--------*               *--------*                *--------*

    :param tier: See :attr `tier0`.
    :param rank: Identify the tier in the whole storage layers.
                 0 means the first tier
                 1 means the second tier
                 2 means the third tier
                 3 or bigger, or negative value is not supported.

    """
    #! conn: represents the connection type, either of redis or swift
    conn = None
    #: next_tier represents the backup tier
    next_tier = None

    def __init__(self, conn, rank, next_tier, *args, **kwargs):
        self.conn = conn
        self.rank = rank
        self.next_tier = next_tier
        self.logger = kwargs.get('logger')
        self.args = args
        self.kwargs = kwargs

    def __str__(self): return self.__class__.__name__

    def upload(self, fname, data=None, *args, **kwargs):
        return self._upload(fname, data, *args, **kwargs)

    def download(self, fname, *args, **kwargs):
        return self._download(fname, *args, **kwargs)

    def failover(self, fname, data=None, callback=None, *args, **kwargs):
        self.logger.warn("{} Warn: failover to next tier:{}"\
                             .format(self, self.next_tier))
        return self._failvoer(fname, data, callback, *args, **kwargs)

class RedisStore(StoreBase):
    """
    RedisStore will handle all data transactions to/from Redis. Generally
    Redis is always faster than other backend. Consequently prefer to use Redis
    as the first tier to store data. In order to store and process data safely,
    reliable queues will be used. Reliabe queues are consist of two seperate
    queues:
        pending_queue
        working_queue
    the pending queue is used for storing data transient. While consumer starts
    processing data, the data is popped from ready queue w/ a copy cloned into
    working queue. That means the working queue always stores data which is
    under processing. This mechanism defend data loss once Redis crashes during
    data processing. After the data processed and persistented into slower tier
    the data in working queue could be removed forever.

    :param redis_conn: See :attr `redis_conn`

    """
    queue = None

    def __init__(self, conn, rank, next_tier, *args, **kwargs):
        super(RedisStore, self).__init__(conn,
                                         rank,
                                         next_tier,
                                         *args,
                                         **kwargs)
        self.queue = ReliableQueue(conn, size=kwargs.get('size', 1024), *args, **kwargs)

    def _upload(self, fname=None, data=None, *args, **kwargs):
        """
        In Redis the fname should be a key, while in polaris infrastructure
        userid and uuid are the only two uniques. Consequently the fname
        should be composed by userid and uuid to identify the unique key.
        By now try to set fname = userid:uuid

        uploading is acchieved by using reliable queue's enqueue operation.

        """
        if not fname:
            raise ERedisKeyError(fname)
        try:
            len = self.queue.enqueue(fname, data)
            return len
        except Exception as e:
            self.logger.info("Upload failed for {}".format(e))
            try:
                return self.failover(fname, data, self.next_tier.upload, *args, **kwargs)
            except:
                raise

    def _download(self, fname=None, *args, **kwargs):
        """
        In Redis fname should be a key. In Polaris infrastructure userid and
        uuid are the only two unique identifiers. So please make sure the fname
        format is "userid:uuid".

        download is completed via reliable queue's dequeue operation:
            1. RPOPLPUSH pending_queue, working_queue, k
            2. HGET working_queue, k
            3. After processing completed, invoking release queue operation
        """
        if not fname:
            raise Exception("Invalid key: {k}".format(k=fname))
        try:
            data = self.queue.dequeue()
            return data
        except (ERedisDataMissing, ERedisKeyNotFound) as e:
            self.logger.warn(e)
            try:
                return self.failover(fname,
                           callback=self.next_tier.download,
                           *args,
                           **kwargs)
            except:
                raise

    def _failover(self, fname, data=None, callback=None, *args, **kwargs):
        """
        failover tries to access the next tier to upload/dowanload the
        requested content. Since the fname in polaris infrastructure is set by
        userid:uuid, user should handle the fname => userid, uuid mapping.

        """
        try:
            self.next_tier.callback(fname, data, *args, **kwargs)
        except:
            raise

class SwiftStore(StoreBase):
    """
    SwiftStore handles all data transactions to/from Swift. Ideally Swift
    only act as the 2rd tier to failover for Redis and store the files that
    need pesistence forever.

    :param swift_conn: See :attr `swift_conn`

    """

    def __init__(self, conn, rank, next_tier, *args, **kwargs):
        super(SwiftStore, self).__init__(conn, rank, next_tier, *args, **kwargs)
        self.container = kwargs.get('container', self.conn.container_name)

    def _upload(self, fname, data, *args, **kwargs):
        content_type = kwargs.get('content_type')
        chunk_size = kwargs.get('chunk_size', 512)
        self.logger.debug("Uploading to {}:{} chunk size {}"\
                          .format(self.container, fname, chunk_size))
        try:
            return self.conn.puth_object(self.container,
                                         fname,
                                         contents=data,
                                         chunk_size=chunk_size,
                                         content_type=content_type
                                         )
        except Exception as e:
            self.logger.exception("Upload to {}:{} failed!"\
                                  .format(self.container, fname))

    def _download(self, fname, *args, **kwargs):
        container = kwargs.get('container')
        resp_chunk_size = kwargs.get('chunk_size')
        try:
            _, body = self.conn.get_object(container,
                                    fname,
                                    resp_chunk_size=resp_chunk_size)
            return body
        except Exception as e:
            self.logger.exception("Download {} from {} failed!"\
                                  .format(fname, container))

    def _failover(self, *args, **kwargs):
        raise NotImplemented