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
from logging import StreamHandler, FileHandler
from exceptions import ERedisDataMissing, ERedisKeyNotFound, \
ERedisQueueFull, ERedisKeyError

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
            logger.debug("function: {} spends {} seconds"\
                         .format(f.__name__, e-b))
            return rt
        return wrapper
    return deco

def setup_logger(log=True, level=logging.INFO, log_file="tiers.log"):
    FORMAT = '[%(asctime)-15s] %(message)s'
    logger = logging.getLogger(__name__)
    logger.setLevel(level)
    handlers = [StreamHandler(stream=sys.stdout)]
    if log:
        handlers.append(FileHandler(log_file))
    format = logging.Formatter(FORMAT)
    for h in handlers:
        h.setFormatter(format)
        logger.addHandler(h)

    return logger

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
    :param next_tier: the next tier of self
    :param logger: the logging logger object

    """
    #! conn: represents the connection type, either of redis or swift
    conn = None
    #: next_tier represents the backup tier
    next_tier = None

    def __init__(self, conn, rank, next_tier, logger, **kwargs):
        self.conn = conn
        self.rank = rank
        self.next_tier = next_tier
        self.logger = logger
        self.kwargs = kwargs

    def __str__(self): return self.__class__.__name__

    def upload(self, fname, data=None, *args, **kwargs):
        return self._upload(fname, data, *args, **kwargs)

    def download(self, fname, *args, **kwargs):
        return self._download(fname, *args, **kwargs)

    def delete(self, fname, *args, **kwargs):
        return self._delete(fname, *args, **kwargs)

    def failover(self, fname, data=None, callback=None, *args, **kwargs):
        if self.logger:
            self.logger.warn("{} Warn: failover to next tier:{}"\
                             .format(self, self.next_tier))
        return self._failvoer(fname, data, callback, *args, **kwargs)

class RedisStore(StoreBase):
    """
    RedisStore will handle all data transactions to/from Redis. Generally
    Redis is always faster than other backend. Consequently prefer to use Redis
    as the first tier to store data. In order to store and process data safely,
    reliable queues will be used. By now several queue types will be applied
    for testing. The following queues are to be implemented:
        HashQueue
        ReliableQueue
        NamingReliableQueue

    :param redis_conn: See :attr `redis_conn`
    :param queue_cls: the internal queue for data storing
    :param kwargs varargs for queue initialization:
        size the queue size
        values the key name of list or hash set to store data
        stats the key name of list or hash set to store data status
        has_proxy identify if has a proxy infront of redis
        logger the logger for logging
        retries the retry limitations

    """
    queue = None

    def __init__(self, conn, rank, next_tier, queue_cls, logger, **kwargs):
        super(RedisStore, self).__init__(conn,
                                         rank,
                                         next_tier,
                                         logger,
                                         **kwargs)
        self.queue = queue_cls(conn, logger=logger, **kwargs)

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
        except :
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
            k, data = self.queue.dequeue(fname)
            return k, data
        except ERedisDataMissing as e:
            self.logger.warn(e)
            return self.failover(fname,
                           callback=self.next_tier.download,
                           *args,
                           **kwargs)

    def _delete(self, fname, *args, **kwargs):
        """
        Remove fname from redis forever.
        """
        if not fname:
            raise Exception("Invalid key:{k}".format(k=fname))
        try:
            self.queue.release(fname)
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

    def _delete(self, fname, *args, **kwargs):
        pass

    def _failover(self, *args, **kwargs):
        raise NotImplemented
