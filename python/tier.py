"""
tier
~~~~~~~~~~

This module contains the interfaces responsible for storing data into a
concrete backend storage. Suppose user can choose where to store data, such as
local file system, a cache system, or an object storage. Currently plan to
support Redis cache firstly. In the future new storage maybe introduced for
data staging or persistence.

"""
from functools import wrap
import time

# Only support three tiers and the third tier is reserved for future usage.
TIER_0 = 0
TIER_1 = 1
TIER_2 = 2

def timing(logger):
    def deco(f, *args, **kwargs):
        @wrap(f)
        def wrapper(*args, **kwargs):
            b = time.time()
            rt = f(*args, **kwargs)
            e = time.time()
            logger.debug("function: {} spends {} seconds"\
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
    The data access in Redis will looks like following format:
        LPUSH pending_queue userid:uuid
        RPOPLPUSH pending_queue, working_queue, userid:uuid

    And a hash set storing real binary data:
        hash: {userid:uuid => data, ...}

    param redis: the redis connection
    param size: the maximum length of a ReliableQueue, default value 1024
    param hash: the internal redis hash table name

    """
    redis = None

    def __init__(self, redis, size=1024, *args, **kwargs):
        self._size = size
        self.redis = redis
        self.hash = kwargs.get('hash', 'thumbnails')
        self.pending_queue = kwargs.get('pending_queue', 'pending_queue')
        self.working_queue = kwargs.get('working_queue', 'working_queue')

    @property
    def size(self):
        return self._size

    @property.setter
    def size(self, new_size):
        self._size = new_size

    def enqueue(self, k, data):
        # enqueue will push data into pending queue
        try:
            if self.redis.llen(self.pending_queue) >= self.size:
                raise Exception("Reliable queue is full!")
            len = self.redis.lpush(self.pending_queue, k)
            self.redis.hset(self.hash, k, data)
            return len
        except:
            raise

    def dequeue(self, k):
        # dequeue firstly uses rpoplpush to get item to process
        # then retrieve binary data from redis hash table
        try:
            e = self.redis.rpoplpush(self.pending_queue, self.working_queue, k)
            data = self.redis.hget(e)
            return data
        except:
            raise

    def release(self, k):
        # Remove the key from working_queue completely
        try:
            self.redis.rpop(self.working_queue, k)
        except:
            raise

    def requeue(self, k, data):
        try:
            if not self.redis.exists(k):
                self.redis.lpush(self.pending_queue, k)
        except:
            raise

class BaseStore(object):
    """TierBase repsents the abstract interface base class. The concrete class
    should inherit from TierBase to implement funcationalities. By now try to
    support two tiers, with the third tier reserved for future usage.
    The first tier should used for the fastest backend storage, the second one
    plays is a slower storge to play as a backup or persistence storage to
    failover, backup, or persistent data for tier 0.

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
        self.args = args
        self.kwargs = kwargs

    def upload(self, fname=None, data=None, *args, **kwargs):
        return self._upload(fname, data, *args, **kwargs)

    def download(self, fname=None, *args, **kwargs):
        return self._download(fname, *args, **kwargs)

    def failover(self, *args, **kwargs):
        return self._failvoer(*args, **kwargs)

class RedisStore(BaseStore):
    """RedisStore will handle all data transactions to/from Redis. Generally
    Redis always faster than other storage. Consequently prefer to make Redis
    as the first tier to store data. In order to store and process data safely
    reliable queues will be used. Reliabe queues are consist of two seperate
    queues:
        pending_queue
        working_queue
    the pending queue is used to store data transient. While consumer starts
    processing data, the data is popped from ready queue w/ a copy cloned into
    working queue. That means the working queue always store the data that are
    under processing, this mechanism defend data loss once Redis crashes during
    data processing. After the data is processed and persistented into disk or
    swift, the data in working queue could be removed forever.

    :param redis_conn: See :attr `redis_conn`

    """

    queue = None

    def __init__(self, conn, rank, next_tier, *args, **kwargs):
        super(RedisStore, self).__init__(rank, next_tier, *args, **kwargs)
        self.queue = ReliableQueue(conn, size=kwargs.get('size', 1024))

    def _upload(self, fname=None, data=None, *args, **kwargs):
        """
        In Redis the fname should be a key, while in polaris infrastructure
        userid and uuid is the only unique identifier. So leave fname None,
        don't confuse! It is just conforming with Polaris.

        uploading is completed via reliable queue's enqueue operation
        """
        userid = kwargs.get('userid', None)
        uuid = kwargs.get('uuid', None)
        fname = userid.format(":{}", uuid) if all((userid, uuid)) else None
        if not fname:
            raise Exception("Invalid key: {k}".format(k=fname))
        try:
            len = self.queue.enqueue(fname, data)
        except :
            self.failover(fname, data, *args, **kwargs)
        finally:
            return len

    def _download(self, fname=None, *args, **kwargs):
        """
        In Redis fname should be a key. In Polaris infrastructure userid and
        uuid are the only unique identifiers. So please make sure the fname is
        consist of userid:uuid composition.

        downloading is completed via reliable queue's dequeue operation:
            1. RPOPLPUSH pending_queue, working_queue, k
            2. HGET working_queue, k
            3. After processing completed, invoking release queue operation
        """
        if not fname:
            raise Exception("Invalid key: {k}".format(k=fname))
        try:
            data = self.queue.dequeue(fname)
            return data
        except:
            self.failover(fname, *args, **kwargs)

    def _failover(self, *args, **kwargs):
        pass

class SwiftStore(BaseStore):
    """SwiftStore handles all data transactions to/from Swift. Ideally Swift
    only act as the 2rd tier to failover for Redis and store the files that
    need pesistence forever.

    :param swift_conn: See :attr `swift_conn`

    """

    def __init__(self, conn, rank, next_tier, *args, **kwargs):
        super(SwiftStore, self).__init__(conn, rank, next_tier, *args, **kwargs)

    def _upload(self, fname, data, *args, **kwargs):
        pass

    def _download(self, fname, *args, **kwargs):
        pass

    def _failover(self, *args, **kwargs):
        pass
