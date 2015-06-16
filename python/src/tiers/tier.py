"""
julep.tiers.tier
~~~~~~~~~~~~~~~~

This module contains the interfaces responsible for storing data into a
concrete backend storage. Suppose user can choose where to store data, such as
local file system, a cache system, or an object storage. Currently plan to
support Redis cache firstly. In the future new storage maybe introduced for
data staging or persistence.

"""
import urllib
from hashlib import md5
from queue import HashQueue, ERedisReleaseError

# Only support three tiers and the third tier is reserved for future usage.
TIER_0 = 0
TIER_1 = 1
TIER_2 = 2

class StoreBase(object):
    """
    StoreBase repsents the abstract interface base class. The concrete class
    should inherit from StoreBase to implement funcationalities.

    :param conn: Specify the conncetion type, ethier redis or swift
    :param rank: Identify the tier in the whole storage layers.
                 0 means the first tier
                 1 means the second tier
                 2 means the third tier
                 3 or bigger, or negative value is not supported.
    """
    #! conn: represents the connection type, either of redis or swift
    conn = None

    def __init__(self, conn, rank, logger):
        self.conn = conn
        self.rank = rank
        self.logger = logger

    def __str__(self): return self.__class__.__name__

    def upload(self, user_id, uuid, data=None, **options):
        return self._upload(user_id, uuid, data, **options)

    def download(self, user_id, uuid, data=None, **options):
        return self._download(user_id, uuid, **options)

    def delete(self, user_id, uuid, data=None, **options):
        return self._delete(user_id, uuid, **options)

class RedisStore(StoreBase):
    """
    RedisStore will handle all data transactions to/from Redis. Generally
    Redis is always faster than other backend. Consequently prefer to use Redis
    as the first tier to store data. In order to store and process data safely,
    reliable queues will be used. By now several queue types will be applied
    for testing. The following queues are to be implemented:
        HashQueue
        ReliableQueue

    :param redis_conn: See :attr `redis_conn`
    :param queue_cls: the internal queue for data storing
    :param logger the logger for logging
    :param kwargs varargs for queue initialization:
        size the queue size
        values the key name of list or hash set to store data
        stats the key name of list or hash set to store data status
        has_proxy identify if has a proxy infront of redis

    """
    queue = None

    def __init__(self, conn, rank, queue_cls, has_proxy, logger, **kwargs):
        super(RedisStore, self).__init__(conn, rank, logger)
        self.queue = queue_cls(conn, has_proxy=has_proxy, logger=logger, **kwargs)

    def _upload(self, user_id, uuid, data=None, **options):
        """
        In Redis the fname should be a key, while in polaris infrastructure
        user_id and uuid are the only two uniques. Consequently the fname
        should be composed by user_id and uuid to identify the unique key.
        By now try to set fname = user_id:uuid

        uploading is achieved by using enqueue operation.

        """
        if not user_id:
            raise ValueError("user_id is None!")
        q = md5("{}_queue".format(user_id)).hexdigest()
        try:
            self.queue.enqueue(q, uuid, data)
        except:
            raise

    def _download(self, user_id, uuid, **options):
        """
        In Redis fname should be a key. In Polaris infrastructure user_id and
        uuid are the only two unique identifiers. So please make sure the fname
        format is "user_id:uuid".

        download is completed via reliable queue's dequeue operation:
            1. RPOPLPUSH pending_queue, working_queue, k
            2. HGET working_queue, k
            3. After processing completed, invoking release queue operation
        """
        if not user_id:
            raise ValueError("user_id is None!")
        q = md5("{}_queue".format(user_id)).hexdigest()
        try:
            k, data = self.queue.dequeue(q, uuid)
            return k, data
        except:
            raise

    def _delete(self, user_id, uuid, **options):
        """
        Remove fname from redis forever.
        """
        if not user_id:
            raise ValueError("user_id is None!")
        q = md5("{}_queue".format(user_id)).hexdigest()
        try:
            user_len, total_len = self.queue.release(q, uuid)
            return user_len, total_len
        except:
            raise

class SwiftStore(StoreBase):
    """
    SwiftStore handles all data transactions to/from Swift. Ideally Swift
    only act as the 2rd tier to failover for Redis and store the files that
    need pesistence forever.

    :param swift_conn: See :attr `swift_conn`

    """

    def __init__(self, conn, rank, logger, **kwargs):
        super(SwiftStore, self).__init__(conn, rank, logger)
        self.kwargs = kwargs

    def _upload(self, user_id, uuid, data, **options):
        ctype = options.get('ctype')
        chunk_size = options.get('chunk_size')
        path_format = options.get('path_format')
        container = options.get('container')
        if container is None:
            container = self.conn.container_name

        if not all((ctype, chunk_size, container, path_format)):
            raise Exception("parameters wrong! ctype:{}, chunk_size:{},\
                            container:{}, path_format:{}"\
                            .format(ctype, chunk_size, container, path_format))

        full_path = urllib.quote(
            path_format.format(user_id=user_id, uuid=uuid))

        try:
            self.conn.put_object(container,
                                 full_path,
                                 contents=data,
                                 chunk_size=chunk_size,
                                 content_type=ctype
                                 )
            return container
        except:
            self.logger.exception("Download {} from {} failed!"
                                  .format(full_path, container))
            raise

    def _download(self, user_id, uuid, **options):
        chunk_size = options.get('chunk_size')
        path_format = options.get('path_format')
        container = options.get('container')
        if container is None:
            container = self.conn.container_name

        if not all((chunk_size, container, path_format)):
            raise Exception("parameters wrong! chunk_size:{},\
                            container:{}, path_format:{}"
                            .format(chunk_size, container, path_format))

        full_path = urllib.quote(
            path_format.format(user_id=user_id, uuid=uuid))
        try:
            _, body_f = self.conn.get_object(container,
                                             full_path,
                                             resp_chunk_size=chunk_size
                                             )
            return body_f
        except:
            self.logger.exception("Download {} from {} failed!"\
                                  .format(full_path, container))
            raise

    def _delete(self, user_id, uuid, **options):
        path_format = options.get('path_format')
        container = options.get('container')
        if container is None:
            container = self.conn.container_name

        if not all((path_format, container)):
            raise Exception("parameters wrong! container:{}, path_format:{}"
                            .format(container, path_format))

        full_path = urllib.quote(
            path_format.format(user_id=user_id, uuid=uuid))
        try:
            self.conn.delete_object(container, full_path)
        except:
            self.logger.exception("delete {} from {} failed!"\
                                  .format(full_path, container))
            raise

class PolarisStageStore(object):
    """
    PolarisStageStore comprises different tiers of backend store. By now try to
    support two tiers, with the third tier reserved for future usage.
    The first tier should be used for the fastest backend storage in the whole
    infrastructure, the second one works  as a slower storge to failover,
    backup, or persistent data for the first tier.

    .--------.   failover    .--------.    failover    .--------.
    | tier 0 | ------------> | tier 1 | -------------> | tier 2 |
    *--------*               *--------*                *--------*

    :param redis_conn the redis connection
    :param swift_conn the swift connection
    :param size specify the capacity, typically it identifies the queue size.
    :param redis conf:
        has_proxy specify if a proxy will be infront of Redis.
        queue_cls only for Redis, specify the internal queue type
    """
    _tier_0 = None
    _tier_1 = None
    _tier_2 = None # Reserved

    def __init__(self, redis_conn, swift_conn, queue_cls=HashQueue,
                 size=1024, has_proxy=False, logger=None, **kwargs):
        self._tier_0 = RedisStore(redis_conn,
                                  TIER_0,
                                  queue_cls,
                                  has_proxy,
                                  logger,
                                  **kwargs)

        self._tier_1 = SwiftStore(swift_conn, TIER_1, logger, **kwargs)
        self.logger = logger

    @property
    def tier_0(self):
        return self._tier_0

    @property
    def tier_1(self):
        return self._tier_1

    def upload(self, tier, user_id, uuid, data, **options):
        try:
            if tier == TIER_0:
                return self.tier_0.upload(user_id, uuid, data, **options)
            elif tier == TIER_1:
                return self.tier_1.upload(user_id, uuid, data, **options)
            else:
                raise ValueError("tier '{}' is not supported!".format(tier))
        except ValueError:
            raise
        except Exception as e:
            self.logger.debug("Upload {} failed for {}".format(uuid, e))
            return self._failover(TIER_1,
                                  user_id,
                                  uuid,
                                  data,
                                  self.tier_1.upload,
                                  **options)

    def download(self, tier, user_id, uuid, **options):
        try:
            if tier == TIER_0:
                data = self.tier_0.download(user_id, uuid, **options)
            elif tier == TIER_1:
                data = self.tier_1.download(user_id, uuid, **options)
            else:
                raise ValueError("tier '{}' is not supported!".format(tier))
            return data
        except ValueError:
            raise
        except Exception as e:
            self.logger.debug("Fail to download for {}".format(e))
            return self._failover(TIER_1,
                                  user_id,
                                  uuid,
                                  None,
                                  on_failover=self.tier_1.download,
                                  **options)

    def delete(self, tier, user_id, uuid, **options):
        try:
            if tier == TIER_0:
                return self.tier_0.delete(user_id, uuid)
            elif tier == TIER_1:
                return self.tier_1.delete(user_id, uuid, **options)
            else:
                raise ValueError("tier '{}' is not supported!".format(tier))
        except (ValueError, ERedisReleaseError):
            raise
        except Exception as e:
            self.logger.debug("Fail to delete for {}".format(e))
            return self._failover(TIER_1,
                                  user_id,
                                  uuid,
                                  None,
                                  on_failover=self.tier_1.delete,
                                  **options)

    def _failover(self, tier, user_id, uuid,
                  data=None, on_failover=None, **options):
        self.logger.warn("{} failover to next tier:{}"\
                             .format(self, tier))
        return on_failover(user_id, uuid, data=data, **options)
