"""
julep.tiers.exceptions
~~~~~~~~~~~~~~~~~~~~~~

This module contains the exceptions responsible for tiers interfaces.

"""

class ERedisDataMissing(Exception):
    """
    Data missing happend in Redis. Should be catched!

    """
    def __init__(self, key):
        self.key = key

    def __str__(self):
        return "Redis Error: Data missing for {}".format(self.key)

class ERedisKeyNotFound(Exception):
    """
    Key is not found in Redis.

    """
    def __init__(self, key):
        self.key = key

    def __str__(self):
        return "Redis Warning: {} was not found".format(self.key)

class ERedisQueueFull(Exception):
    """
    A reliable queue is full
    """
    def __init__(self, q):
        self.q = q

    def __str__(self):
        return "Redis Error: Queue {} is full!".format(self.q)

class ERedisKeyError(Exception):
    """
    Invalid key
    """
    def __init__(self, key):
        self.key = key

    def __str__(self):
        return "Redis Error:Invlid key: {}".format(self.key)
