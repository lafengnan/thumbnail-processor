import redis
from functools import wraps

# redis driver 2.10.0 introduces new Exception type
def redis_driver_version(major=2, mid=10, minor=0):
    def validator(f, *args, **kwargs):
        @wraps(f)
        def deco(*args, **kargs):
            r_major = redis.VERSION[0]
            r_mid = redis.VERSION[1]
            r_minor = redis.VERSION[-1]
            if r_major > major or (r_major == major and r_mid > mid)\
                    or (r_major == major and r_mid == mid and r_minor > minor):
                rv = f(*args, **kwargs)
                return rv
        return deco
    return validator
