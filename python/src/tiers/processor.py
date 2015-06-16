#! /usr/bin/env pytho
# coding=utf-8

import sys
import os
import logging
from logging import StreamHandler, FileHandler
from tier import PolarisStageStore, TIER_0
from queue import HashQueue

from optparse import OptionParser
import gevent
from hashlib import md5
import redis
import time
from functools import wraps
from uuid import uuid4
from hashlib import md5


Commands = {"upload", "download", "delete"}

REDIS = 'localhost'
PORT = 22121

USAGE = """
%prog <command> [options]
Commands:
""" + '\n'.join(["%10s: " % x for x in Commands])

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
    FORMAT = '[%(asctime)s %(levelname)s %(pathname)s %(funcName)s %(lineno)s] %(message)s'
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

logger = setup_logger(level=logging.DEBUG)

def get_file_content(fpath):
    content = str()
    with open(fpath, 'r') as f:
        for l in f:
            content += l
    return content

def travel_dir(path):
    """
    Return all the files in a directory
    """
    assert os.path.exists(path)
    path, _, fs = os.walk(path).next()
    return [os.path.join(os.path.abspath(path), f) for f in fs]

@timing(logger)
def upload(client, user_id, uuid, content):
    logger.info("uploading {}-{}".format(user_id, uuid))
    client.upload(TIER_0, user_id, uuid, data=content)

@timing(logger)
def download(client, user_id, uuid):
    logger.info("downloading {}-{}".format(user_id, uuid))
    return client.download(TIER_0, user_id, uuid)

@timing(logger)
def delete(client, user_id, uuid):
    logger.info("deleting {}-{}".format(user_id, uuid))
    return client.delete(TIER_0, user_id, uuid)

@timing(logger)
def flush(client, userid, uuid, output, content):
    with open(output, 'wb') as f:
        f.write(content)
        f.flush()
    if os.path.exists(output):
        client.delete(userid, uuid)

def main():
    parser = OptionParser(USAGE)
    parser.add_option('-b', '--batch', action='store_true', dest='batch',
                      default=False, help='batch upload files')
    parser.add_option('-d', '--dir', type='string', dest='dir',
                      help='the dir to upload')
    parser.add_option('-e', '--expire', type='int', dest='expire',
                      default=-1, help='expired after expire seconds')
    parser.add_option('-f', '--file', type='string', dest='file',
                      help='the file to upload')
    parser.add_option('-i', '--uuid', type='string', dest='uuid',
                      help='the uuid of a file')
    parser.add_option('-o', '--outfile', type='string', dest='outfile',
                      help='the generated file')
    parser.add_option('-p', '--prefix', type='string', dest='prefix',
                      help='the prefix directory of generated files')
    parser.add_option('-r', '--redis', action='store_true', dest='redis',
                      default=False, help='using redis to store file')
    parser.add_option('-s', '--source_dir', type='string', dest='source_dir',
                      help='the source file directory')
    parser.add_option('-t', '--target_dir', type='string', dest='target_dir',
                      help='the target file directory')
    parser.add_option('-u', '--userid', type='string', dest='userid',
                      help='the user id')

    options, args = parser.parse_args()
    if len(args) != 1:
        parser.print_help()
        logger.error("Error: config the command")
        return 1

    cmd = args[0]
    if cmd not in Commands:
        parser.print_help()
        logger.error("Error: Unkown command: {}".format(cmd))
        return 1

    redis_conn = redis.client.StrictRedis(host=REDIS, port=PORT)
    if options.redis:
        redis_conf = {'has_proxy': True,
                      'queue_cls':HashQueue,
                      'logger':logger,
                      'size':1024
                      }
        client = PolarisStageStore(redis_conn, None, **redis_conf)
    if options.dir:
        fs = travel_dir(options.dir)
    if options.file:
        fs = [options.file]
    if cmd == 'upload':
        # Trick filename is split by , to simulate multiple upload
        contents = [get_file_content(fpath=f) for f in fs]
        #gevent.joinall([gevent.spawn(upload, client, options.userid,
        #                             md5(content).hexdigest(), content)
        #                for content in contents])
        gevent.joinall([gevent.spawn(upload, client, options.userid,
                                     md5(content).hexdigest(), content)
                        for content in contents])
    elif cmd == 'download':
        k, content = download(client, userid=options.userid, uuid=options.uuid)
        flush(client,
              userid=options.userid,
              uuid=options.uuid,
              output=options.outfile,
              content=content)

    elif cmd == 'delete':
        qname = md5(options.userid + "_queue").hexdigest()
        for k in redis_conn.hkeys(qname):
            delete(client, options.userid, k)

if __name__ == '__main__':
    sys.exit(main())
