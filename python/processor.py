#! /usr/bin/env pytho
# coding=utf-8

import sys
import os
import logging
from logging import StreamHandler, FileHandler
import redis
from tiers.tier import RedisStore, timing, TIER_0

from optparse import OptionParser
import gevent
from hashlib import md5

Commands = {"upload", "download"}

REDIS = 'localhost'
PORT = 6379

USAGE = """
%prog <command> [options]
Commands:
""" + '\n'.join(["%10s: " % x for x in Commands])

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

@timing(logger)
def upload(client, userid, uuid, content):
    logger.info("downloading {}".format(userid+":"+uuid))
    client.upload(userid+":"+uuid, data=content)

@timing(logger)
def download(client, userid, uuid):
    logger.info("downloading {}".format(userid+":"+uuid))
    fname = userid + ":" + uuid
    return client.download(fname)

@timing(logger)
def flush(client, userid, uuid, output, content):
    with open(output, 'wb') as f:
        f.write(content)
        f.flush()
    if os.path.exists(output):
        client.queue.release(userid+":"+uuid)

def main():
    parser = OptionParser(USAGE)
    parser.add_option('-b', '--batch', action='store_true', dest='batch',
                      default=False, help='batch upload files')
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
        client  = RedisStore(redis_conn, TIER_0, None, logger=logger)
    if cmd == 'upload':
        # Trick filename is split by , to simulate multiple upload
        contents = [get_file_content(fpath=f) for f in options.file.split(',')]
        gevent.joinall([gevent.spawn(upload, client, options.userid,
                                     md5(content).hexdigest(), content)
                        for content in contents])
    elif cmd == 'download':
        k, content = download(client,
                           userid=options.userid,
                           uuid=options.uuid)
        flush(client,
              userid=options.userid,
              uuid=k.split(':')[-1],
              output=k.split(':')[-1],
              content=content)

if __name__ == '__main__':
    sys.exit(main())
