#! /usr/bin/env python
# coding=utf-8
import sys
import os
import time
import redis
from uuid import uuid4
from optparse import OptionParser
import logging
from logging import FileHandler, StreamHandler

Commands = {"upload", "download"}

REDIS = 'localhost'

USAGE = """
%prog <command> [options]
Commands:
""" + '\n'.join(["%10s: " % x for x in Commands])

def info(logger, msg):
    logger.info(msg)

def setup_logger(log=False, level=logging.INFO):
    FORMAT = '[%(asctime)-15s] %(message)s'
    logger = logging.getLogger(__name__)
    logger.setLevel(level)
    handlers = [StreamHandler(stream=sys.stdout)]
    if log:
        handlers.append(FileHandler("prototype.log"))
    format = logging.Formatter(FORMAT)
    for h in handlers:
        h.setFormatter(format)
        logger.addHandler(h)

    return logger

def get_connection_pool(host=REDIS, port=6379, db=0):
    pool = redis.ConnectionPool(host=host, port=port, db=db)
    return pool

def timing(f, *args, **kwargs):
    def deco(*args, **kwargs):
        b = time.time()
        r = f(*args, **kwargs)
        e = time.time()
        print("spending {} seconds".format(e-b))
        return r
    return deco

def get_file_content(path):
    assert os.path.exists(path)
    content = ''
    with open(path, 'rb') as f:
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


class PolarisRedis(redis.StrictRedis):
    """
    Redis
    Redis represents the connection to redis cluster
    """
    def __init__(self, logger, *args, **kwargs):
        super(PolarisRedis, self).__init__(*args, **kwargs)
        self.logger = logger or setup_logger()
        self.logger.info("Using {} to store thumbnails".format(self.__class__))

    @timing
    def write(self, userid, uuid, content, *args, **kwargs):
        """
        write interface will write thumbnails to redis for cache
        the file will be writen into redis as blob string:
        userid:uuid thumbnail
        """
        k = userid + ":" + uuid
        self.setex(k, 60, content)
        return k

    @timing
    def read(self, userid, uuid, out=None, *args, **kwargs):
        """
        read interface will read thumbnails from reids cache
        """
        k = userid + ":" + uuid
        content = self.get(k)
        target_dir = os.path.join(kwargs.get('prefix', '/tmp'), userid)
        if not os.path.exists(target_dir):
            os.mkdir(target_dir)
        o_file = out or uuid
        target_path = os.path.join(target_dir, o_file)
        with open(target_path, 'wb') as f:
            f.write(content)

class PolarisFile(object):
    """
    File
    File represents the local file system write/read operations
    """
    def __init__(self, logger, *args, **kwargs):
        super(PolarisFile, self).__init__()
        self.logger = logger or setup_logger()
        self.logger.info("Using {} to store thumbnails".format(self.__class__))

    @timing
    def write(self, userid, uuid, content, *args, **kwargs):
        target_dir = os.path.join(kwargs.get('prefix', '/tmp'), userid)
        if not os.path.exists(target_dir):
            os.mkdir(target_dir)
        path = os.path.join(target_dir, uuid)
        with open(path, 'wb') as f:
            f.write(content)

        return userid + ":" + uuid

    @timing
    def read(self, userid, uuid, out=None, *args, **kwargs):
        source_dir = kwargs.get('source_dir')
        target_dir = os.path.join(kwargs.get('prefix', '/tmp'), userid)
        if not os.path.exists(target_dir):
            os.mkdir(target_dir)

        o_file_name = out or uuid
        source_path = os.path.join(source_dir, userid, uuid)
        if not os.path.exists(source_path):
            raise Exception("Source file {} not exists".format(source_path))
        target_path = os.path.join(target_dir, o_file_name)

        content = ''
        with open(source_path, 'rb') as f:
            for l in f:
                content += l
        if content != '':
            with open(target_path, 'wb') as o:
                o.write(content)
        else:
            raise Exception("Empty source file {}!".format(source_path))

class Swift(object):
    """
    Swift
    Swift represents the interface to work with OpenStack/Swift cluster
    """

    @timing
    def write(self, userid, uuid, buffer, *args, **kwargs):
        pass

    @timing
    def read(self, userid, uuid, out, *args, **kwargs):
        pass

def upload(handler, source_path, target_dir=None, redis=True, **kwargs):
    logger = kwargs.get('logger')
    info(logger, "uploading {}...".format(source_path))
    assert os.path.exists(source_path)

    userid = kwargs.get('userid')
    uuid = uuid4().hex
    content = get_file_content(source_path)
    k = handler.write(userid, uuid, content, prefix=target_dir)
    return k

def download(handler, source_dir, target_dir, **kwargs):
    logger = kwargs.get('logger')
    info(logger, "downloading from {} to {}".format(source_dir, target_dir))

    userid = kwargs.get('userid', None)
    uuid = kwargs.get('uuid', None)
    assert userid and uuid

    o_file = kwargs.get('outfile', uuid)
    if source_dir != "Redis":
        source_file = os.path.join(source_dir, userid, uuid)
        if not os.path.exists(source_file):
            info(logger, "{} not exists!".format(source_file))
            raise Exception("File not exists!")

    abs_target_dir = os.path.abspath(target_dir)

    handler.read(userid, uuid, o_file, source_dir=source_dir, prefix=abs_target_dir)

def main():
    parser = OptionParser(USAGE)
    parser.add_option('-b', '--batch', action='store_true', dest='batch',
                      default=False, help='batch upload files')
    parser.add_option('-f', '--file', type='string', dest='file',
                      help='the file to upload')
    parser.add_option('-i', '--uuid', type='string', dest='uuid',
                      help='the uuid of a file')
    parser.add_option('-l', '--log', action='store_true', dest='log',
                      default=False, help='log all info in log file')
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
    logger = setup_logger(options.log)
    if len(args) != 1:
        parser.print_help()
        info(logger, "Error: config the command")
        return 1

    cmd = args[0]
    if cmd not in Commands:
        parser.print_help()
        info(logger, "Error: Unkown command: {}".format(cmd))
        return 1


    userid = options.userid or 'testing'
    prefix = options.prefix or "/tmp"

    pool = get_connection_pool(host=REDIS, port=6379)
    h = PolarisRedis(logger, connection_pool=pool) if options.redis \
    else PolarisFile(logger)

    if cmd == 'upload':
        file = options.file
        batch = options.batch
        if batch:
            fs = travel_dir(options.source_dir)
            for file in fs:
                k = upload(h, file, userid=userid, target_dir=prefix, logger=logger)
                info(logger, "Key:{}".format(k))
    elif cmd == 'download':
        assert options.uuid != ""
        source_dir = options.source_dir if not options.redis else "Redis"
        target_dir = options.target_dir
        o_file = options.outfile
        download(h, source_dir, target_dir, userid=userid, \
                 uuid=options.uuid, outfile=o_file, logger=logger)
    else:
        raise Exception("Unknown command!")

if __name__ == '__main__':
    sys.exit(main())
