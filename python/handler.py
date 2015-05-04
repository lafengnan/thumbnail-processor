#! /usr/bin/env python
# coding=utf-8
import sys
import os
import time
import redis
from uuid import uuid4
from optparse import OptionParser

Commands = ("upload", "download")

USAGE = """
%prog <command> [options]
Commands:
""" + '\n'.join(["%10s: " % x for x in Commands])

def get_connection_pool(host='localhost', port=6379, db=0):
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
    buffer = ''
    with open(path, 'rb') as f:
        for l in f:
            buffer += l
    return buffer


class PolarisRedis(redis.Redis):
    """
    Redis
    Redis represents the connection to redis cluster
    """
    def __init__(self, *args, **kwargs):
        super(PolarisRedis, self).__init__(*args, **kwargs)
        print("Using Redis to store thumbnails")

    @timing
    def write(self, userid, uuid, buffer, *args, **kwargs):
        """
        write interface will write thumbnails to redis for cache
        the file will be writen into redis as blob string:
        userid:uuid thumbnail
        """
        k = userid + ":" + uuid
        self.set(k, buffer)
        return k

    @timing
    def read(self, userid, uuid, out, *args, **kwargs):
        """
        read interface will read thumbnails from reids cache
        """
        k = userid + ":" + uuid
        content = self.get(k)
        with open(out, 'wb') as f:
            f.write(content)

class File(object):
    """
    File
    File represents the local file system write/read operations
    """
    def __init__(self):
        super(File, self).__init__()
        print("Using Local FS to store thumbnails")

    @timing
    def write(self, userid, uuid, buffer, *args, **kwargs):
        if not os.path.exists(userid):
            os.mkdir(userid)
        path = os.path.join(userid, uuid)
        with open(path, 'wb') as f:
            f.write(buffer)

    @timing
    def read(self, userid, uuid, out):
        path = os.path.join(userid, uuid)
        buffer = ''
        with open(path, 'rb') as f:
            for l in f:
                buffer += l
        with open(out, 'wb') as o:
            o.write(buffer)

class Swift(object):
    """
    Swift
    Swift represents the interface to work with OpenStack/Swift cluster
    """

    @timing
    def write(self, userid, uuid, buffer, *args, **kwargs):
        pass

    @timing
    def read(self, userid, uuid, out):
        pass

def main():
    parser = OptionParser(USAGE)
    parser.add_option('-f', '--file', type='string', dest='thumbnail',
                      help='the thumbnail to upload')
    parser.add_option('-o', '--output', type='string', dest='output',
                      help='the generated file')
    parser.add_option('-r', '--redis', action='store_true', dest='redis',
                      default=False, help='using redis to store file')
    parser.add_option('-t', '--thumbnailid', type='string', dest='uuid',
                      help='the uuid of a thumbnail')
    parser.add_option('-u', '--userid', type='string', dest='userid',
                      help='the user id')

    options, args = parser.parse_args()
    if len(args) != 1:
        parser.print_help()
        print "Error: config the command"
        return 1

    cmd = args[0]
    if cmd not in Commands:
        parser.print_help()
        print "Error: Unkown command: ", cmd
        return 1
    
    pool = get_connection_pool() 
    h = PolarisRedis(connection_pool=pool) if options.redis else File()
    userid = options.userid or 'testing'
    if cmd == 'upload':
        uuid = uuid4().hex
        buffer = get_file_content(options.thumbnail)
        k = h.write(userid, uuid, buffer)
        print("key: {}".format(k))
    elif cmd == 'download':
        h.read(userid, options.uuid, options.output)
    else:
        raise Exception("Unknown command!")

if __name__ == '__main__':
    sys.exit(main())
