#!/usr/bin/env python2

# Canary monitor that performs repeated S3 bucket and object operations
# based on the Kyle Bader's work - 
# https://gist.github.com/mmgaggle/198004a3e88d124bed48747e8448eac3

import logging
import time
import sys
import os
import string
import random
import binascii

from argparse import ArgumentParser
from ConfigParser import ConfigParser

import boto
import boto.s3.connection

class RGWTest(object):

    test_bucket = "canary"

    def __init__(self, bucket_name=None):

        hostname, port = opts.rgw.split(':')
        self.conn = boto.connect_s3(
                                    aws_access_key_id=opts.access_key,
                                    aws_secret_access_key=opts.secret_key,
                                    host=hostname,
                                    port=int(port),
                                    is_secure=False,
                                    calling_format=boto.s3.connection.OrdinaryCallingFormat(),
                                   )
        self.canary_bucket = bucket_name if bucket_name else RGWTest.test_bucket
        self.objects = []   # list of object names in the canary bucket
        self.obj_size = opts.object_size
        self.seed_contents = self._create_seed()
        self.clean_up()

    def clean_up(self):

        for bkt in self.conn.get_all_buckets():
            if bkt.name == self.canary_bucket:
                logger.debug("clearing old bucket contents")
                for key in bkt.list():
                    bkt.delete_key(key)
                self.conn.delete_bucket(bkt.name)


    def _create_seed(self):

        return binascii.b2a_hex(os.urandom(int(opts.object_size/2)))

    def create_bucket(self):
        logger.debug("create bucket starting")
        start = time.time()

        try:
            self.bucket = self.conn.create_bucket(self.canary_bucket)
        except boto.exception.S3ResponseError as err:
            print("Canary bucket already exists")
            raise

        elapsed = float(time.time() - start)
        logger.info("Test:create_bucket, secs={}".format(elapsed))
        logger.debug("create bucket complete")

    def create_object(self, count=1):
        logger.debug("create objects starting - for {} objects".format(count))
        start = time.time()
        for n in xrange(count):
            obj_name = ''.join(random.choice(string.ascii_uppercase +
                                             string.digits) for _ in range(8))

            logger.debug("creating {}".format(obj_name))
            obj = self.bucket.new_key(obj_name)
            obj.set_contents_from_string(self.seed_contents)

            self.objects.append(obj_name)

        elapsed = float(time.time() - start)
        logger.info("Test:create_object, count={}, "
                    "bytes={}, secs={}".format(count,
                                               (count*opts.object_size),
                                               elapsed))
        logger.debug("create object(s) complete")

    def read_object(self):
        num_objects = len(self.objects)
        logger.debug("read object starting - {} object(s)".format(num_objects))
        start = time.time()

        for obj_name in self.objects:
            logger.debug("reading object {}".format(obj_name))
            key = self.bucket.get_key(obj_name)
            key.get_contents_as_string()

        elapsed = float(time.time() - start)
        logger.info("Test:read_object(s), count={}, "
                    "bytes={}, secs={}".format(len(self.objects),
                                               (num_objects*opts.object_size),
                                               elapsed,
                                               ))

        logger.debug("read object(s) complete")

    def delete_object(self):
        logger.debug("deleting objects from bucket")
        start = time.time()

        num_objects = len(self.objects)

        for obj_name in list(self.objects):
            logger.debug("deleting {}".format(obj_name))
            self.bucket.delete_key(obj_name)
            self.objects.remove(obj_name)

        elapsed = float(time.time() - start)
        logger.info("Test:delete_objects, count={}, "
                    "secs={}".format(num_objects,
                                     elapsed))
        logger.debug("delete object(s) complete")

    def delete_bucket(self):
        logger.debug("delete bucket starting")
        start = time.time()

        self.conn.delete_bucket(self.canary_bucket)
        self.conn.close()
        logger.debug("{} bucket deleted".format(self.canary_bucket))

        elapsed = float(time.time() - start)
        logger.info("Test:delete_bucket, secs={}".format(elapsed))
        logger.debug("delete bucket complete")

    def test_sequence(self):

        self.create_bucket()

        self.create_object(count=opts.object_count)

        self.read_object()

        self.delete_object()

        self.delete_bucket()

def get_opts():

    defaults = {}
    config = ConfigParser()

    dataset = config.read('parms.conf')
    if len(dataset) > 0:
        if config.has_section("config"):
            defaults.update(dict(config.items("config")))
        else:
            print("Config file detected, but the format is not supported. "
                  "Ensure the file has a single section [config]")
            sys.exit(12)
    else:
        # no config files detected, to seed the run time options
        pass

    parser = ArgumentParser()
    parser.add_argument("-r", "--rgw", type=str,
                        help="RGW http URL (host:port)")
    parser.add_argument("-i", "--interval", type=int,
                        default=60,
                        help="interval (secs) between cycles")
    parser.add_argument("-t", "--time-limit", type=int,
                        help="run time (mins) - default is run forever")
    parser.add_argument("-a", "--access-key", type=str,
                        help="S3 access key")
    parser.add_argument("-s", "--secret-key", type=str,
                        help="S3 secret key")
    parser.add_argument("-o", "--object-size", type=int,
                        default=65536,
                        help="object size in bytes to upload/read")
    parser.add_argument("-c", "--object-count", type=int,
                        default=1,
                        help="# objects to create in the canary bucket")

    parser.set_defaults(**defaults)
    runtime_opts = parser.parse_args()



    return runtime_opts


def main(opts):

    # define the must haves
    if not all([opts.access_key, opts.secret_key, opts.rgw]):
        print("Unable to continue. S3 credentials and RGW URL is needed")
        sys.exit(12)

    logger.info("Started")
    print("\nRun time parameters are:")
    parameters = sorted(opts.__dict__)
    width = max([len(_f) for _f in parameters])
    for prm in parameters:
        print("{:<{}} : {}".format(prm,
                                   width,
                                   getattr(opts, prm)))

    start_time = time.time()        # epoc start (secs)

    if opts.time_limit:
        end_time = start_time + (opts.time_limit * 60)

    rgw = RGWTest()

    now = time.time()
    print("\nRunning")
    try:
        while True:

            rgw.test_sequence()
            logger.debug("waiting for next test iteration")
            time.sleep(opts.interval)
            now = time.time()
            if opts.time_limit:
                if now > end_time:
                    break

    except KeyboardInterrupt:
        print("\nCleaning up")
        rgw.clean_up()

    logger.info("Finished")

if __name__ == "__main__":
    opts = get_opts()


    logger = logging.getLogger("canary")
    logger.setLevel(logging.DEBUG)
    handler = logging.FileHandler(filename="/var/log/query_canary.log",
                                  mode="a")

    fmt = logging.Formatter('%(asctime)s [%(levelname)-12s] - %(message)s')
    handler.setFormatter(fmt)
    logger.addHandler(handler)

    main(opts)
