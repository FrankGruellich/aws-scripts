#!/usr/bin/python

from boto.s3.connection import S3Connection
import boto.exception
from Queue import Queue
from threading import Thread
from argparse import ArgumentParser, FileType
from sys import exit
from time import sleep
from random import random

def worker(queue):
    while True:
        wait = 0
        item = queue.get()
        mp = item["mp"]
        src_bucket_name = item["src_bucket_name"]
        src_key_name = item["src_key_name"]
        part_num = item["part_num"]
        start = item["start"]
        end = item["end"]
        total_size = item["total_size"]

        print "Goto: {0}[{1}] ({2} .. {3}/{4}%/{5}).".format(src_key_name, part_num, start, end, start*100/total_size, queue.qsize())
        while True:
            sleep(0.01*(2**wait-1)*random())
            try:
                mp.copy_part_from_key(src_bucket_name, src_key_name, part_num, start, end)
            except (boto.exception.S3CopyError, boto.exception.BotoServerError) as e:
                if wait < 8:
                    wait = wait + 1
                print "FAIL {7}: {0}[{1}] ({2} .. {3}/{4}%/{5}): {6}".format(src_key_name, part_num, start, end, start*100/total_size, queue.qsize(), e, wait)
                pass
            else:
                print "Done: {0}[{1}] ({2} .. {3}/{4}%/{5}).".format(src_key_name, part_num, start, end, start*100/total_size, queue.qsize())
                break
        queue.task_done()

def main():
    parser = ArgumentParser(description="Parallel S3 copy/move.")
    parser.add_argument("-s", "--source", help="Source bucket.", required=True)
    parser.add_argument("-d", "--destination", help="Destination bucket.", required=True)
    parser.add_argument("-l", "--key-list", help="File with key list to copy.", type=FileType('r'), required=True)
    parser.add_argument("-w", "--worker", type=int, default=16, help="The number of workers.")

    try:
        args = parser.parse_args()
    except IOError as e:
        print e
        return 1

    q = Queue(8*args.worker)

    for i in range(args.worker):
        t = Thread(target=worker, args=[q])
        t.daemon = True
        t.start()

    s3_conn = S3Connection()

    try:
        src_bucket = s3_conn.get_bucket(args.source)
        dst_bucket = s3_conn.get_bucket(args.destination)
    except boto.exception.S3ResponseError as e:
        print e
        return 1

    for key in args.key_list:
        key = key.strip()
        src_key = src_bucket.get_key(key)
        if dst_bucket.get_key(key):
            print "{0} already in {1}".format(key, dst_bucket.name)
            continue
        if src_key:
            mp = dst_bucket.initiate_multipart_upload(src_key.name)
            chunk_size = src_key.size / 8000
            if chunk_size < 5*2**20:
                chunk_size = 5*2**20
            if chunk_size > 10*2**20:
                chunk_size = 10*2**20
            for chunk in range(0, src_key.size - 1 - chunk_size, chunk_size):
                q.put(
                    {
                        "mp": mp,
                        "src_bucket_name": src_bucket.name,
                        "src_key_name": src_key.name,
                        "part_num": int(chunk/chunk_size)+1,
                        "start": chunk,
                        "end": chunk + chunk_size,
                        "total_size": src_key.size
                })
            q.put(
                {
                    "mp": mp,
                    "src_bucket_name": src_bucket.name,
                    "src_key_name": src_key.name,
                    "part_num": int(src_key.size/chunk_size)+1,
                    "start": chunk + chunk_size,
                    "end": src_key.size - 1,
                    "total_size": src_key.size
            })
            q.join()
            print "Completing: {0}".format(src_key.name)
            mp.complete_upload()
            continue
        else:
            print "Cannot find {0}".format(key)
            continue

if __name__ == "__main__":
    exit(main())
