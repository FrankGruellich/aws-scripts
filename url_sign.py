#!/usr/bin/python
"""Generates signed URLs for S3."""

from boto.s3.connection import S3Connection
from argparse import ArgumentParser


def main():
    """The main function"""
    parser = ArgumentParser()
    parser.add_argument('-b', '--bucket', type=str, required=True)
    parser.add_argument('-e', '--expire', type=int, default=60)
    parser.add_argument(
        '-s', '--secure', default=True, action='store_false')
    parser.add_argument('keys', nargs='+', type=str)

    args = parser.parse_args()

    s3_conn = S3Connection()

    for k in args.keys:
        print s3_conn.generate_url(
            args.expire, 'GET', args.bucket,
            k, force_http=args.secure)
    return

if __name__ == "__main__":
    main()
