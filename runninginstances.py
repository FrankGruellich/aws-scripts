#!/usr/bin/python
"""Pulls instances from an AWS account."""

import boto.ec2
import argparse
from prettytable import PrettyTable
from threading import Thread


def get_region_instances(region, tables, auth):
    """Collects all instances in given region, using given authentication and
    passes it back in given table."""
    table = PrettyTable([
        "Name", "Key-Name", "Type", "Placement", "Public-DNS",
        "Private-IP", "Instance-ID", "State", "Launch Time"
    ])
    table.padding_width = 1
    ec2 = boto.ec2.connect_to_region(
        region.name, aws_access_key_id=auth.aws_access_key_id,
        aws_secret_access_key=auth.aws_secret_access_key)
    if ec2:
        reservations = ec2.get_all_instances()
        if reservations:
            for reservation in reservations:
                for i in reservation.instances:
                    try:
                        instance_name = i.tags['Name']
                    except KeyError:
                        instance_name = "N/A"
                    table.add_row([
                        instance_name,
                        i.key_name,
                        i.instance_type,
                        i.placement,
                        i.public_dns_name,
                        i.private_ip_address,
                        i.id,
                        i.state,
                        i.launch_time
                        ])
            tables[region.name] = table
    return


def main():
    """The main function."""
    parser = argparse.ArgumentParser(
        description="Show available ec2 instances in AWS account."
    )
    parser.add_argument(
        "-p",
        "--profile",
        required=False,
        default=None,
        help="The profile to use from .boto, "
        "if not provided the default credentials will be used."
    )
    args = parser.parse_args()

    conn = boto.ec2.connection.EC2Connection()
    regions = conn.get_all_regions()
    auth = boto.connection.AWSAuthConnection(
        "ec2.eu-west-1.amazonaws.com",
        profile_name=args.profile
    )

    threads = {}
    tables = {}

    for region in regions:
        threads[region.name] = Thread(
            target=get_region_instances,
            args=[region, tables, auth]
        )
        threads[region.name].daemon = True
        threads[region.name].start()

    for region in regions:
        threads[region.name].join()

    for region in sorted(tables.keys()):
        print region
        print tables[region]

if __name__ == "__main__":
    main()
