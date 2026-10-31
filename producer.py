#!/usr/bin/env python3
"""
Warehouse dock scanner simulator.

Sends JSON "scan" events to the Kinesis stream "inbound-receiving-events"
so you can test how downstream consumers handle normal traffic, retries,
double-scans, and crashes.

Each event looks like:
    {"event_id": "<uuid4>", "shipment_id": "S1", "sku": "SKU-A", "qty": 1}

Usage examples:
    python3 producer.py normal --shipment S1 --sku SKU-A --count 5
    python3 producer.py retry --shipment S1 --sku SKU-A --times 5
    python3 producer.py double-scan --shipment S1 --sku SKU-A
    python3 producer.py crash --shipment S1 --sku SKU-A
"""

import argparse
import json
import sys
import uuid

import boto3
from botocore.exceptions import ClientError

STREAM_NAME = "inbound-receiving-events"
REGION = "ap-south-1"


def build_event(shipment_id, sku, event_id=None, simulate_crash=False):
    """Build one scan event dict. Pass event_id to reuse an existing id
    (used by 'retry' mode to simulate the same scan being resent)."""
    event = {
        "event_id": event_id or str(uuid.uuid4()),
        "shipment_id": shipment_id,
        "sku": sku,
        "qty": 1,
    }
    if simulate_crash:
        event["simulate_crash"] = True
    return event


def send_event(client, event):
    """Put one event on the stream and print the Kinesis response details."""
    # PartitionKey determines which shard the record lands on. Using
    # shipment_id means all events for the same shipment land on the same
    # shard, which keeps them in order relative to each other.
    response = client.put_record(
        StreamName=STREAM_NAME,
        Data=json.dumps(event).encode("utf-8"),
        PartitionKey=event["shipment_id"],
    )
    print(
        f"event_id={event['event_id']} "
        f"ShardId={response['ShardId']} "
        f"SequenceNumber={response['SequenceNumber']}"
    )


def run_normal(client, shipment_id, sku, count):
    """Send 'count' events, each a distinct scan with its own new uuid."""
    for _ in range(count):
        send_event(client, build_event(shipment_id, sku))


def run_retry(client, shipment_id, sku, times):
    """Send the exact same event (same event_id) 'times' times, simulating
    a scanner that retries after a network timeout without realizing the
    first attempt actually succeeded."""
    event = build_event(shipment_id, sku)
    for _ in range(times):
        send_event(client, event)


def run_double_scan(client, shipment_id, sku):
    """Send 2 events with different uuids, simulating a worker who scans
    the same physical unit twice by mistake."""
    for _ in range(2):
        send_event(client, build_event(shipment_id, sku))


def run_crash(client, shipment_id, sku):
    """Send 1 event flagged with simulate_crash: true, so a consumer can be
    tested for how it behaves when processing that event fails."""
    send_event(client, build_event(shipment_id, sku, simulate_crash=True))


def main():
    parser = argparse.ArgumentParser(
        description="Simulate a warehouse dock scanner sending scan events to Kinesis."
    )
    subparsers = parser.add_subparsers(dest="mode", required=True)

    normal_parser = subparsers.add_parser(
        "normal", help="Send N distinct events, each with a new uuid."
    )
    normal_parser.add_argument("--shipment", required=True, help="Shipment id, e.g. S1")
    normal_parser.add_argument("--sku", required=True, help="SKU being scanned, e.g. SKU-A")
    normal_parser.add_argument("--count", type=int, required=True, help="Number of events to send")

    retry_parser = subparsers.add_parser(
        "retry", help="Send the same event (same event_id) N times."
    )
    retry_parser.add_argument("--shipment", required=True, help="Shipment id, e.g. S1")
    retry_parser.add_argument("--sku", required=True, help="SKU being scanned, e.g. SKU-A")
    retry_parser.add_argument("--times", type=int, required=True, help="Number of times to resend")

    double_scan_parser = subparsers.add_parser(
        "double-scan", help="Send 2 events with different uuids for the same unit."
    )
    double_scan_parser.add_argument("--shipment", required=True, help="Shipment id, e.g. S1")
    double_scan_parser.add_argument("--sku", required=True, help="SKU being scanned, e.g. SKU-A")

    crash_parser = subparsers.add_parser(
        "crash", help="Send 1 event flagged with simulate_crash: true."
    )
    crash_parser.add_argument("--shipment", required=True, help="Shipment id, e.g. S1")
    crash_parser.add_argument("--sku", required=True, help="SKU being scanned, e.g. SKU-A")

    args = parser.parse_args()

    client = boto3.client("kinesis", region_name=REGION)

    try:
        if args.mode == "normal":
            run_normal(client, args.shipment, args.sku, args.count)
        elif args.mode == "retry":
            run_retry(client, args.shipment, args.sku, args.times)
        elif args.mode == "double-scan":
            run_double_scan(client, args.shipment, args.sku)
        elif args.mode == "crash":
            run_crash(client, args.shipment, args.sku)
        return 0
    except ClientError as e:
        print(f"Error: {e}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    sys.exit(main())
