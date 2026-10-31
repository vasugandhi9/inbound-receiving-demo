"""
AWS Lambda consumer for a Kinesis stream of inbound shipment events.

For every record in the batch, this function:
  1. Decodes and parses the event payload.
  2. Writes the event to DynamoDB using a transaction that does two things
     atomically: records the event as "processed" (so we never double count
     it) and adds the received quantity to the inventory record.
  3. If the event was already processed before (the "processed-events" put
     fails its condition check), it is skipped instead of being applied
     twice. This makes the function safe to retry after a Lambda error,
     since Kinesis will redeliver records on failure.

Table shapes expected in DynamoDB:
  - "processed-events": partition key "event_id" (string)
  - "inventory": partition key "shipment_id" (string), sort key "sku" (string),
    plus a numeric "received_qty" attribute that gets incremented.
"""

import base64
import json
import logging

import boto3

logger = logging.getLogger()
logger.setLevel(logging.INFO)

# Use the DynamoDB resource's underlying client. This is still a low-level
# client (so it has transact_write_items), but it auto-converts plain Python
# types (str, int, ...) to/from DynamoDB's AttributeValue format for us, so
# we don't have to hand-write things like {"S": "abc"} or {"N": "3"}.
dynamodb = boto3.resource("dynamodb").meta.client

PROCESSED_EVENTS_TABLE = "processed-events"
INVENTORY_TABLE = "inventory"


def lambda_handler(event, context):
    """Entry point Lambda calls with a batch of Kinesis records."""
    processed_count = 0
    duplicate_count = 0

    for record in event["Records"]:
        payload = decode_record(record)

        # Lets us test how Lambda/Kinesis handle a failure mid-batch:
        # the record will be retried since we never finished processing it.
        if payload.get("simulate_crash"):
            raise Exception(f"Simulated crash for event {payload.get('event_id')}")

        event_id = payload["event_id"]
        shipment_id = payload["shipment_id"]
        sku = payload["sku"]
        qty = payload["qty"]

        was_duplicate = write_event_and_update_inventory(
            event_id, shipment_id, sku, qty
        )

        if was_duplicate:
            duplicate_count += 1
            logger.info("DUPLICATE skipped %s", event_id)
        else:
            processed_count += 1
            logger.info(
                "PROCESSED %s %s %s +%s", event_id, shipment_id, sku, qty
            )

    logger.info(
        "SUMMARY processed=%d duplicates=%d", processed_count, duplicate_count
    )


def decode_record(record):
    """Base64-decode a Kinesis record's data and parse it as JSON."""
    raw_bytes = base64.b64decode(record["kinesis"]["data"])
    return json.loads(raw_bytes)


def write_event_and_update_inventory(event_id, shipment_id, sku, qty):
    """
    Atomically mark an event as processed and add its quantity to inventory.

    Both writes happen in a single DynamoDB transaction, so either both
    succeed or neither does. If this event_id was already processed, the
    condition check on the first write fails, the whole transaction is
    canceled, and nothing is double-applied.

    Returns True if the event was a duplicate (already processed before),
    False if it was newly processed.
    """
    try:
        dynamodb.transact_write_items(
            TransactItems=[
                {
                    "Put": {
                        "TableName": PROCESSED_EVENTS_TABLE,
                        "Item": {"event_id": event_id},
                        # Fails the transaction if this event_id already exists.
                        "ConditionExpression": "attribute_not_exists(event_id)",
                    }
                },
                {
                    "Update": {
                        "TableName": INVENTORY_TABLE,
                        "Key": {"shipment_id": shipment_id, "sku": sku},
                        "UpdateExpression": "ADD received_qty :q",
                        "ExpressionAttributeValues": {":q": qty},
                    }
                },
            ]
        )
        return False

    except dynamodb.exceptions.TransactionCanceledException as error:
        # CancellationReasons lines up 1:1 with the TransactItems above, so
        # index 0 is the reason the "processed-events" Put did or didn't fail.
        reasons = error.response.get("CancellationReasons", [])
        first_reason_code = reasons[0].get("Code") if reasons else None

        if first_reason_code == "ConditionalCheckFailed":
            # We already processed this event_id before. Treat as a
            # duplicate rather than an error.
            return True

        # Anything else (e.g. the inventory update itself failed) is a real
        # problem, so let it propagate and fail the Lambda invocation.
        raise
