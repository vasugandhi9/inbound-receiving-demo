# Inbound Receiving Demo

Simulates warehouse dock receiving for an inbound fulfillment flow. A scanner
(`producer.py`) emits scan events onto a Kinesis stream; an idempotent Lambda
consumes them and updates on-hand inventory in DynamoDB, so the same physical
scan can be retried or redelivered any number of times without double-counting
it.

## Architecture

```
                                      batch size 10
                                      max retries 2
                                      bisect on error
                                      +-------------------+
producer.py  --PutRecord-->  Kinesis  | event source      |  --invoke-->  Lambda
(partition key =             stream   | mapping            |              (lambda_function.py)
 shipment_id)                         +-------------------+                   |
                                                |                              |
                                                | on-failure                   | TransactWriteItems
                                                v                              v
                                        SQS queue                     +------------------+
                                     (receiving-failures)             | DynamoDB          |
                                                                       |  processed-events |
                                                                       |  inventory        |
                                                                       +------------------+
```

- **producer.py** sends a JSON scan event per `PutRecord` call, using
  `shipment_id` as the partition key so all scans for one shipment land on
  (and stay ordered within) the same shard.
- The **event source mapping** polls the stream and invokes the Lambda in
  batches of up to 10 records, retries a failed batch up to 2 times, bisects
  the batch on repeated errors to isolate the bad record, and, once retries
  are exhausted, sends a failure pointer to an **SQS** queue instead of
  blocking the shard forever.
- **lambda_function.py** processes each record and writes to two DynamoDB
  tables in one transaction: `processed-events` (dedup ledger) and
  `inventory` (on-hand quantity).

## How idempotency works

Each record is applied via a single `TransactWriteItems` call with two
items:

1. **Put** into `processed-events` with `ConditionExpression:
   attribute_not_exists(event_id)` — fails if this `event_id` was already
   recorded.
2. **Update** on `inventory` with `ADD received_qty :q` — increments the
   quantity for that `shipment_id` + `sku`.

Because both items are in the same transaction, either **both** writes
commit or **neither** does — there's no window where inventory is
incremented but the event isn't marked processed (or vice versa). When the
condition check fails, the transaction is canceled with a
`TransactionCanceledException`; the Lambda inspects the cancellation reason,
and if it's `ConditionalCheckFailed` on the first item, it treats the record
as a duplicate and skips it rather than raising. Any other cancellation
reason (e.g. the inventory update itself failing) is re-raised and fails the
invocation.

## Producer modes

| Mode | What it sends | Simulates |
|---|---|---|
| `normal` | N distinct events, each a new `event_id` | Ordinary scanning traffic |
| `retry` | The same event (same `event_id`) sent N times | A scanner that resends after a timeout, not realizing the first attempt succeeded |
| `double-scan` | 2 events with different `event_id`s | A worker physically scanning the same unit twice by mistake |
| `crash` | 1 event flagged `simulate_crash: true` | A bug that makes the Lambda throw while processing a specific record |

## Test results

| Scenario | Input | Result |
|---|---|---|
| `normal` | 5x SKU-A, 3x SKU-B | SKU-A = 5, SKU-B = 3, 8 processed events |
| `retry` | Same `event_id` sent 5x | SKU-A 5 → 6, `SUMMARY processed=1 duplicates=4` |
| `double-scan` | 2 new `event_id`s for the same unit | SKU-A 6 → 8 — idempotency can't catch human duplicates; needs reconciliation against the ASN |
| `crash` | Poison pill (`simulate_crash: true`) | 3 invocations, then `RetryAttemptsExhausted`; SQS received a pointer (shard + sequence number, not the payload); record replayed via `get-records`; shard was not blocked — the next scan processed normally |

## IAM

The Lambda's execution role (`trust-policy.json`) trusts
`lambda.amazonaws.com` to assume it, and is scoped to least privilege:

- `dynamodb-policy.json` — `PutItem` / `UpdateItem` / `ConditionCheckItem`
  on exactly the two table ARNs (`processed-events`, `inventory`).
- `sqs-policy.json` — `SendMessage` on exactly the one failure-destination
  queue ARN (`receiving-failures`).
- The AWS-managed `AWSLambdaKinesisExecutionRole` policy, which grants the
  read/checkpoint permissions the event source mapping needs
  (`GetRecords`, `DescribeStream`, etc.).

## How to run

### Setup

```bash
python3 -m venv .venv
source .venv/bin/activate
pip install boto3
```

### AWS resources

```bash
# Kinesis stream
aws kinesis create-stream \
  --stream-name inbound-receiving-events \
  --shard-count 1 \
  --region ap-south-1

# DynamoDB tables
aws dynamodb create-table \
  --table-name processed-events \
  --attribute-definitions AttributeName=event_id,AttributeType=S \
  --key-schema AttributeName=event_id,KeyType=HASH \
  --billing-mode PAY_PER_REQUEST \
  --region ap-south-1

aws dynamodb create-table \
  --table-name inventory \
  --attribute-definitions \
      AttributeName=shipment_id,AttributeType=S \
      AttributeName=sku,AttributeType=S \
  --key-schema \
      AttributeName=shipment_id,KeyType=HASH \
      AttributeName=sku,KeyType=RANGE \
  --billing-mode PAY_PER_REQUEST \
  --region ap-south-1

# SQS failure destination
aws sqs create-queue --queue-name receiving-failures --region ap-south-1

# IAM role for the Lambda
aws iam create-role \
  --role-name inbound-receiving-lambda-role \
  --assume-role-policy-document file://trust-policy.json

aws iam put-role-policy \
  --role-name inbound-receiving-lambda-role \
  --policy-name dynamodb-access \
  --policy-document file://dynamodb-policy.json

aws iam put-role-policy \
  --role-name inbound-receiving-lambda-role \
  --policy-name sqs-access \
  --policy-document file://sqs-policy.json

aws iam attach-role-policy \
  --role-name inbound-receiving-lambda-role \
  --policy-arn arn:aws:iam::aws:policy/service-role/AWSLambdaKinesisExecutionRole

# Package and create the Lambda
zip function.zip lambda_function.py

aws lambda create-function \
  --function-name inbound-receiving-consumer \
  --runtime python3.9 \
  --handler lambda_function.lambda_handler \
  --role arn:aws:iam::<ACCOUNT_ID>:role/inbound-receiving-lambda-role \
  --zip-file fileb://function.zip \
  --region ap-south-1

# Event source mapping: Kinesis -> Lambda
aws lambda create-event-source-mapping \
  --function-name inbound-receiving-consumer \
  --event-source-arn arn:aws:kinesis:ap-south-1:<ACCOUNT_ID>:stream/inbound-receiving-events \
  --batch-size 10 \
  --maximum-retry-attempts 2 \
  --bisect-batch-on-function-error \
  --destination-config '{"OnFailure":{"Destination":"arn:aws:sqs:ap-south-1:<ACCOUNT_ID>:receiving-failures"}}' \
  --starting-position LATEST \
  --region ap-south-1
```

### Run the producer

```bash
python3 producer.py normal --shipment S1 --sku SKU-A --count 5
python3 producer.py retry --shipment S1 --sku SKU-A --times 5
python3 producer.py double-scan --shipment S1 --sku SKU-A
python3 producer.py crash --shipment S1 --sku SKU-A
```

## Cost and cleanup

Kinesis bills per shard-hour whether or not the shard is receiving traffic,
so an idle stream still costs money. Delete it (and the other demo
resources) when you're done:

```bash
aws kinesis delete-stream --stream-name inbound-receiving-events --region ap-south-1

aws lambda delete-event-source-mapping --uuid <mapping-uuid>
aws lambda delete-function --function-name inbound-receiving-consumer --region ap-south-1
aws dynamodb delete-table --table-name processed-events --region ap-south-1
aws dynamodb delete-table --table-name inventory --region ap-south-1
aws sqs delete-queue --queue-url <receiving-failures-queue-url>
aws iam detach-role-policy --role-name inbound-receiving-lambda-role --policy-arn arn:aws:iam::aws:policy/service-role/AWSLambdaKinesisExecutionRole
aws iam delete-role-policy --role-name inbound-receiving-lambda-role --policy-name dynamodb-access
aws iam delete-role-policy --role-name inbound-receiving-lambda-role --policy-name sqs-access
aws iam delete-role --role-name inbound-receiving-lambda-role
```

## Key concepts

- **Idempotency** — processing the same event more than once has the same
  effect as processing it once.
- **Conditional write** — a DynamoDB write that only succeeds if a condition
  on existing data holds (here, that the item doesn't already exist).
- **Transaction** — a group of writes that all succeed or all fail together;
  used here so the dedup record and the inventory update are never split.
- **Partition key** — the attribute Kinesis hashes to pick a shard (or
  DynamoDB uses to locate an item's partition); same key, same shard/partition.
- **Shard** — a unit of Kinesis throughput and ordering; records with the
  same partition key always land on the same shard, in order.
- **Bisect (on error)** — splitting a failing batch in half repeatedly to
  isolate the specific record causing errors, instead of retrying the whole
  batch.
- **Poison pill** — a record that fails processing every time it's
  retried, which would block a shard forever without a retry limit and
  failure destination.
- **Failure destination** — where a Kinesis event source mapping sends a
  pointer to a batch it gives up on, after exhausting retries.
- **Iterator age** — how far behind (in time) a stream consumer is from the
  latest record; a rising iterator age signals a stuck or slow consumer.
- **Hot shard** — a shard receiving disproportionate traffic because its
  partition key is too coarse (e.g. one giant shipment), causing throttling
  on that shard while others sit idle.
</content>
