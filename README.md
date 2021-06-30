# Hedwig Firehose Dataflow

This is a dataflow template that can be used for Hedwig Firehose. It reads input messages from your subscriptions and
writes them into a GCS file. The messages are decoded using protobuf format on a best-effort basis. If a message can't
be decoded, its still written to the file as a base64 encoded string.

**Assumptions:**
- Protobuf validator using message transport attributes.

Output files are compressed using gzip by default.

Output structure in GCS:
- Top level bucket / directory: `gs://$BUCKET/$DIR` (supplied as input parameter `outputDirectory`).
- Directory for current project, or the other project for cross-project subscriptions: `gs://$BUCKET/$DIR/$PROJECT/ ...`
- Directory for every message type and major version: `gs://$BUCKET/$DIR/$PROJECT/user-created-v1/ ...`
- Directory for year: `gs://$BUCKET/$DIR/$PROJECT/user-created-v1/2020/ ...`
- Directory for month: `gs://$BUCKET/$DIR/$PROJECT/user-created-v1/2020/08/ ...`
- Directory for day: `gs://$BUCKET/$DIR/$PROJECT/user-created-v1/2020/08/25/ ...`
- Files bucketed in multiple shards in x-min windows: `gs://$BUCKET/$DIR/$PROJECT/user-created-v1/2020/08/25/user-created-v1-21:04:00-21:06:00-0-of-1.gz ...` (window supplied as input parameter `windowDuration`).

Code is based on [this blog post](https://medium.com/@robinriclet/streaming-multiple-pubsub-subscriptions-to-gcs-with-fixed-windows-and-dynamic-naming-7f70cdd4584).

## Reading files with unknown protobuf messages

```shell script
OUTPUT_FILE=<path to firehosed file (must start with gs://)>
SCHEMA_FILE=<path to your schema .proto file>
CONTAINER_SCHEMA_FILE=<path to container schema .proto file>

gsutil cat gs://${OUTPUT_FILE} | \
  gzip -d | \
  while IFS= read -r line; do \
    echo -n $line | \
    base64 -d | \
    protoc --decode PayloadV1 --proto_path=/usr/local/lib/protobuf/include --proto_path=$(dirname ${CONTAINER_SCHEMA_FILE}) ${CONTAINER_SCHEMA_FILE} | \
    python -c 'import sys, pathlib; sys.stdout.buffer.write(bytes(str(next(line[6:].strip(b"\"") for line in sys.stdin.buffer.read().split(b"\n") if line.startswith(b"data:")), "unicode_escape"), "raw_unicode_escape"))' | \
    protoc --decode hedwig.VisitCreatedV1 --proto_path=/usr/local/lib/protobuf/include --proto_path=$(dirname ${SCHEMA_FILE}) ${SCHEMA_FILE}; \
  done;
```
