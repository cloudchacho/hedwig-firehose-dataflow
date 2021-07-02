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

## Deploying

- Install `protoc`
- Install Hedwig custom options:
    ```shell
    git clone https://github.com/cloudchacho/hedwig.git /usr/local/lib/protobuf/include/hedwig
    ```
- Define your Hedwig schema in one or more files.
- Compile all the files in your schema into a fileDescriptorSet file:
    ```shell
    protoc --descriptor_set_out=schema-v1 -I /usr/local/lib/protobuf/include/ hedwig/protobuf/options.proto google/protobuf/descriptor.proto <SCHEMA FILES...>
    ```
  If your schema uses any other dependencies, make sure to compile them in as well (e.g. for timestamp, add `google/protobuf/timestamp.proto` to the command).
- Create a pom file:
    ```xml
    <?xml version="1.0" encoding="UTF-8"?>
    <project xmlns="http://maven.apache.org/POM/4.0.0" xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance" xsi:schemaLocation="http://maven.apache.org/POM/4.0.0 http://maven.apache.org/xsd/maven-4.0.0.xsd">
      <modelVersion>4.0.0</modelVersion>

    <groupId>com.example</groupId>
    <artifactId>hedwig-firehose-dataflow</artifactId>
    <version>0.1</version>

      <dependencies>
        <dependency>
          <groupId>io.github.cloudchacho</groupId>
          <artifactId>hedwig-firehose-dataflow</artifactId>
          <version>0.4</version>
        </dependency>
      </dependencies>

      <build>
        <plugins>
          <plugin>
            <groupId>org.codehaus.mojo</groupId>
            <artifactId>exec-maven-plugin</artifactId>
            <version>3.0.0</version>
            <configuration>
              <includeProjectDependencies>true</includeProjectDependencies>
              <mainClass>io.github.cloudchacho.hedwig.Firehose</mainClass>
            </configuration>
          </plugin>
        </plugins>
      </build>
    </project>
    ```
- Deploy your template using the following command, adjusting variables as necessary:
    ```shell
    DATAFLOW_BUCKET=<...>
    FIREHOSE_BUCKET=<...>
    REGION="us-central1"

    version=$(mvn -q -Dexec.executable=echo -Dexec.args='${project.version}' --non-recursive exec:exec)    firehose_location="gs://${FIREHOSE_BUCKET}/firehose"
    dataflow_bucket="gs://${DATAFLOW_BUCKET}"
    dataflow_template="${dataflow_bucket}/templates/hedwig-firehose-v${version}"
    dataflow_temp="${dataflow_bucket}/temp"
    dataflow_staging="${dataflow_bucket}/stage"
    schema_file="${dataflow_bucket}/schemas/schema-v1"
    args="\
    --runner=DataflowRunner \
    --project=${GCP_PROJECT} \
    --stagingLocation=${dataflow_staging} \
    --templateLocation=${dataflow_template} \
    --region=${REGION} \
    --tempLocation=${dataflow_temp} \
    --userTempLocation=${dataflow_bucket}/tmp/ \
    --outputDirectory=${firehose_location} \
    --inputSubscriptions=<...> \
    --inputSubscriptionsCrossProject=<...> \
    --schemaFileDescriptorSetFile=${schema_file}"

    mvn compile exec:java -Dexec.args="$args"
    ```

    To enable debug logging, add:
    ```shell
    --workerLogLevelOverrides='{\"io.cloudchacho.hedwig.Firehose\":\"DEBUG\"}'
    ```
    to `args`.
