#!/usr/bin/env bash

set -eu

version=$(mvn -q -Dexec.executable=echo -Dexec.args='${project.version}' --non-recursive exec:exec)
firehose_location="gs://${FIREHOSE_BUCKET}/firehose"
dataflow_bucket="gs://${DATAFLOW_BUCKET}"
dataflow_template="${dataflow_bucket}/templates/hedwig-firehose-v${version}"
dataflow_temp="${dataflow_bucket}/temp"
dataflow_staging="${dataflow_bucket}/stage"
region="us-central1"
schema_file="${dataflow_bucket}/schemas/schema-v1"
args="\
--runner=DataflowRunner \
--project=${GCP_PROJECT} \
--stagingLocation=${dataflow_staging} \
--templateLocation=${dataflow_template} \
--region=${region} \
--tempLocation=${dataflow_temp} \
--userTempLocation=${dataflow_bucket}/tmp/ \
--outputDirectory=${firehose_location} \
--workerLogLevelOverrides='{\"io.github.cloudchacho.hedwig.Firehose\":\"DEBUG\"}' \
--inputSubscriptions=hedwig-firehose-dev-user-created-v1 \
--inputSubscriptionsCrossProject=hedwig-firehose-other-project-dev-user-created-v1;other-project \
--schemaFileDescriptorSetFile=${schema_file}"

mvn compile exec:java -Dexec.args="$args"
