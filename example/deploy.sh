#!/usr/bin/env bash

set -eu

version="0.1"
firehose_location="gs://${FIREHOSE_BUCKET}/firehose"
dataflow_bucket="gs://${DATAFLOW_BUCKET}"
dataflow_template="${dataflow_bucket}/templates/hedwig-firehose-v${version}"
dataflow_temp="${dataflow_bucket}/temp"
dataflow_staging="${dataflow_bucket}/stage"
region="us-central1"
args="\
--runner=DataflowRunner \
--project=${GCP_PROJECT} \
--stagingLocation=${dataflow_staging} \
--templateLocation=${dataflow_template} \
--region=${region} \
--tempLocation=${dataflow_temp} \
--userTempLocation=${dataflow_bucket}/tmp/ \
--outputDirectory=${firehose_location} \
--workerLogLevelOverrides='{\"io.cloudchacho.hedwig.Firehose\":\"DEBUG\"}' \
--inputSubscriptions=hedwig-firehose-dev-user-created-v1 \
--inputSubscriptionsCrossProject=hedwig-firehose-other-project-dev-user-created-v1;other-project"

mvn compile exec:java -Dexec.mainClass=com.example.Firehose -Dexec.args="$args"