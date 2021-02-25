#!/bin/bash

# Runs the specified backbone pipeline on Google Cloud Platform using Dataflow
# For a full description of options available here, please consult https://beam.apache.org/documentation/runners/dataflow/

BACKBONE_CONFIG=NAME_OF_CONFIG_TO_USE.json
GCP_PROJECT_ID=YOUR_GCP_PROJECT_ID
GCP_REGION=YOUR_GOOGLE_CLOUD_REGION

BACKBONEDIR=$(cd `dirname $0` && pwd)
cd $BACKBONEDIR

BACKBONE_PACKAGED_FILE=bin/Backbone-Core-GCP-Packaged.jar
if [ -f "$BACKBONE_PACKAGED_FILE" ]; then
    java -jar bin\Backbone-Core-GCP-Packaged.jar \
    --config=$BACKBONE_CONFIG \
    --runner=DataflowRunner \
    --project=$GCP_PROJECT_ID \
    --region=$GCP_REGION
else
    echo "Packaged backbone installation does not exist. Run package_modules_and_configs for your platform first!"
fi
