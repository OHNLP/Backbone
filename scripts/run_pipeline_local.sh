#!/bin/bash

# Runs a backbone pipeline locally on a computer. Note that this is not recommended beyond for debugging/testing
# as it is not scalable and has additional local debugging.
# GCP or Spark options are recommended for production and/or large-scale use cases

BACKBONE_CONFIG=NAME_OF_CONFIG_TO_USE.json

BACKBONEDIR=$(cd `dirname $0` && pwd)
cd $BACKBONEDIR

BACKBONE_PACKAGED_FILE=bin/Backbone-Core-LocalDebug-Packaged.jar
if [ -f "$BACKBONE_PACKAGED_FILE" ]; then
    java -cp bin/Backbone-Core-LocalDebug-Packaged.jar org.ohnlp.backbone.core.BackboneRunner --config=$BACKBONE_CONFIG
else
    echo "Packaged backbone installation does not exist. Run package_modules_and_configs for your platform first!"
fi
