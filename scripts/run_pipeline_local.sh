#!/bin/bash

# Runs a backbone pipeline locally on a computer using an embedded flink cluster
# GCP or Spark options are recommended for production and/or large-scale use cases

BACKBONE_CONFIG=NAME_OF_CONFIG_TO_USE.json

BACKBONEDIR=$(cd `dirname $0` && pwd)
cd $BACKBONEDIR

FLINK_DIR=flink-1.14.3/bin
FLINK_EXECUTABLE=$FLINK_DIR/flink
if [ -f "$FLINK_EXECUTABLE" ]; then
    echo "Embedded Flink Cluster Already Setup - Skipping New Install"
else
    echo "Downloading Apache Flink for Local Run -"
    wget https://dlcdn.apache.org/flink/flink-1.14.3/flink-1.14.3-bin-scala_2.11.tgz -O flink.tgz
    tar -xf flink.tgz
    echo "***Important***: Please adjust default flink settings located at flink-1.14.3/conf/flink-conf.yaml to match your hardware"
  	echo "Particularly taskmanager.numberOfTaskSlots (generally number of cores * .8 rounded down), "
    echo "parallelism.default (set equal to number of task slots), "
  	echo "and taskmanager.memory.process.size (good start is 2GB * number of task slots)"
    read -p "When done, press [ENTER] to continue"
fi

BACKBONE_PACKAGED_FILE=bin/Backbone-Core-Flink-Packaged.jar
if [ -f "$BACKBONE_PACKAGED_FILE" ]; then
    echo "Starting Embedded Flink Cluster..."
    $FLINK_DIR/start-cluster.sh
    echo "Flink Cluster Started - Job Progress Can be Seen via Configured WebUI Port (Default: localhost:8081)"
    echo "Submitting Job..."
    $FLINK_DIR/flink run -c org.ohnlp.backbone.core.BackboneRunner bin/Backbone-Core-Flink-Packaged.jar --runner=FlinkRUnner --config=$BACKBONE_CONFIG
    echo "Job Complete, Shutting Down Embedded Flink Cluster"
    $FLINK_DIR/stop-cluster.sh
else
    echo "Packaged backbone installation does not exist. Run package_modules_and_configs for your platform first!"
fi
