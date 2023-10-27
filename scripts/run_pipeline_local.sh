#!/bin/bash

# Runs a backbone pipeline locally on a computer using an embedded flink cluster
# GCP or Spark options are recommended for production and/or large-scale use cases


BACKBONEDIR=$(cd `dirname $0` && pwd)
cd $BACKBONEDIR


OLD_FLINK_DIRS=(flink-1.17.1 flink-1.13.6)
UPGRADE=false
UPGRADE_PATH=NONE
for olddir in $OLD_FLINK_DIRS; do
  if [ -f "$olddir/conf/flink-conf.yaml" ]; then
    UPGRADE=true
    UPGRADE_PATH=$olddir/conf/flink-conf.yaml
    break
  fi
done

FLINK_DIR=flink-1.18.0/bin
FLINK_EXECUTABLE=$FLINK_DIR/flink
if [ -f "$FLINK_EXECUTABLE" ]; then
    echo "Embedded Flink Cluster Already Setup - Skipping New Install"
else
    if $UPGRADE; then
      echo "Flink Update Detected, Upgrading to v1.18.0 from " $UPGRADE_PATH
    else
      echo "Downloading Apache Flink for Local Run"
    fi
    wget https://archive.apache.org/dist/flink/flink-1.18.0/flink-1.18.0-bin-scala_2.12.tgz -O flink.tgz
    tar -xf flink.tgz
    if $UPGRADE; then
      cp $UPGRADE_PATH $FLINK_DIR/../conf/flink-conf.yaml
    else
      echo "***Important***: Please adjust default flink settings located at flink-1.13.6/conf/flink-conf.yaml to match your hardware"
      echo "Particularly taskmanager.numberOfTaskSlots (generally number of cores available for use, good starting point is CPU * .8 rounded down), "
      echo "parallelism.default (set equal to number of task slots), "
      echo "and taskmanager.memory.process.size (good start is 2GB * number of task slots)"
      read -p "When done, press [ENTER] to continue"
fi

echo "Repackaging Backbone with Current Configs, Modules, and Resources"
java -cp bin/Plugin-Manager.jar org.ohnlp.backbone.pluginmanager.PluginManager Flink

if [ $# -eq 0 ]; then
    cd configs
    echo "No configuration parameter supplied, scanning for Available Configurations..."
    options=( $(find -mindepth 1 -maxdepth 1 -print0 -type f | xargs -0) )
    select opt in "${options[@]}" "Quit" ; do
        if (( REPLY == 1 + ${#options[@]} )) ; then
            exit

        elif (( REPLY > 0 && REPLY <= ${#options[@]} )) ; then
            BACKBONE_CONFIG=${opt#\./}
            break

        else
            echo "Invalid option. Try another one."
        fi
    done
    cd ..
else
    BACKBONE_CONFIG=$1
fi

echo "Running Job with Configuration $BACKBONE_CONFIG"


BACKBONE_PACKAGED_FILE=bin/Backbone-Core-Flink-Packaged.jar
if [ -f "$BACKBONE_PACKAGED_FILE" ]; then
    echo "Starting Embedded Flink Cluster..."
    $FLINK_DIR/start-cluster.sh
    echo "Flink Cluster Started - Job Progress Can be Seen via Configured WebUI Port (Default: localhost:8081)"
    echo "Submitting Job..."
    $FLINK_DIR/flink run -c org.ohnlp.backbone.core.BackboneRunner bin/Backbone-Core-Flink-Packaged.jar --runner=FlinkRunner --config=$BACKBONE_CONFIG
    read -p "Job Complete, Press [ENTER] to Shut Down Embedded Flink Cluster"
    $FLINK_DIR/stop-cluster.sh
else
    echo "Packaged backbone installation does not exist. Run package_modules_and_configs for your platform first!"
fi
