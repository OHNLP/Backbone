#!/bin/bash

BACKBONEDIR=$(cd `dirname $0` && pwd)
cd $BACKBONEDIR

java -cp bin/Plugin-Manager.jar org.ohnlp.backbone.pluginmanager.PluginManager