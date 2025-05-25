#!/bin/bash

if [ -z "$ROOT_DIR" ]; then
    export ROOT_DIR=$(pwd)
fi

# start hadoop
$HADOOP_HOME/sbin/stop-dfs.sh
rm -rf /tmp/*
$HADOOP_HOME/bin/hdfs namenode -format
$HADOOP_HOME/sbin/start-dfs.sh