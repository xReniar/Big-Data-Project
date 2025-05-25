#!/bin/bash

export ROOT_DIR=$(pwd)

$HADOOP_HOME/sbin/stop-dfs.sh
rm -rf /tmp/*
$HADOOP_HOME/bin/hdfs namenode -format
$HADOOP_HOME/sbin/start-dfs.sh