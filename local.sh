#!/bin/bash

export PROJECT_PATH=$(pwd)

$HADOOP_HOME/sbin/stop-dfs.sh
rm -rf /tmp/*
$HADOOP_HOME/bin/hdfs namenode -format
$HADOOP_HOME/sbin/start-dfs.sh