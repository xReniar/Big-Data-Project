#!/bin/bash

# stop hadoop if already running
$HADOOP_HOME/sbin/stop-dfs.sh

# clean tmp folder
rm -rf /tmp/*
$HADOOP_HOME/bin/hdfs namenode -format

# start hadoop
$HADOOP_HOME/sbin/start-dfs.sh