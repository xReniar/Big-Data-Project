#!/bin/bash

$SPARK_HOME/bin/spark-submit \
    --master local \
    "$1"