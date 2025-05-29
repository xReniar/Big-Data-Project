#!/bin/bash

rm -rf metastore_db
schematool -dbType derby -initSchema

hive --f $ROOT_DIR/hive/$1.hql