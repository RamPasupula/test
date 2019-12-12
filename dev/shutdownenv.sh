#!/usr/bin/env bash

cd $HADOOP_HOME

./sbin/stop-yarn.sh
./sbin/stop-dfs.sh

kill $(ps aux | grep 'HiveServer2' | awk '{print $2}')
$HIVE_HOME/hcatalog/sbin/hcat_server.sh stop
$HIVE_HOME/hcatalog/sbin/webhcat_server.sh stop