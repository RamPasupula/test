#!/usr/bin/env bash

echo "WARNING: Make sure to have modified HADOOP_HOME/libexec/hadoop-config.sh after installation (http://stackoverflow.com/questions/33968422/bin-bash-bin-java-no-such-file-or-directory)"

echo $HADOOP_HOME

echo "Cleaning namenode"
$HADOOP_HOME/bin/hdfs namenode -format

echo "Start NameNode daemon and DataNode daemon"
$HADOOP_HOME/sbin/start-dfs.sh

echo "Create HDFS home directory"
$HADOOP_HOME/bin/hdfs dfs -mkdir -p /user/`whoami`

echo "Start YARN"
$HADOOP_HOME/sbin/start-yarn.sh

echo "Testing..."
$HADOOP_HOME/bin/hdfs dfs -put etc/hadoop input
$HADOOP_HOME/bin/hadoop jar share/hadoop/mapreduce/hadoop-mapreduce-examples-2.6.0-cdh5.8.0.jar grep input output 'dfs[a-z.]+'

echo "Initiating HIVE"
$HADOOP_HOME/bin/hdfs dfs -mkdir -p /user/hive/warehouse
$HADOOP_HOME/bin/hdfs dfs -chmod g+w /user/hive/warehouse
$HADOOP_HOME/bin/hdfs dfs -mkdir /tmp
$HADOOP_HOME/bin/hdfs dfs -chmod g+w /tmp

cd $HIVE_HOME
$HIVE_HOME/hcatalog/sbin/hcat_server.sh start
$HIVE_HOME/hcatalog/sbin/webhcat_server.sh start
$HIVE_HOME/bin/hiveserver2 &
nohup $HIVE_HOME/bin/hiveserver2 >$HIVE_HOME/logs/hiveserver2.out 2> $HIVE_HOME/logs/hiveserver2.log &
echo "Connect with 'beeline -u jdbc:hive2://localhost:10000/default -n cb186046'"

$HIVE_HOME/bin/hiveserver2 -hiveconf hive.root.logger=INFO,console

kinit -kt /Users/cb186046/workspace/docker-kdc/krb5.keytab cb186046@KDC.LOCAL
beeline -u "jdbc:hive2://localhost:10000/default;principal=hive/kdc.local@KDC.LOCAL"
