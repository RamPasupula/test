#!/usr/bin/env bash


# <COMMAND> <SQL_FILE_PATH> <SRC_SYSTEM>
java -Djava.io.tmpdir=/tmp -Dlog4j.configuration=file:///Users/cs186076/Documents/work/edf_dds_merge/conf/log4j.xml
-Dframework-conf.properties=file:///Users/cs186076/Documents/work/edf_dds_merge/conf/framework-conf-cb.properties
-Ddatabase.properties=file:///Users/cs186076/Documents/work/edf_dds_merge/conf/database-cb.properties
-cp edf.jar com.uob.edag.process.PIISpecProcessor "$@"