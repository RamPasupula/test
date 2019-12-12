#!/usr/bin/env bash

# <COMMAND>(register) <FILE_PATH>(INTERFACE_SPEC) <TRUNCATE_INSERT>(true/false) <COLUMN_INDEXES>(0,1,2,4,7,14)
# EXCEl_PATH true 0,1,2,4,7,14
java -Djava.io.tmpdir=/tmp -Dlog4j.configuration=file://Users/cs186076/Documents/work/edf_dds_merge/conf/log4j.xml
-Dframework-conf.properties=file:///Users/cs186076/Documents/work/edf_dds_merge/conf/framework-conf-cb.properties
-Ddatabase.properties=file:///Users/cs186076/Documents/work/edf_dds_merge/conf/database-cb.properties
-cp edf.jar com.uob.edag.process.PIISpecProcessor register "$@"