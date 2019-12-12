#!/usr/bin/env bash

schema=$1
pwd=$2
list=$3

if [[ (-z $schema) || (-z $pwd) || (-z $list) ]]
then
    echo "At least one argument is missing: Should be deploy_metastore.sh schema password file_containing_list_of_sql_to_be_executed"
    echo "e.g. ./deploy_metastore.sh edf hadoop ddl.list"
    exit 1
fi

# run docker image
#docker run -d -p 49160:22 -p 49161:1521 -e ORACLE_ALLOW_REMOTE=true wnameless/oracle-xe-11g
#echo "Will wait for 60 seconds while the container startup"
#sleep 60
#echo "Deploying Oracle schema"

# create oracle schema
# --ALTER USER edf ACCOUNT UNLOCK;
sqlplus -s "system/oracle@(DESCRIPTION=(ADDRESS=(PROTOCOL=TCP)(Host=localhost)(Port=49161))(CONNECT_DATA=(SID=xe)))" <<EOF
CREATE TABLESPACE tbs_perm_01
  DATAFILE 'tbs_perm_01.dat'
    SIZE 10M
    AUTOEXTEND ON;

CREATE USER $schema
  IDENTIFIED BY $pwd
  DEFAULT TABLESPACE tbs_perm_01
  TEMPORARY TABLESPACE temp;

GRANT CREATE SESSION TO $schema;

GRANT CONNECT, RESOURCE, DBA TO $schema;

quit
EOF

for f in `cat ../data/metastore/$list`
do
    echo "will execute $f"
    echo exit | sqlplus -s "$schema/$pwd@(DESCRIPTION=(ADDRESS=(PROTOCOL=TCP)(Host=localhost)(Port=49161))(CONNECT_DATA=(SID=xe)))"  @../data/metastore/$f
done

echo "DL metastore deployment completed"

exit 0