#!/bin/sh
#|==============================================================================
#|   purge_StagingT1.sh
#|==============================================================================
#|
#|   Project:          UOB EDAG
#|   Author:           Santhi Animireddy
#|   Initial Date:     24 July 2017
#|
#|==============================================================================
#|
#|   Description :     Script to purge the files in staging area 
#|
#|==============================================================================
#|
#|   Modification History:
#|   ====================
#|
#|   Date      Who  Description
#|   --------  ---  ------------------------------------------------------------
#|   24/07/17  Santhi  Created
#|==============================================================================
#|   Set Variable
#|==============================================================================
#set -x
# Determine path
BASE_NM=`basename $0`
BASE_PATH=`dirname $0`
echo $BASE_PATH

if [ "$BASE_PATH" = "." ]; then
        BASE_PATH=`pwd`
fi

export ENVNM=`echo $BASE_PATH | rev | awk -F "/" '{print $4"/"$5"/"$6"/"$7"/"$8}'| rev | sed 's/\/\///g'`
export ENVVR=`echo $BASE_PATH | rev | awk -F "/" '{print $3}'| rev`
export METADBCONN1=`grep METADBCONN /$ENVNM/$ENVVR/edf/config/setenv.sh | head -1 | awk -F "=" '{print $2}' | sed 's/"//g'`
export FUNCDIR="/$ENVNM/$ENVVR/edf/scripts"
export TEMP_DIR=`grep LOGHOME /$ENVNM/$ENVVR/edf/config/setenv.sh |  head -1 | awk -F "=" '{print $2}' | awk -F "/" '{print "/"$2}'`
export vdate=`date +%Y%m%d_%H%M%S%N`
export LOGDIR="${TEMP_DIR}/$ENVVR/logs/edf"
export LOGFILE="${LOGDIR}/`basename $0`_${vdate}.log"
export TMPDIR="${TEMP_DIR}/$ENVVR/tmp"
export SCHEMA=`grep SchemaNm /$ENVNM/$ENVVR/edf/config/setenv.sh | head -1 | awk -F "=" '{print $2}' | sed 's/"//g'`
export HIVEDBCONN=`grep Hive.JDBC.ConnectionURL /$ENVNM/$ENVVR/edf/config/database.properties | awk -F "=" '{print $2"="$3}'`

if [ -e $FUNCDIR/functions.sh -a -s $FUNCDIR/functions.sh ]
then
        . $FUNCDIR/functions.sh
else
        echo " Functions does not exist.Exiting" >> $LOGFILE
        exit 1
fi

# Input parameter validation
if [ $# -ne 2 ]
then
      echo "USAGE: purge_StagingT1.sh ctry_cd source_system_code" >> $LOGFILE
      echo "[ERROR]:Two input parameters required for staging purge " >> $LOGFILE
      exit 1
fi

logger "==============================================================================" 
logger "                      Script Name: ${BASE_NM}                                 " 
logger "=============================================================================="

logger "-----------------------------------------------------------------------------"
logger "                     START TIME:${TIMESTAMP}                                 "
logger "                   Purging Files In Staging Area                             "
logger "-----------------------------------------------------------------------------"

CTRY_CD=$1
SRC_SYS_CD=$2
FILE=${TMPDIR}/Files_list_${vdate}_$$.csv
PARTITION_FILE=list_partitions_$$_${vdate}.csv
HDFS_PARTITION=${TMPDIR}/part_hdf_$$_${vdate}
PARTITION_BEFORE_DEL=${TMPDIR}/part_list_BD_$$_${vdate}.hql
EXECFILE=${TMPDIR}/SQL_T1_${vdate}_$$.sql

logger "Starting Purging process for country code : $CTRY_CD"
logger "Source system code: $SRC_SYS_CD"

#Connect to oracle to get the details of directory and files

#logger "Connecting to oracle,to retrieve the details required for purging"
#logger "spooling data to a file: $FILE"

# Changed the spooling logic to execute the query from ${EXECFILE}

echo "set pagesize 50000" > ${EXECFILE}
echo "set heading off" >> ${EXECFILE}
echo "set linesize 1024" >> ${EXECFILE}
echo "set trimspool on" >> ${EXECFILE}
echo "set feedback off" >> ${EXECFILE}

echo "select proc_id||','||src_sys_cd||','||ctry_cd||','||tier_typ||','||retn_typ||','||retn_val||','||REGEXP_REPLACE(db_nm,'<c.+?d>',LOWER(CTRY_CD))||','||tbl_nm||','||tbl_part_txt||','||REGEXP_REPLACE(purge_dir_path,'<c.+?d>',CTRY_CD)||','||purge_flg from edag_purge_master where src_sys_cd='$SRC_SYS_CD' and ctry_cd='$CTRY_CD' and tier_typ='T1.0' and PURGE_FLG = 'Y';" >> ${EXECFILE}

echo "EXIT" >> ${EXECFILE}

$METADBCONN1 @${EXECFILE} | sed '/^\s*$/d' > $FILE
#check the size of the file
if [[ -s "$FILE" ]];then
       logger "$FILE contains the details of the files to be purged "
else
       logger "Invalid ctry_cd:$1,source system code :$2 please enter valid values "
       echo "Invalid ctry_cd:$1,source system code :$2 please enter valid values "
       exit 12
fi
cat $FILE | awk -F "," '{print $5,$6,$7,$8,$9,$10,$11}' | awk 'NF'  > ${TMPDIR}/List_${vdate}.$$
logger "List_${vdate}.$$ contains purge data"
logger "---------------------------------------------------------------------------------"
while read line 
do
echo "${TIMESTAMP}:INFO - $line" >> $LOGFILE
done < ${TMPDIR}/List_${vdate}.$$
logger "---------------------------------------------------------------------------------"
#logger "Started reading file: List_${vdate}.$$"


#Reading details from file_list to purge
while read line
do
DB_NM=`echo $line | awk -F " " '{print $3}'`
TB_NM=`echo $line | awk -F " " '{print $4}'`
logger "                     "
logger "Reading below details for DATABASE:$DB_NM and TABLE:$TB_NM"
logger "----------------------------------------------------------"
DIR_PATH=`echo $line | awk -F " " '{print $6}' | awk -F"/" '{print $1"/"$2"/"$3"/"$4"/"$5}'`
logger "Directory Path : $DIR_PATH"
RETN_VAL=`echo $line | awk -F " " '{ print $2}'`
logger "Retention value: $RETN_VAL"
RETN_TYP=`echo $line | awk -F " " '{print $1}'`
logger "Retention type: $RETN_TYP"
PURGE_FLG=`echo $line | awk -F " " '{print $7}'`
logger "Purge flag: $PURGE_FLG"

if [[ "$PURGE_FLG" != 'Y' ]]; then
logger "Purge Flag is not 'Y', Skipping.."
continue
fi

# If Invalid dir_path found just come out
DIR_PATH_PAT=`echo $DIR_PATH | grep -i '/user/hive/warehouse'`
if [ $? -eq 0 ] || [[ "$DIR_PATH" == "" ]]; then
logger "Incorrect directory path.T1.0 tables should not be in hive directory path.Invalid Metadata.Exiting..."
logger "Invalid Metadata:$DB_NM,$TB_NM,$DIR_PATH,$RETN_VAL,$RETN_TYP,$PURGE_FLG .Skipping" 
continue
fi

# If Invalid dbname found just come out
DB_PAT=`echo $DB_NM | awk -F "_" '{print $1}'`
if [[ "$DB_PAT" != "staging" ]]; then
logger "Database is not staging database.Exiting.."
logger "Invalid Metadata:$DB_NM,$TB_NM,$DIR_PATH,$RETN_VAL,$RETN_TYP,$PURGE_FLG .Skipping"
continue 
fi


# "Calculating retention date based on retention type and retention value"
#if [[ "$RETN_TYP" == "M" ]]; then
#logger "Calculating current date from edag_business_date table to find retention date for retention type 'Month'"
#CUR_DT=$($METADBCONN1 <<EOF
#set heading off
#set pagesize 0
#select to_char(to_date(CURR_BIZ_DT, 'dd-mon-yy'), 'YYYY-MM-DD') from edag_business_date where src_sys_cd='$SRC_SYS_CD' and ctry_cd='$CTRY_CD' and freq_cd='M';
#exit;
#EOF
#)
#logger "$CUR_DT is the current business date for country code $ctry_cd and source system code $src_sys_cd"
#RETN_DT=$($METADBCONN1 <<EOF
#set heading off
#set pagesize 0
#select add_months(to_date('$CUR_DT','yyyy-mm-dd'),-$RETN_VAL) from dual;
#exit;
#EOF
#)
#elif [[ "$RETN_TYP" == "D" ]]; then
#logger "Calculating retention date for retention type 'Daily'"
#RETN_DT=$($METADBCONN1 <<EOF  
#set heading off
#set pagesize 0
#select trunc(to_date('$BIZ_DT','yyyy-mm-dd'))-$RETN_VAL from dual;
#exit;
#EOF
#)
#logger "Retention date for file retention type 'D': $RETN_DT"
#elif [[ "$RETN_TYP" == "Y" ]]; then
#RETN_DT=$($METADBCONN1 <<EOF
#set heading off
#set pagesize 0
#select add_months(to_date('$BIZ_DT','yyyy-mm-dd'),-12) from dual;
#exit;
#EOF
#)
if [[ "$RETN_TYP" == "G" ]]; then
logger "Calculating date for partition record number:$RETN_VAL from hdfs location"
RETN_DT=`hdfs dfs -ls $DIR_PATH | awk -F "=" '{print $2}' | awk 'NF' | sort -r | awk 'NR=='$RETN_VAL''` 
fi

if [[ "$RETN_DT" == "" ]]; then
logger "Retention date is null, since total number of partitions are less than $RETN_VAL files"
logger "Skipping $DIR_PATH"
continue
fi
  
logger "Retention date :$RETN_DT"
# "copying retention date: $RETN_DT, database namme:$DB_NM and tablename:$TB_NM to hql files"

#creating hql file to list out all the partitions before dropping
echo "!sh echo $DB_NM.$TB_NM" >> $PARTITION_BEFORE_DEL
echo "SHOW PARTITIONS $DB_NM.$TB_NM;" >> $PARTITION_BEFORE_DEL
echo "!sh echo END_$DB_NM.$TB_NM" >> $PARTITION_BEFORE_DEL

#creating hql file for dropping partitions
echo "ALTER TABLE $DB_NM.$TB_NM DROP IF EXISTS PARTITION (biz_dt<'$RETN_DT');" >> ${TMPDIR}/part_hive_$$_${vdate}.hql
echo "$DIR_PATH,$RETN_DT,$DB_NM.$TB_NM" >>$HDFS_PARTITION 

done <${TMPDIR}/List_${vdate}.$$

# "Executing hql files to list and drop partitions"
echo "==========================================" >$LOGDIR/$PARTITION_FILE
echo "LIST OF PARTITIONS BEFORE DROPPING IN HIVE" >>$LOGDIR/$PARTITION_FILE
echo "==========================================" >>$LOGDIR/$PARTITION_FILE

#connecting to hive to list and drop partitions
logger "Connecting to hive to list partitions"
beeline -u "$HIVEDBCONN" --silent=true --outputformat=csv --showHeader=false -f $PARTITION_BEFORE_DEL >> $LOGFILE 2>&1 1>>$LOGDIR/$PARTITION_FILE 

#Appending part_list_BD_$$.hql to part_hive_$$.hql to list partitions after dropping
cat >> ${TMPDIR}/part_hive_$$_${vdate}.hql $PARTITION_BEFORE_DEL
echo "==========================================" >>$LOGDIR/$PARTITION_FILE
echo "LIST OF PARTITIONS AFTER DROPPING" >>$LOGDIR/$PARTITION_FILE
echo "==========================================" >>$LOGDIR/$PARTITION_FILE
logger "part_hive_$$_${vdate}.hql file contains statements to drop and show partition after dropping"
logger "--------------------------------------------------------------------------------------------"
#logger `cat part_hive_$$_${vdate}.hql` >> $LOGFILE 

logger "Connecting to hive to drop partitions based on retention date"
logger "-------------------------------------------------------------"
beeline -u "$HIVEDBCONN" --silent=true --outputformat=csv --showHeader=false -f ${TMPDIR}/part_hive_$$_${vdate}.hql >>$LOGFILE 2>&1 1>>$LOGDIR/$PARTITION_FILE
rc1=$?
if [ $rc1 -eq 0 ]; then
logger "Dropped list of partitions for src_sys_cd: $SRC_SYS_CD and ctry_cd: $CTRY_CD" 
else
logger " An error in dropping partition list for src_sys_cd: $SRC_SYS_CD and ctry_cd: $CTRY_CD" 
exit 99
fi
cat $LOGDIR/$PARTITION_FILE | awk 'NF' >$LOGDIR/MOD_PARTITION_FILE_$$_${vdate}
logger "$PARTITION_FILE contains list of partitions before and after dropping"
logger "====================================================================="
while read line
do 
echo "${TIMESTAMP}:INFO - $line" >> $LOGFILE
done <$LOGDIR/MOD_PARTITION_FILE_$$_${vdate}

logger "                                                                          "
logger "Reading below details from file part_hdf_$$ to purge partition dir in HDFS"
logger "--------------------------------------------------------------------------"
#purging partitions in hdfs
while read line
do
DIR_PTH=`echo $line | awk -F "," '{print $1}'`
#logger "Checking in directory: $DIR_PTH"
RETN_DATE=`echo $line | awk -F "," '{print $2}'`
#logger "Searching partitions in $DIR_PTH which are older than $RETN_DATE"
DB_TB_NM=`echo $line | awk -F "," '{print $3}'`
RETN_DATE_M=`date -d $RETN_DATE +"%Y%m%d"`
logger "Purging partitions in HDFS path: $DIR_PTH, which are older than Retention date:$RETN_DATE_M"
count=0
for i in `hdfs dfs -find $DIR_PTH | awk -F"=" '{print $2}' | awk 'NF' | awk -F "/" '{print $1}' | sort | uniq`
do
  PT_DATE=`date -d $i +"%Y%m%d"`
  #logger "Retention date: $RETN_DATE_M"
  logger "partition date: $PT_DATE"
  #logger "Comapring partition date and retention date"
  if [ $PT_DATE -lt $RETN_DATE_M ]; then
    logger "$PT_DATE -lt $RETN_DATE_M"
    HIVE_PURGE_STATUS=`sed -n '/AFTER/,$p' $PARTITION_FILE | sed -n '/'$DB_TB_NM'/,/END_'$DB_TB_NM'/p' | grep $i`
    if [ $? -eq 0 ]; then
       logger"Partition $i has not dropped in HIVE, hence not purging in HDFS"
    else
       #logger "Partition file $i dropped in HIVE, hence purging below partitions in HDFS"
        count=`expr $count + 1`
        logger  "purging $DIR_PTH/biz_dt=$i"
        hdfs dfs -rm -r $DIR_PTH/biz_dt=$i
       if [ $? -ne 0 ]; then
         logger "Failed to delete partition: $DIR_PTH/biz_dt=$i"
       fi
    fi
  fi
done
if [[ "$count" -eq 0 ]]; then
logger "No partitions to purge in $DIR_PTH"
fi
logger " "
done < $HDFS_PARTITION

rm -f $HDFS_PARTITION
rm -f ${TMPDIR}/part_hive_$$_${vdate}.hql
rm -f $PARTITION_BEFORE_DEL
rm -f ${TMPDIR}/Files_list_${vdate}_$$.csv
rm -f ${TMPDIR}/List_${vdate}.$$
rm -f $LOGDIR/$PARTITION_FILE
rm -f $LOGDIR/MOD_PARTITION_FILE_$$_${vdate}
rm -f ${EXECFILE}
rm -f ${FILE}
 
logger "---------------------------------------------------------------------------------------------------"
logger "                               End Time: ${TIMESTAMP}                                              "
logger "---------------------------------------------------------------------------------------------------"
