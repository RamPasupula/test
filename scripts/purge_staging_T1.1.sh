#!/bin/sh
#|==============================================================================
#|   purge_Staging_T1.1.sh
#|==============================================================================
#|
#|   Project:          UOB EDAG
#|   Author:           Ganapathy Raman
#|   Initial Date:     14 Aug 2017
#|
#|==============================================================================
#|
#|   Description :     Script to purge the partitions in Staging T1.1
#|
#|==============================================================================
#|
#|   Modification History:
#|   ====================
#|
#|   Date      Who        Description
#|   --------  ---        ------------------------------------------------------
#|   14/08/17  Ganapathy  Created
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

#Set the variabels
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
export ENV=`grep EnvNmSn /$ENVNM/$ENVVR/edf/config/setenv.sh | head -1 | awk -F "=" '{print $2}'`

# Check if the functions file exists
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
   echo "USAGE: purge_Staging_T1.1.sh ctry_cd source_system_code" >> $LOGFILE
   echo "[ERROR]:Two input parameters required for T1.1 purge " >> $LOGFILE
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
LISTING_T11=${TMPDIR}/List_T11_${vdate}.$$
FILE_T11=${TMPDIR}/Files_list_T11_${vdate}_$$.csv
PARTITION_FILE_T11=list_partitions_T11_$$_${vdate}.csv
PARTITION_BEFORE_DEL_T11=${TMPDIR}/part_list_BD_T11_$$_${vdate}.hql
DROP_PARTITIONS=${TMPDIR}/DROP_PART_${vdate}_$$
touch ${DROP_PARTITIONS}
EXECFILE=${TMPDIR}/SQL_T1.1_${vdate}_$$.sql
logger "Starting T1.1 Purging process for country code : $CTRY_CD"
logger "Source system code: $SRC_SYS_CD"

#Connect to oracle to get the details of directory and files
#logger "Connecting to oracle,to retrieve the details required for purging"

echo "set pagesize 50000" > ${EXECFILE}
echo "set heading off" >> ${EXECFILE}
echo "set linesize 1024" >> ${EXECFILE}
echo "set trimspool on" >> ${EXECFILE}
echo "set feedback off" >> ${EXECFILE}

echo "select proc_id||','||src_sys_cd||','||ctry_cd||','||tier_typ||','||retn_typ||','||retn_val||','||REGEXP_REPLACE(db_nm,'<c.+?d>',LOWER(CTRY_CD))||','||tbl_nm||','||tbl_part_txt||','||','||purge_flg||','||PURGE_TBL_TYP||','||ROLL_RETN_VAL  from edag_purge_master where src_sys_cd='${SRC_SYS_CD}' and ctry_cd='${CTRY_CD}' and tier_typ='T1.1' and purge_flg='Y';" >> ${EXECFILE}

echo "EXIT" >> ${EXECFILE}

$METADBCONN1 @${EXECFILE} | sed '/^\s*$/d' > $FILE_T11

#check the size of the file
if [[ -s "$FILE_T11" ]];then
   logger "$FILE_T11 contains the details of the files to be purged "
else
   logger "Invalid ctry_cd:$1,source sytem code :$2 please enter valid country code "
   echo "Invalid ctry_cd:$1,source sytem code :$2 please enter valid country code "
   exit 12
fi

cat $FILE_T11 | awk -F "," '{print $5,$6,$7,$8,$9,$10,$12,$13,$14}' | awk 'NF'  >$LISTING_T11
logger "$LISTING_T11  contains purge data"
logger "---------------------------------------------------------------------------------"

# Log the metadata into the logfile
while read line
do
echo "${TIMESTAMP}:INFO - $line" >> $LOGFILE
done < $LISTING_T11
logger "---------------------------------------------------------------------------------"
#logger "Started reading file: List_${vdate}.$$"

#Reading details from file_list to generate the show partitions statements
# While loop start
while read line
do
DB_NM=`echo $line | awk -F " " '{print $3}'`
TB_NM=`echo $line | awk -F " " '{print $4}'`
#logger "                     "
#logger "Reading below details for DATABASE:*$DB_NM* and TABLE:*$TB_NM*"
#logger "----------------------------------------------------------"
RETN_VAL=`echo $line | awk -F " " '{ print $2}'`
#logger "Retention value: $RETN_VAL"
RETN_TYP=`echo $line | awk -F " " '{print $1}'`
#logger "Retention type: $RETN_TYP"
PURGE_FLG=`echo $line | awk -F " " '{print $7}'`
#logger "Purge flag: $PURGE_FLG"

DB_PAT=`echo $DB_NM | awk -F "_" '{print $1}'`
if [[ "$DB_PAT" != "$ENV" ]]; then
   logger "Database is not staging database"
   logger "Skipping"
   continue
fi

if [[ "$RETN_TYP" != "G" ]]; then
   logger "Skipping"
   continue
fi

#creating hql file to list out all the partitions before dropping
echo "!sh echo $DB_NM.$TB_NM, $RETN_VAL" >>$PARTITION_BEFORE_DEL_T11
echo "SHOW PARTITIONS $DB_NM.$TB_NM;" >>$PARTITION_BEFORE_DEL_T11
echo "!sh echo END_$DB_NM.$TB_NM" >>$PARTITION_BEFORE_DEL_T11

done < $LISTING_T11
#While loop end

# "Executing hql files to list and drop partitions"
echo "==========================================" >$LOGDIR/$PARTITION_FILE_T11
echo "LIST OF PARTITIONS BEFORE DROPPING IN HIVE" >>$LOGDIR/$PARTITION_FILE_T11
echo "==========================================" >>$LOGDIR/$PARTITION_FILE_T11

#connecting to hive to list and drop partitions
logger " -------------------------------------"
logger "|CONNECTING TO HIVE TO LIST PARTITIONS|"
logger " -------------------------------------"
beeline -u "$HIVEDBCONN" --silent=true --outputformat=csv --showHeader=false -f $PARTITION_BEFORE_DEL_T11 >> $LOGFILE 2>&1 1>>$LOGDIR/$PARTITION_FILE_T11
logger "Creating HQL list file to drop below partitions"
logger "==============================================="

#While loop start.This loop generates the purge command.
while read line
do
DBTB=`echo $line | awk -F " " '{print $3"."$4}'`
RETN_VAL=`echo $line | awk -F " " '{print $2}'`
TBL_TYPE=`echo $line | awk -F " " '{print $8}'`
ROLL_RETN_VAL=`echo $line | awk -F " " '{print $9}'`

logger "checking partitions in table :*$DBTB* for retention value:$RETN_VAL"
logger "-------------------------------------------------------------------------------------------------"

rollcnt=0

# Checking if the table type is Transaction/Master/reference.Rolling purge appiled for Master/Reference tables.
if [[ "$TBL_TYPE" == "TXN" ]]; then
   logger "Skipping rolling purge and using standard retention value"
elif [[ "$TBL_TYPE" == "MR" ]]; then
   logger "Using the rolling purge as the table is a master/reference table"
   rollcnt=1
else
   logger "Skipping purge.Invalid value"
   continue
fi

if [[ $rollcnt -eq 0 ]]; then
   cat $LOGDIR/$PARTITION_FILE_T11 | sed -n '/'$DBTB'/,/END_'$DBTB'/p' | grep -v $DBTB | grep $CTRY_CD | sort -r | awk 'NR > '$RETN_VAL'' >>${TMPDIR}/TOBE_DROP_${vdate}_$$
else
   # Get the data filtered based on rolling retention value to a file
   cat $LOGDIR/$PARTITION_FILE_T11 | sed -n '/'$DBTB'/,/END_'$DBTB'/p' | grep -v $DBTB | grep $CTRY_CD | sort -r   | awk 'NR > '$ROLL_RETN_VAL'' >> ${TMPDIR}/TOBE_ROLL_ALL_${vdate}_$$

   if [ `wc -l ${TMPDIR}/TOBE_ROLL_ALL_${vdate}_$$ | awk '{print $1}'` -gt $RETN_VAL ]; then
   	#Get the first month boundry value yyyy-mm 
   	first_mnth_bndry=$(cat ${TMPDIR}/TOBE_ROLL_ALL_${vdate}_$$ | awk -F"/" '{print $2}' | awk -F"=" '{print $2}' | tr -d "'" | cut -c1-7 | uniq | head -1)
   	# Get the first day of the month ending in a boundry month
   	first_day_mnth=$(echo $first_mnth_bndry"-01" | sed "s/-//g")

   	#Get the last day of the month ending in a boundry month
   	last_day_mnth=$(cat ${TMPDIR}/TOBE_ROLL_ALL_${vdate}_$$ | grep -i $first_mnth_bndry | sort -r | head -1 | awk -F"/" '{print $2}' | awk -F"=" '{print $2}' | tr -d "'" | sed "s/-//g")
   	# Get difference of days 
   	diff_days=$((last_day_mnth - first_day_mnth))
   	if [ $diff_days -ge 0 ]; then
   	   cat ${TMPDIR}/TOBE_ROLL_ALL_${vdate}_$$ | grep $first_mnth_bndry >> ${TMPDIR}/TOBE_DROP_${vdate}_$$
           cat ${TMPDIR}/TOBE_ROLL_ALL_${vdate}_$$ | grep -v $first_mnth_bndry >> ${TMPDIR}/TOBE_ROLL_${vdate}_$$
   	elif [ $diff_days -lt 0 ]; then
           cat ${TMPDIR}/TOBE_ROLL_ALL_${vdate}_$$ >> ${TMPDIR}/TOBE_ROLL_${vdate}_$$
   	else
           logfile "Invalid diff days calculation.$diff_days.Exiting" "1"
   	fi

   	# Get the yr and month boundary values in yyyy-mm to a file
   	cat ${TMPDIR}/TOBE_ROLL_${vdate}_$$ | awk -F"/" '{print $2}' | awk -F"=" '{print $2}' | tr -d "'" | cut -c1-7 | uniq > ${TMPDIR}/TOBE_MNTH_ROLL_${vdate}_$$

   	# Loop through the month values and exclude last partition of the month
   	for part in `cat ${TMPDIR}/TOBE_MNTH_ROLL_${vdate}_$$`
   	do
   	    grep $part ${TMPDIR}/TOBE_ROLL_${vdate}_$$ | sort -nr | sed -n -e '2,$p' >> ${TMPDIR}/TOBE_DROP_${vdate}_$$
            grep $part ${TMPDIR}/TOBE_ROLL_${vdate}_$$ | sort -nr | head -1 >> ${TMPDIR}/TOBE_RETAINED_${vdate}_$$
        done
   	mnth_roll_rtn_val=0
   	mnth_roll_rtn_val=$(grep -ic "$CTRY_CD" ${TMPDIR}/TOBE_RETAINED_${vdate}_$$)
   	if [ ${RETN_VAL} > ${mnth_roll_rtn_val} ]; then
   	   cat ${TMPDIR}/TOBE_RETAINED_${vdate}_$$ | sort -r | awk 'NR > '${RETN_VAL}'' >> ${TMPDIR}/TOBE_DROP_${vdate}_$$
   	fi
   	rm -f ${TMPDIR}/TOBE_ROLL_${vdate}_$$
   	rm -f ${TMPDIR}/TOBE_ROLL_ALL_${vdate}_$$
   	rm -f ${TMPDIR}/TOBE_MNTH_ROLL_${vdate}_$$
   	rm -f ${TMPDIR}/TOBE_RETAINED_${vdate}_$$
   else
      logger "Skipping purge as the number of remaining rolling partitions after retaining minimum $ROLL_RETN_VAL occurences are less than $RETN_VAL"
      rm -f ${TMPDIR}/TOBE_ROLL_ALL_${vdate}_$$
   fi
fi

if [[ ! -s "${TMPDIR}/TOBE_DROP_${vdate}_$$" ]];then
   logger "No partition found that qualify for purge for DBNAME:$DBTB,country_cd:$CTRY_CD,Retention_value:$RETN_VAL"
   logger "Skipping purge" 
else
   for partition in `cat ${TMPDIR}/TOBE_DROP_${vdate}_$$`
   do
      logger "$partition"
      site_id=`echo $partition | awk -F"/" '{print $1}' | awk -F"=" '{print $2}'`
#logger "site_id: $site_id"
      biz_dt=`echo $partition | awk -F"/" '{print $2}' | awk -F"=" '{print $2}' | tr -d "'"`
#logger "biz_dt: $biz_dt"
      echo "ALTER TABLE $DBTB DROP IF EXISTS PARTITION(site_id='$site_id', biz_dt='$biz_dt');" >>$DROP_PARTITIONS
   done
fi
rm -f ${TMPDIR}/TOBE_DROP_${vdate}_$$
done < $LISTING_T11

# While loop ends

#Execute the drop partitions hql statements

if [[ ! -s "$DROP_PARTITIONS" ]]; then
   logger "No partition qualify for purge for country_cd:$CTRY_CD,Source_System:${SRC_SYS}"
   logger "Skipping drop partition"
else
   logger " -------------------------------------"
   logger "|CONNECTING TO HIVE TO DROP PARTITIONS|"
   logger " -------------------------------------"
   beeline -u "$HIVEDBCONN" --silent=true --outputformat=csv --showHeader=false -f $DROP_PARTITIONS >>$LOGFILE 2>&1 
   rc1=$?
   if [ $rc1 -eq 0 ]; then
      logger "Dropped list of partitions for src_sys_cd: $SRC_SYS_CD and ctry_cd: $CTRY_CD"
   else
      logger " An error in dropping partition list for src_sys_cd: $SRC_SYS_CD and ctry_cd: $CTRY_CD"
      exit 99
   fi
fi

logger "------------------------------------------------------------------------------------------"
logger "                                  End Time: ${TIMESTAMP}                                  "
logger "------------------------------------------------------------------------------------------"

#Cleanup tmp files
rm -f $FILE_T11
rm -f $LOGDIR/$PARTITION_BEFORE_DEL_T11
rm -f $LISTING_T11
rm -f $DROP_PARTITIONS
rm -f $LOGDIR/$PARTITION_FILE_T11
rm -f ${EXECFILE}
