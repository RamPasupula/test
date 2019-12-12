#!/bin/sh
#|==============================================================================
#|   purge_landing.sh
#|==============================================================================
#|
#|   Project:          UOB EDAG
#|   Author:           Santhi Animireddy
#|   Initial Date:     11 May 2017
#|
#|==============================================================================
#|
#|   Description :     Script to purge the files older than 7 generations 
#|
#|==============================================================================
#|
#|   Modification History:
#|   ====================
#|
#|   Date      Who  Description
#|   --------  ---  ------------------------------------------------------------
#|   11/05/17  Santhi  Created
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
export TMPDIR="${TEMP_DIR}/$ENVVR/tmp"
export LOGFILE="${LOGDIR}/`basename $0`_${vdate}.log"
export SCHEMA=`grep SchemaNm /$ENVNM/$ENVVR/edf/config/setenv.sh | head -1 | awk -F "=" '{print $2}' | sed 's/"//g'`

if [ -e $FUNCDIR/functions.sh -a -s $FUNCDIR/functions.sh ]
then
        . $FUNCDIR/functions.sh
else
        echo " Functions does not exist.Exiting" >> $LOGFILE
        echo " Functions does not exist.Exiting"
        exit 1
fi



# Input parameter validation
if [ $# -ne 1 ]
then
      echo "USAGE: purge_landing.sh ctry_cd" >> $LOGFILE
      echo "[ERROR]: provide country code as input" >> $LOGFILE
      echo "USAGE: purge_landing.sh ctry_cd.Please provide country code as input"
      exit 1
fi

CTRY_CD=$1

logger "==============================================================================" 
logger "                      Script Name: ${BASE_NM}                                 " 
logger "=============================================================================="

logger "-----------------------------------------------------------------------------"
logger "                     START TIME:${TIMESTAMP}                                 "
logger "                   Purging Files In Landing Area for Country Code $CTRY_CD                            "
logger "-----------------------------------------------------------------------------"


FILE=${TMPDIR}/Archive_Files_list_${vdate}_$$.csv

#Connect to oracle to get the details of directory and files

EXECFILE=${TMPDIR}/SQL_Landing_${vdate}_$$.sql

echo "set pagesize 50000" > ${EXECFILE}
echo "set heading off" >> ${EXECFILE}
echo "set linesize 1024" >> ${EXECFILE}
echo "set trimspool on" >> ${EXECFILE}
echo "set feedback off" >> ${EXECFILE}

echo "select PROC_ID||','||SRC_SYS_CD||','||CTRY_CD||','||RETN_VAL||','||RETN_TYP||','||REGEXP_REPLACE(PURGE_DIR_PATH,'<c.+?d>',LOWER(CTRY_CD))||','||PURGE_FLG from $SCHEMA.edag_purge_master where ctry_cd='$CTRY_CD' and tier_typ = 'L' and PURGE_FLG = 'Y';" >> ${EXECFILE}
echo "exit" >> ${EXECFILE}

$METADBCONN1 @${EXECFILE} | sed '/^\s*$/d' > $FILE

#check the size of the file
if [[ -s "$FILE" ]];then
       logger "$FILE contains the details of the files to be purged "
else
       logger "Invalid ctry_cd: $1, please enter valid country code "
       echo "Invalid ctry_cd: $1, please enter valid country code"
       exit 12
fi

cat ${FILE} | awk -F "," '{print $2,$4,$5,$7,$6}' | cut -d "." -f 1 | sed '/^$/d' > ${TMPDIR}/File_List_${vdate}.$$

#Reading details from file_list to purge
while read line
do
#echo $line
echo -e "\n" >> ${LOGFILE}
SRC_SYS_CD=`echo $line | awk -F " " '{ print $1}'`
logger "Source system code: $SRC_SYS_CD"
RETN_VAL=`echo $line | awk -F " " '{ print $2}'` 
logger "Retention value: $RETN_VAL"  
RETN_TYP=`echo $line | awk -F " " '{print $3}'` 
logger "Retention type: $RETN_TYP"
PURGE_FLG=`echo $line | awk -F " " '{print $4}'`
logger "Purge Flag : $PURGE_FLG"
FILE_PATH=`echo $line | awk -F " " '{print $5}'`
logger "File Path : $FILE_PATH"
#DIR_PATH=`echo $FILE_PATH | awk -F "/" '{print $1"/"$2"/"$3"/"$4"/"$5"/"$6"/"$7}'`
#Removing dependency on path length using $1,$2,....$7

DIR_PATH=`dirname $FILE_PATH`
logger "Directory Path: $DIR_PATH"

#FILE_PATN=`ls -C1 -t $FILE_PATH* | grep -w $FILE_PATH | awk -F "." '{print $1}' | cut -d "/" -f 8 | sort | uniq`
#Removing the cut -f8 dependency as the dir path can be of any length

FILE_PATN=`basename $FILE_PATH`
logger "File Pattern : $FILE_PATN"

#to check if purge flag is 'yes'
if [ $PURGE_FLG != 'Y' ]; then
logger "Purge flag is not 'Y' for this file"
logger "Skipping purge for $FILE_PATN"
continue
fi

#to check if present directory is previous
#PRV_DIR=`echo $DIR_PATH | awk -F "/" '{print $7}'` 
# Removing the awk print $7 dependency when the path is less than 7

PRV_DIR=`basename $DIR_PATH`
if [[ "$PRV_DIR" != "previous" ]]; then
logger "This is not a correct directory"
logger "Invalid metadata for $PRV_DIR.Exiting"
logger "Metadata:$SRC_SYS_CD $RETN_VAL $RETN_TYP $PURGE_FLG $FILE_PATH $DIR_PATH" "1"
fi


#check if any files exists in directory path

skip_flg="N"
ls -lrt $FILE_PATH*  > /dev/null 2>&1
if [ ${?} -ne 0 ]; then
   logger "$FILE_PATN pattern files are not present in this directory : $DIR_PATH.Skipping Purge"
   skip_flg="Y"
else
   logger "$FILE_PATN pattern files exists in $DIR_PATH..proceeding"
fi

#echo ${skip_flg}
if [[ "$RETN_TYP" == "G"  &&  "${skip_flg}" == "N" ]]; then
        #Sorting files in a directory
        logger "Purging files which are older than $RETN_VAL generations"
        ls -C1 -t $FILE_PATH* | grep -w $FILE_PATN | awk -F "." '{print $2}' | sort -n | uniq > ${TMPDIR}/Bizdt_List_${vdate}.$$

        logger "Looking for any invalid date pattern" 
        invalid_cnt=`cat ${TMPDIR}/Bizdt_List_${vdate}.$$ |  egrep -c -ve "^[0-9]{4}-[0-9]{2}-[0-9]{2}$"`

        if [ ${invalid_cnt} -gt 0 ]; then
           logger "Invalid file pattern found while purging data.Excluding them from purge"
           logger "The files excluded from purge are having pattern below in $DIR_PATH.Please review"
           echo "****************************************************" >> $LOGFILE
           for i in `cat ${TMPDIR}/Bizdt_List_${vdate}.$$ | egrep -ve "^[0-9]{4}-[0-9]{2}-[0-9]{2}$"`
           do
             ls -C1 -t $FILE_PATH* | grep -w $FILE_PATN | grep -w ${i} | awk 'NR>0' >> $LOGFILE
           done
           echo "****************************************************" >> $LOGFILE
        else 
           logger "No Invalid date pattern found for file pattern $FILE_PATN at $DIR_PATH"
        fi
      
        logger "Valid biz_dt values for file pattern $FILE_PATN at $DIR_PATH are:"  
        echo "****************************************************" >> $LOGFILE
        echo "`cat ${TMPDIR}/Bizdt_List_${vdate}.$$ | egrep -e "^[0-9]{4}-[0-9]{2}-[0-9]{2}$"`"  >> $LOGFILE
        echo "****************************************************" >> $LOGFILE

        all_cnt=`cat ${TMPDIR}/Bizdt_List_${vdate}.$$ | egrep -ce "^[0-9]{4}-[0-9]{2}-[0-9]{2}$"`
        
        if [ $all_cnt -le $RETN_VAL ]; then
           logger "Distinct biz_dt files greater than $RETN_VAL not available for file pattern $FILE_PATN at $DIR_PATH.Skipping purge"
        else
           logger "Purging files for file pattern $FILE_PATN at $DIR_PATH"    
           purge_cnt=$(( all_cnt - RETN_VAL ))
           if [ $purge_cnt -le 0 ]; then
              logger "$file Files are not available in $DIR_PATH which are older than $RETN_VAL generations"
           else
              logger "Biz date to be identified for purge"
              cat ${TMPDIR}/Bizdt_List_${vdate}.$$ | egrep -e "^[0-9]{4}-[0-9]{2}-[0-9]{2}$" | head -${purge_cnt} > ${TMPDIR}/Purge_List_${vdate}.$$
              echo "The biz_dt identified for purge on file pattern $FILE_PATN at $DIR_PATH are:" >> $LOGFILE
              echo "****************************************************" >> $LOGFILE
              cat ${TMPDIR}/Purge_List_${vdate}.$$ >> $LOGFILE
              echo "****************************************************" >> $LOGFILE

              logger "Files being purged for file pattern $FILE_PATN at $DIR_PATH are below:"
              echo "****************************************************" >> $LOGFILE
 
              for i in `cat ${TMPDIR}/Purge_List_${vdate}.$$`
              do
                 ls -C1 -t $FILE_PATH* | grep -w $FILE_PATN | grep -w ${i} | awk 'NR>0' >> $LOGFILE
                 ls -C1 -t $FILE_PATH* | grep -w $FILE_PATN | grep -w ${i} | awk 'NR>0' | xargs rm -f >> $LOGFILE 
              done
            fi
          fi     
      purge_cnt=0
      all_cnt=0
fi
done <${TMPDIR}/File_List_${vdate}.$$

rm -f ${TMPDIR}/Archive_Files_list_${vdate}_$$.csv
rm -f ${TMPDIR}/File_List_${vdate}.$$
rm -f ${TMPDIR}/SQL_Landing_${vdate}_$$.sql
rm -f ${TMPDIR}/Purge_List_${vdate}.$$
rm -f ${TMPDIR}/Bizdt_List_${vdate}.$$

logger "---------------------------------------------------------------------------------------------------"
logger "                               End Time: ${TIMESTAMP}                                              "
logger "---------------------------------------------------------------------------------------------------"
