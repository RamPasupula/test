#!/bin/sh
#|==============================================================================
#|   bizdtUpdate.sh
#|==============================================================================
#|
#|   Project:          UOB EDAG
#|   Author:           Ganapathy Raman
#|   Initial Date:     03 March 2017
#|
#|==============================================================================
#|
#|   Description :     Update EDAG_BUSINESS_DATE based on input values
#|
#|   Parameters  :     Source System Code,Country Code,Frequency Code
#|   Mandatory Associated Files: set_env.param -> Variable Declaration file
#!                                
#!                                                
#|       
#|==============================================================================
#|
#|   Modification History:
#|   ====================
#|
#|   Date      Who  Description
#|   --------  ---  ------------------------------------------------------------
#|   19/12/16  SCK  Created
#|   17/09/18  BALA Added Support for Quarterly(Q) and Yearly(Y)
#|
#|==============================================================================

#|==============================================================================
#|   Set Variable
#|==============================================================================

set -x

# Determine path
BASE_NM=`basename $0`
BASE_PATH=`dirname $0`
echo $BASE_PATH

if [ "$BASE_PATH" = "." ]; then
        BASE_PATH=`pwd`
fi

export ENVNM=`echo $BASE_PATH | rev | awk -F "/" '{print $4"/"$5"/"$6"/"$7"/"$8}'| rev | sed 's/\/\///g'`
export ENVVR=`echo $BASE_PATH | rev | awk -F "/" '{print $3}'| rev`

#export SCRIPTDIR="$ROOT_HOME/$ENVNM/$ENVVR/edf/appl/bin"
export FUNCDIR="/$ENVNM/$ENVVR/edf/scripts"
export CONFDIR="/$ENVNM/$ENVVR/edf/config"

if [ -e $FUNCDIR/functions.sh -a -s $FUNCDIR/functions.sh ]
then
	. $FUNCDIR/functions.sh
else
	logger " Functions does not exist.Exiting" "1"
fi


if [ -e $CONFDIR/set_env.param -a -s $CONFDIR/set_env.param ]
then
	. $CONFDIR/set_env.param
else
	logger " Environment variable not set.Exiting" "1"
fi

export TABLE="$SCHEMA.EDAG_BUSINESS_DATE"
echo $SqlExecScript
echo ${dataTmp}

# Input parameter validation
if [ "$#" -ne 3 ] || [ -z "$1" ] || [ -z "$2" ] || [ -z "$3" ]; then
   echo "Usage:Please input proper values for source system code,country code and frequency code "
   exit 1
fi

src_sys_cd=$1
ctry_cd=$2
freq=$3

# Current date in dd-Mon-YY format
currdt=`date +%d-%b-%y`

#check validity of input values with metadata
chkSQL="select count(1) from ${TABLE} where src_sys_cd = '${src_sys_cd}' and ctry_cd = '${ctry_cd}' and freq_cd = '${freq}';"
echo "set heading off" > ${SqlExecScript}
echo ${chkSQL} >> ${SqlExecScript}
echo "quit" >> ${SqlExecScript}

$METADBCONN @${SqlExecScript} | egrep -v "Release|Copyright|Last Successful|Connected"  >${dataTmp} 2>&1
STATUS=$(grep -ic '^ORA-' ${dataTmp}) 

if [[ $STATUS -ne 0 ]]; then
        cat ${dataTmp} | sed -e '/^$/d'  >> $LOGFILE
        logger "=============================================================================="
        logger "Error Occurred during execution of SQL script : ${chkSQL}"
        logger "Please review execution script and investigate"
        logger "EDA Infrastructure team should investigate this"                                                                 
        logger "=============================================================================="
        RC=1
        
else
        cnt=$(cat ${dataTmp} | sed -e '/^$/d' | awk '{print $1}' )
        logger "=============================================================================="
        logger "Successfully executed  SQL : ${chkSQL}"
        logger "=============================================================================="
		   
fi

#--------

#If count of rows is 0 or greater than 1 then it is invalid data
if [ ${cnt} -eq 0  -o ${cnt} -gt 1 ]; then
   logger "The count:$cnt of rows fetched for ${src_sys_cd}:${ctry_cd}:${freq} is invalid.Please fix the metadata" "1"
else  
   logger " The input parameter values:${src_sys_cd}:${ctry_cd}:${freq} are present in metadata.Proceeding"
fi


#Capture previous values in log
prevbizSQL="select prev_biz_dt,curr_biz_dt,next_biz_dt,proc_flg from ${TABLE} where src_sys_cd = '${src_sys_cd}' and ctry_cd = '${ctry_cd}' and freq_cd = '${freq}';"
echo "column proc_flg format a10" > $SqlExecScript
echo ${prevbizSQL} >> $SqlExecScript
echo "quit" >> $SqlExecScript

$METADBCONN @${SqlExecScript} | egrep -v "Release|Copyright|Last Successful|Connected"  > ${dataTmp} 2>&1
STATUS=$(grep -ic '^ORA-' ${dataTmp}) 

if [[ $STATUS -ne 0 ]]; then
        cat ${dataTmp} | sed -e '/^$/d'  >> $LOGFILE
        logger "=============================================================================="
        logger "Error Occurred during execution of SQL : ${prevbizSQL}"
        logger "Please review execution script and investigate"
        logger "EDA Infrastructure team should investigate this"                                                                 
        logger "=============================================================================="
        RC=1
        
else
        logger "=============================================================================="
        logger "Successfully executed  SQL  : ${prevbizSQL}"
        logger "=============================================================================="
        logger "PREVIOUS VALUE OF TABLE ${TABLE}" 
        logger "=============================================================================="
	cat ${dataTmp} | sed -e '/^$/d' >> $LOGFILE   
        echo -e "\n"  >> $LOGFILE
        logger "=============================================================================="
fi
#--------
# Check with edag_proc_holiday table if the holiday_dt match the next_biz_dt.
# We are using next_biz_dt as the below update stmt would converts next_biz_dt to curr_biz_dt

holidaychkSQL="select count(1) from ${TABLE} t,edag_proc_holiday h where t.next_biz_dt = h.holiday_dt and t.ctry_cd = h.ctry_cd and t.src_sys_cd = h.src_sys_cd and t.ctry_cd = '${ctry_cd}' and t.src_sys_cd = '${src_sys_cd}' and t.freq_cd = '${freq}' ;"
echo "set heading off" > $SqlExecScript
echo ${holidaychkSQL} >> $SqlExecScript
echo "quit" >> $SqlExecScript

$METADBCONN @${SqlExecScript} | egrep -v "Release|Copyright|Last Successful|Connected"  > ${dataTmp} 2>&1
STATUS=$(grep -ic '^ORA-' ${dataTmp}) 

if [[ $STATUS -ne 0 ]]; then
        cat ${dataTmp} | sed -e '/^$/d'  >> $LOGFILE
        logger "=============================================================================="
        logger "Error Occurred during execution of SQL : ${holidaychkSQL}"
        logger "Please review execution script and investigate"
        logger "EDA Infrastructure team should investigate this"                                                                 
        logger "=============================================================================="
        RC=1
        
else
        hcnt=$(cat ${dataTmp} | sed -e '/^$/d' | awk '{print $1}' )
        logger "=============================================================================="
        logger "Successfully executed  SQL  : ${holidaychkSQL}"
        logger "=============================================================================="
       	
fi

#--------
# Proc update only when  frequency code is daily.For monthly no update is needed
# 
if  [[ ${hcnt} -eq 0 ]] && [[ "${freq}" == "D" ]]; then 
     logger "Current Biz Date do not match the holiday date.Process flag would be set to Y"
     echo -e "\n"  >> $LOGFILE
     updStmt="update ${TABLE} set prev_biz_dt = curr_biz_dt, curr_biz_dt = next_biz_dt, next_biz_dt = next_biz_dt + 1, proc_flg='Y' where ctry_cd = '${ctry_cd}' and src_sys_cd = '${src_sys_cd}' and freq_cd = '${freq}';"
elif [[ ${hcnt} -gt 0 ]] && [[ "${freq}" == "D" ]]; then
     logger "Current Biz Date match  the holiday date.Process flag would be set to N"
     echo -e "\n"  >> $LOGFILE
     updStmt="update ${TABLE} set prev_biz_dt = curr_biz_dt, curr_biz_dt = next_biz_dt, next_biz_dt = next_biz_dt + 1, proc_flg='N' where ctry_cd = '${ctry_cd}' and src_sys_cd = '${src_sys_cd}' and freq_cd = '${freq}';"
elif [[ "${freq}" == "M" ]]; then
     logger "This is a monthly process.Process flag would not be checked"
     echo -e "\n"  >> $LOGFILE
     updStmt="update ${TABLE} set prev_biz_dt = curr_biz_dt, curr_biz_dt = '${currdt}', next_biz_dt = null where src_sys_cd = '${src_sys_cd}' and ctry_cd = '${ctry_cd}' and freq_cd = '${freq}'; "
elif [[ "${freq}" == "Q" ]]; then
     logger "This is a quarterly process.Process flag would not be checked"
     echo -e "\n"  >> $LOGFILE
     updStmt="update ${TABLE} set prev_biz_dt = curr_biz_dt, curr_biz_dt = '${currdt}', next_biz_dt = null where src_sys_cd = '${src_sys_cd}' and ctry_cd = '${ctry_cd}' and freq_cd = '${freq}'; "
elif [[ "${freq}" == "Y" ]]; then
     logger "This is a yearly process.Process flag would not be checked"
     echo -e "\n"  >> $LOGFILE
     updStmt="update ${TABLE} set prev_biz_dt = curr_biz_dt, curr_biz_dt = '${currdt}', next_biz_dt = null where src_sys_cd = '${src_sys_cd}' and ctry_cd = '${ctry_cd}' and freq_cd = '${freq}'; "
elif [[ ${hcnt} -lt 0 ]]; then
     logger "Invalid value for holiday check sql hcnt variable value is ${hcnt}" "1"
else 
     logger "Invalid frequency code ${freq}" "1"   
fi
#--------

# Run update statement
echo "set heading off" > $SqlExecScript
echo ${updStmt} >> $SqlExecScript
echo "commit;" >> $SqlExecScript
echo "quit" >> $SqlExecScript

$METADBCONN @${SqlExecScript} | egrep -v "Release|Copyright|Last Successful|Connected"  > ${dataTmp} 2>&1
STATUS=$(grep -ic '^ORA-' ${dataTmp}) 

if [[ $STATUS -ne 0 ]]; then
        cat ${dataTmp} | sed -e '/^$/d'  >> $LOGFILE
        logger "=============================================================================="
        logger "Error Occurred during execution of SQL  : ${updStmt}"
        logger "Please review execution script and investigate"
        logger "EDA Infrastructure team should investigate this"                                                                 
        logger "=============================================================================="
        RC=1
        
else
        logger "=============================================================================="
        logger "Successfully executed  SQL  : ${updStmt}"
        logger "=============================================================================="
	cat ${dataTmp} | sed -e '/^$/d'  >> $LOGFILE   
        echo -e "\n" >> $LOGFILE
        logger "UPDATE COMPLETED SUCCESSFULLY"
        logger "=============================================================================="
fi

#--------

# Run value check post update
echo "column proc_flg format a10" > $SqlExecScript
echo ${prevbizSQL} >> $SqlExecScript
echo "quit" >> $SqlExecScript

$METADBCONN @${SqlExecScript} | egrep -v "Release|Copyright|Last Successful|Connected"  > ${dataTmp} 2>&1
STATUS=$(grep -ic '^ORA-' ${dataTmp})

if [[ $STATUS -ne 0 ]]; then
        cat ${dataTmp} | sed -e '/^$/d'  >> $LOGFILE
        logger "=============================================================================="
        logger "Error Occurred during execution of SQL  : ${prevbizSQL}"
        logger "Please review execution script and investigate"
        logger "EDA Infrastructure team should investigate this"
        logger "=============================================================================="
        RC=1

else
        logger "=============================================================================="
        logger "Successfully executed  SQL  : ${prevbizSQL}"
        logger "=============================================================================="
        logger "CURRENT VALUE OF TABLE ${TABLE}"
        logger "=============================================================================="
        cat ${dataTmp} | sed -e '/^$/d'  >> $LOGFILE
        echo -e "\n" >> $LOGFILE
        logger "=============================================================================="
fi

#--------


if [ $STATUS -eq 0 ]; then   
   logger "The bizdtUpdater process completed successfully"
else 
   logger "The bizdtUpdater process failed" "1"
fi



 

