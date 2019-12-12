
# -------------------------------------------------------------------------------
# This function is a logging utility
# It takes message(mandatory) and return code(optional)value as input 
# and logs the message in the LOGFILE.LOGFILE variable declaration needs to be 
# added in the calling program
# -------------------------------------------------------------------------------

function logger
{
set -x

TIMESTAMP=$(date '+%Y-%m-%d-%H:%M:%S')
msg=$1
rc=$2

if [ -z "$msg" ] && [ -z "$rc" ]; then
  echo "$TIMESTAMP:FATAL - Invalid logging.Exiting" >> $LOGFILE
  exit 1
elif [ ! -z "${msg}" ] && [ ! -z "${rc}" ] && [ ${rc} -gt 0 ]; then
  echo "$TIMESTAMP:ERROR - $msg - RETURN CODE - ${rc} " >> $LOGFILE
  exit 10
elif [ ! -z "${msg}" ] && [ ! -z "${rc}" ] && [ ${rc} -eq 0 ]; then
  echo "$TIMESTAMP:INFO - $msg - RETURN CODE - ${rc} " >> $LOGFILE
elif [ ! -z "${msg}" ] && [ -z "${rc}" ]; then
  echo "$TIMESTAMP:INFO - $msg  " >> $LOGFILE
else
   echo "$TIMESTAMP:FATAL - No error message.Exiting" >> $LOGFILE
   exit 2
fi
}

# -------------------------------------------------------------------------------
# This function checks for valid format for date in YYYY-MM-DD.
# It takes date value as input and logs the validity via logger function
# -------------------------------------------------------------------------------

function checkdate {
set -x

dt=$1
date -d ${dt}

if [ $? -ne 0 ]; then
 logger "Invalid date:${dt}" 1
fi

 if [[ ${dt} =~ ^[0-9]{4}-[0-9]{2}-[0-9]{2}$ ]]; then
    logger "Date ${dt} is in valid format (YYYY-MM-DD)"
 else
    logger "Date ${dt} is in invalid format (not YYYY-MM-DD)" 1
 fi

}

# -------------------------------------------------------------------------------
# This function finds the difference between java source files and java class files.
# It takes no input and all variables are sourced from calling script
# Mandatory variable to be declared at calling program are : THE_SOURCEPATH,THE_CLASSFOLDER,LOGDIR
# -------------------------------------------------------------------------------

function diffsrcnclassfiles {
    set -x
        find ${THE_SOURCEPATH} -name \*.java > ${LOGDIR}/tmp_src.out
        find ${THE_CLASSFOLDER} -name \*.class > ${LOGDIR}/tmp_class.out
        >${LOGDIR}/missing_class_files
        for i in `cat ${LOGDIR}/tmp_src.out`
        do
                filename=$(basename ${i} | awk -F "." '{print $1}')
                logger "${filename}"
                grep -w ${filename}".class" tmp_class.out
                if [ $? -ne 0 ]; then
                        logger "Class file does not exist for src file ${i}.java"
                        echo $i >> ${LOGDIR}/missing_class_files
                fi
        done

}
