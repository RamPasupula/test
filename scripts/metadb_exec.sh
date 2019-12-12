#!/usr/bin/ksh
#|==============================================================================
#|   tddb_exec.sh
#|==============================================================================
#|
#|   Project:          UOB EDAG 
#|   Author:           Shaun Kim
#|   Initial Date:     19 December 2016
#|
#|==============================================================================
#|
#|   Description :     Deploy EDF Metadata Database Object by executing sql 
#|
#|   Parameters  :     
#|
#|                     PARAM1 - full path of the calling package
#|                     PARAM2 - environment number (i.e. "S01", "D01")
#|                     PARAM3 - EDA code version number (optional: "" "01" "02" etc)
#|                     PARAM4 - TNS Name
#|                     PARAM5 - Schema Name
#|                     PARAM6 - individual email address (optional)
#|
#|==============================================================================
#|
#|   Modification History:
#|   ====================
#|
#|   Date      Who  Description
#|   --------  ---  ------------------------------------------------------------
#|   19/12/16  SCK  Created 
#|
#|==============================================================================

#|==============================================================================
#|   Set Variable
#|==============================================================================
export PackageFile=$1
export EnvNmSn=$2
export EmailAddress=$3

# Determine path
BASE_NM=`basename $0`
BASE_PATH=`dirname $0`
echo $BASE_PATH

if [ "$BASE_PATH" = "." ]; then
        BASE_PATH=`pwd`
fi

export ENV_PATH=`echo $BASE_PATH | rev | awk -F "/" '{print $4"/"$5"/"$6"/"$7"/"$8}'| rev | sed 's/\/\///g'`
export ENVVn=`echo $BASE_PATH | rev | awk -F "/" '{print $3}' | rev`

. /$ENV_PATH/$ENVVn/edf/config/setenv.sh
PN=$(basename $0)

LogFileName=${PN}_${PackageFile}_${TIMESTAMP}_$$.log
#LogFileName=${PN}.log

ArchivePackageFile=`hostname`_${TNSNm}`echo ${PackageFile} | sed -e 's/\//_/g'`_${TIMESTAMP}
#ArchivePackageFile=`hostname`_${TNSNm}`echo ${PackageFile} | sed -e 's/\//_/g'`_${TIMESTAMP}

LogTS="`date +'%Y/%m/%d %H:%M:%S'` ${PN}" 
print "${LogTS} I: ==============================================================================" > $LOGDIR/$LogFileName
print "${LogTS} I:  Script Name:${PN}">> $LOGDIR/$LogFileName
print "${LogTS} I: ==============================================================================">> $LOGDIR/$LogFileName
print "${LogTS} I: Variable Lists ">> $LOGDIR/$LogFileName
print "${LogTS} I: Environment Name: ${ENVNM}">> $LOGDIR/$LogFileName
print "${LogTS} I: Environment Number: ${ENVSN}">> $LOGDIR/$LogFileName
print "${LogTS} I: Oracle TNS Name: ${TNSNm}">> $LOGDIR/$LogFileName
print "${LogTS} I: Oracle Schema Name: ${SchemaNm}">> $LOGDIR/$LogFileName
print "${LogTS} I: Home Directory: ${MODULEHOME} ">> $LOGDIR/$LogFileName
print "${LogTS} I: TimeStamp: ${TIMESTAMP}">> $LOGDIR/$LogFileName
print "${LogTS} I: SQL Directory: ${SQLDIR}">> $LOGDIR/$LogFileName
print "${LogTS} I: Script Directory: ${SRTDIR}">> $LOGDIR/$LogFileName
print "${LogTS} I: Archive Directory: ${ARCDIR}">> $LOGDIR/$LogFileName
print "${LogTS} I: Log Directory: ${LOGDIR}">> $LOGDIR/$LogFileName
print "${LogTS} I: Temp Directory: ${TMPDIR}">> $LOGDIR/$LogFileName
print "${LogTS} I: Package File : ${PackageFile}">> $LOGDIR/$LogFileName
print "${LogTS} I: METADATA DB Connection: ${METADBCONN}">> $LOGDIR/$LogFileName
print "${LogTS} I: Email List: ${EMAILLIST}">> $LOGDIR/$LogFileName
print "${LogTS} I: ==============================================================================">> $LOGDIR/$LogFileName

#|==============================================================================
#|   Check if PackageFile exist
#|==============================================================================
LogTS="`date +'%Y/%m/%d %H:%M:%S'` ${PN}" 
print "${LogTS} I: Check if PackageFile '${PackageFile}' exist">> $LOGDIR/$LogFileName

# Check if a file exists
if [[ ! -f $PackageFile ]]; then
	LogTS="`date +'%Y/%m/%d %H:%M:%S'` ${PN}" 
	print "${LogTS} F: Package List File ${PackageFile} is not found" >> $LOGDIR/$LogFileName
    print "${LogTS} F: ==============================================================================">> $LOGDIR/$LogFileName
	print "${LogTS} F: Application Terminated (Exit Code: ${EC_FATAL})">> $LOGDIR/$LogFileName
	print "${LogTS} F: File ${PackageFile} is not found" 
	print "${LogTS} F: Application Terminated (Exit Code: ${EC_FATAL})"
	exit $EC_FATAL
fi

# Check if a file is readable
if [[ ! -r $PackageFile ]]; then
	LogTS="`date +'%Y/%m/%d %H:%M:%S'` ${PN}" 
	print "${LogTS} F: Insufficient read privileges on Package List File ${PackageFile}" >> $LOGDIR/$LogFileName
    print "${LogTS} F: ==============================================================================">> $LOGDIR/$LogFileName
	print "${LogTS} F: Application Terminated (Exit Code: ${EC_FATAL})">> $LOGDIR/$LogFileName
	print "${LogTS} F: Insufficient read privileges on file ${PackageFile}"
	print "${LogTS} F: Application Terminated (Exit Code: ${EC_FATAL})"
	exit $EC_FATAL
fi

LogTS="`date +'%Y/%m/%d %H:%M:%S'` ${PN}" 
print "${LogTS} I: Package File Exists">> $LOGDIR/$LogFileName
print "${LogTS} I: ==============================================================================">> $LOGDIR/$LogFileName

#|==============================================================================
#| Prepare Runtime Files and variables 
#|==============================================================================
LogTS="`date +'%Y/%m/%d %H:%M:%S'` ${PN}" 
print "${LogTS} I: Prepare Runtime Files and variables">> $LOGDIR/$LogFileName

# Create runtime directory 
#RunTimeDir=${TMPDIR}/runtime 
RunTimeDir=${TMPDIR}/${TIMESTAMP}
mkdir -p $RunTimeDir
mkdir -p $RunTimeDir/repo 
print "${LogTS} I: Runtime Directory : ${RunTimeDir}">> $LOGDIR/$LogFileName

# Runtime Filelist 
#FileList=${RunTimeDir}/${PN}.lst
FileList=${RunTimeDir}/${PN}_${TIMESTAMP}_$$.lst
print "${LogTS} I: Runtime Fileslist: ${FileList}">> $LOGDIR/$LogFileName

#| Runtime SQL script 
#SqlExecScript=${RunTimeDir}/${PN}.sql
SqlExecScript=${RunTimeDir}/${PN}_${TIMESTAMP}_$$.sql
print "${LogTS} I: Runtime sql Script : ${SqlExecScript}">> $LOGDIR/$LogFileName

#| Create sed replace token file 
#SedToken=${RunTimeDir}/${PN}.sed
SedToken=${RunTimeDir}/${PN}_${TIMESTAMP}_$$.sed 
echo "s/\[ENV\]/${ENVNM}${ENVSN}/g" > $SedToken
echo "s/\[DATADIR\]/${DATADIR}/g" >> $SedToken
print "${LogTS} I: Runtime SED token file: ${SedToken}">> $LOGDIR/$LogFileName
print "${LogTS} I: ==============================================================================">> $LOGDIR/$LogFileName


#|==============================================================================
#| copy the SQL script to runtime directory and replace the token 
#| if any of the SQL script included inside the package doesn't exist, quit
#|==============================================================================
LogTS="`date +'%Y/%m/%d %H:%M:%S'` ${PN}" 
print "${LogTS} I: Check if all SQL files exist">> $LOGDIR/$LogFileName
print "${LogTS} I: If existing, move to runtime execution folder">> $LOGDIR/$LogFileName

grep "@" $PackageFile | sed -e "s/@//g" >  $FileList
for SQLFileName in `cat $FileList`
do
	print "${LogTS} I: checking for file ${SQLFileName}">> $LOGDIR/$LogFileName

	if [[ ! -f ${SQLDIR}/$SQLFileName ]] ; then 
		print "${LogTS} F: SQL file ${SQLFileName} doesn't exist in ${SQLDIR}" 1>&2;
		# Logging in to log file 
		print "${LogTS} F: ==============================================================================">> $LOGDIR/$LogFileName
		print "${LogTS} F: Missing SQL files  ${SQLDIR}/${SQLFileName}">> $LOGDIR/$LogFileName
		print "${LogTS} F: This notification email is generated automatically">> $LOGDIR/$LogFileName
		print "${LogTS} F: EDA Infrastructure team should investigate this">> $LOGDIR/$LogFileName                                                                 #" >> $LOGDIR/$LogFileName
		print "${LogTS} F: ==============================================================================">> $LOGDIR/$LogFileName
		# send alert email
		#mail -s `hostname`"@"$ORACLE_SID": Warning: missing SQL file " $gdw_infra_email_addr < $LOG_DIR/ctrlm_exe.sql.log.$$
		print "${LogTS} F: Application Terminated (Exit Code: ${EC_FATAL})">> $LOGDIR/$LogFileName
		print "${LogTS} F: Missing SQL files" ${SQLDIR}/$SQLFileName 
		print "${LogTS} F: Application Terminated (Exit Code: ${EC_FATAL})"	
		exit $EC_FATAL
	fi
    print "${LogTS} I: copying SQL file ${SQLFileName} to runtime direcoty ${RunTimeDir}/sql/${SQLFileName}" >> $LOGDIR/$LogFileName
	
	# create sub-directory if deployment list have sub-directory structure in runtime dir. 
	tmpString=`echo $SQLFileName | grep "/" | cut -d"/" -f 1`
	if [[ ! -z $tmpString ]]; then
		# Check if a directory exists
		if [[ ! -d  ${RunTimeDir}/repo/${tmpString} ]]; then
			mkdir -p ${RunTimeDir}/repo/${tmpString}
		fi		
	fi 
	cp ${SQLDIR}/${SQLFileName} ${RunTimeDir}/repo/${SQLFileName}
	print "${LogTS} I: replace token in runtime script">> $LOGDIR/$LogFileName
	sed -i -f ${SedToken}  ${RunTimeDir}/repo/${SQLFileName}	
done
print "${LogTS} I: ==============================================================================">> $LOGDIR/$LogFileName

#|==============================================================================
#| Construct SQL script 
#|==============================================================================
LogTS="`date +'%Y/%m/%d %H:%M:%S'` ${PN}" 
print "${LogTS} I: Constructing sql script to execute sql ">> $LOGDIR/$LogFileName

echo "set echo on" > $SqlExecScript
echo "set feedback on" >> $SqlExecScript
echo "" >> $SqlExecScript

for SQLFileName in `cat $FileList`
do
	echo "@${RunTimeDir}/repo/${SQLFileName}" >> $SqlExecScript
done
echo "EXIT">> $SqlExecScript
print "${LogTS} I: SQL script ${SqlExecScript} created  ">> $LOGDIR/$LogFileName
print "${LogTS} I: ==============================================================================">> $LOGDIR/$LogFileName

#|==============================================================================
#| Execute SQL Script 
#|==============================================================================
LogTS="`date +'%Y/%m/%d %H:%M:%S'` ${PN}" 
print "${LogTS} I: Executing SQL script : ${SqlExecScript}">> $LOGDIR/$LogFileName
print " ">> $LOGDIR/$LogFileName

echo $METADBCONN $SqlExecScript

$METADBCONN @${SqlExecScript} >> $LOGDIR/$LogFileName  2>&1


STATUS=$?
print " ">> $LOGDIR/$LogFileName

LogTS="`date +'%Y/%m/%d %H:%M:%S'` ${PN}" 
if [[ $STATUS -ne 0 ]]; then 
	print "${LogTS} F: ==============================================================================">> $LOGDIR/$LogFileName
	print "${LogTS} F:  Error Occurred during execution of SQL script : ${SqlExecScript}">> $LOGDIR/$LogFileName
	print "${LogTS} F:  Please review execution script and investigate">> $LOGDIR/$LogFileName
	print "${LogTS} F:  EDA Infrastructure team should investigate this">> $LOGDIR/$LogFileName                                                                 #" >> $LOGDIR/$LogFileName
	print "${LogTS} F: ==============================================================================">> $LOGDIR/$LogFileName
	print "${LogTS} F:  Error Occurred during execution of SQL script : ${SqlExecScript}"
else
	if grep '^ORA-' $LOGDIR/$LogFileName
	then 
		print "${LogTS} F: ==============================================================================">> $LOGDIR/$LogFileName
		print "${LogTS} F:  Error Occurred during execution of SQL script : ${SqlExecScript}">> $LOGDIR/$LogFileName
		print "${LogTS} F:  Please review execution script and investigate">> $LOGDIR/$LogFileName
		print "${LogTS} F:  EDA Infrastructure team should investigate this">> $LOGDIR/$LogFileName                                                                 #" >> $LOGDIR/$LogFileName
		print "${LogTS} F: ==============================================================================">> $LOGDIR/$LogFileName
		print "${LogTS} F:  Error Occurred during execution of SQL script : ${SqlExecScript}"
		STATUS=$EC_FATAL
	else 
		print "${LogTS} I: ==============================================================================">> $LOGDIR/$LogFileName
		print "${LogTS} I:  Successfully executed  SQL script : ${SqlExecScript}">> $LOGDIR/$LogFileName
		print "${LogTS} I: ==============================================================================">> $LOGDIR/$LogFileName
	fi
fi 

print " ">> $LOGDIR/$LogFileName
print " ">> $LOGDIR/$LogFileName

#|==============================================================================
#| Construct Log file for email 
#|==============================================================================
# paste package sql file to execution log file
print "==============================================================================">> $LOGDIR/$LogFileName
print  "* SQL Package File: ${PackageFile}">> $LOGDIR/$LogFileName
print "==============================================================================">> $LOGDIR/$LogFileName
cat  $PackageFile >> $LOGDIR/$LogFileName
print " ">> $LOGDIR/$LogFileName

# paste runtile list sql file to execution log file
print "==============================================================================">> $LOGDIR/$LogFileName
print  "* Runtime List File: ${FileList}">> $LOGDIR/$LogFileName
print "==============================================================================">> $LOGDIR/$LogFileName
cat  $FileList >> $LOGDIR/$LogFileName
print " ">> $LOGDIR/$LogFileName

print "==============================================================================">> $LOGDIR/$LogFileName
print  "* Runtime SED token  File:  ${SedToken}">> $LOGDIR/$LogFileName
print "==============================================================================">> $LOGDIR/$LogFileName
cat  $SedToken >> $LOGDIR/$LogFileName
print " ">> $LOGDIR/$LogFileName

print "==============================================================================">> $LOGDIR/$LogFileName
print  "* Runtime SQL Script: ${SqlExecScript}">> $LOGDIR/$LogFileName
print "==============================================================================">> $LOGDIR/$LogFileName
cat  $SqlExecScript >> $LOGDIR/$LogFileName
print " ">> $LOGDIR/$LogFileName

for SQLFileName in `cat $FileList`
do
	print "==============================================================================">> $LOGDIR/$LogFileName
	print  "* Runtime SQL Script : ${RunTimeDir}/repo/${SQLFileName}">> $LOGDIR/$LogFileName
	print "==============================================================================">> $LOGDIR/$LogFileName
	cat ${RunTimeDir}/repo/${SQLFileName} >> $LOGDIR/$LogFileName
	print " ">> $LOGDIR/$LogFileName

done

if [[ $STATUS -ne 0 ]]; then 
	print "==============================================================================">> $LOGDIR/$LogFileName
	print "${LogTS} F: Application Terminated (Exit Code: ${EC_FATAL})">> $LOGDIR/$LogFileName
	print "${LogTS} I: Log file can be found in $LOGDIR/$LogFileName"
	print "${LogTS} F: Application Terminated (Exit Code: ${EC_FATAL})"
else 
	print "==============================================================================">> $LOGDIR/$LogFileName
	print "${LogTS} I: The Package file has been moved to">> $LOGDIR/$LogFileName
	print "${LogTS} I: ${ARCDIR}/${ArchivePackageFile}">> $LOGDIR/$LogFileName	
	#mv -f ${PackageFile} $ARCDIR/$ArchivePackageFile
	
	print "==============================================================================">> $LOGDIR/$LogFileName
	print "${LogTS} I: Application completed successfully (Exit Code: ${EC_OK})">> $LOGDIR/$LogFileName	
	print "${LogTS} I: The Package file has been moved to"
	print "${LogTS} I: ${ARCDIR}/${ArchivePackageFile}"
	print "${LogTS} I: Log File: $LOGDIR/$LogFileName"
	print "${LogTS} I: Application completed successfully (Exit Code: ${EC_OK})"
	rm -R ${RunTimeDir}
fi
# send alert email
#mail -s `hostname`"@"$ORACLE_SID": $exec_status in executing $PackageFile " $email_addr < $LOGDIR/$LogFileName

exit $STATUS

