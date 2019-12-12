#!/usr/bin/ksh
#|==============================================================================
#|   metadb_exec.sh
#|==============================================================================
#|
#|   Project:          UOB EDAG 
#|   Author:           Shaun Kim
#|   Initial Date:     19 December 2016
#|
#|==============================================================================
#|
#|   Description :     Deploy EDF Metadata Database Object by executing sql 
#|  				   This script will be called by Ctrl-M to execute sql script
#|                     This script will call metadb_exec.sh to execute the script in
#|
#|   Parameters  :     
#|                     PARAM1 - full path of the calling package
#|                     PARAM2 - environment number
#|                     PARAM3 - EDA code version number (optional: "" "01" "02" etc))
#|                     PARAM4 - individual email address (optional)
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

CallingPackageSql=$1
EnvNm=$2
IndividualEmail=$3

# Determine path
BASE_NM=`basename $0`
BASE_PATH=`dirname $0`
echo $BASE_PATH

if [ "$BASE_PATH" = "." ]; then
        BASE_PATH=`pwd`
fi

export ENV_PATH=`echo $BASE_PATH | rev | awk -F "/" '{print $4"/"$5"/"$6"/"$7"/"$8}'| rev | sed 's/\/\///g'`
export ENVVn=`echo $BASE_PATH | rev | awk -F "/" '{print $3}'| rev`

#apply to the target environment
/$ENV_PATH/$ENVVn/edf/scripts/metadb_exec.sh "${CallingPackageSql}" "${EnvNm}" "${IndividualEmail}"

exit 0
