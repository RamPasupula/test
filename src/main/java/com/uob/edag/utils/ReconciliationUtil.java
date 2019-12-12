package com.uob.edag.utils;


import java.math.BigDecimal;
import java.math.RoundingMode;

import org.apache.commons.lang3.StringUtils;
import org.apache.log4j.Logger;

import com.uob.edag.constants.UobConstants;
import com.uob.edag.dao.HiveDao;
import com.uob.edag.dao.ImpalaDao;
import com.uob.edag.exception.EDAGMyBatisException;
import com.uob.edag.exception.EDAGValidationException;
import com.uob.edag.model.ControlModel;
import com.uob.edag.model.HadoopModel;
import com.uob.edag.model.ProcessInstanceModel;
import com.uob.edag.model.ProcessModel;
import com.uob.edag.mybatis.CountAndSumResult;

/**
 * @Author : Daya Venkatesan.
 * @Date of Creation: 10/24/2016
 * @Description : The file is used for handling the Reconciliation during the process
 *                finalization stage. Row Count and Hash Sum Validations are currently
 *                supported.
 * 
 */
public class ReconciliationUtil {

  protected Logger logger = Logger.getLogger(getClass());
  
  private boolean useImpala = false;
  
  public ReconciliationUtil() {
  	String propertyName = getClass().getName() + ".useImpala";
  	try {
			this.useImpala = UobUtils.parseBoolean(PropertyLoader.getProperty(propertyName));
			logger.info("Reconciliation will be done using " + (this.useImpala ? "Impala" : "Hive"));
		} catch (EDAGValidationException e) {
			logger.warn("Unable to get " + propertyName + " value as boolean: " + e.getMessage());
		}
  }

  /**
   * This method is used to compare the source and target row count.
   * @param rowCount1 The Row Count in the source
   * @param rowCount2 The Row Count in the Target
   * @return true if the Source and Target Row count matches, false if it
   *     doesnt match
   */
  private boolean validateRowCount(int rowCount1, int rowCount2, int errRowCount, int stagingRecordsCount) {
  	boolean result = rowCount1 == (rowCount2 + errRowCount);
  	boolean resultWithTwoExtracounts = stagingRecordsCount == (rowCount2 + errRowCount) && rowCount1 == (rowCount2 + errRowCount) + 2;
    logger.info("Row count 1: " + rowCount1 + ", row count 2: " + rowCount2 + ", error count: " + errRowCount + 
    		        ", result: " + result);
    return result || !result && resultWithTwoExtracounts;
  }
  
  /** This method is used to validate if the number of error records exceeds the threshold.
   * @param threshold The Threshold defined on the Process. < 1 - Percentage, >= 1 Row Count
   * @param errRowCount The Row Count from the Error Table
   * @param totalRowCount The Total Row Count from the Source
   * @return false if the error exceeds threshold, false if it is below threshold
   */
  private boolean validateErrRowCount(double threshold, double errRowCount, double totalRowCount) {
    if (errRowCount == 0) {
      logger.debug("Error Row Count is 0; Returning true");
      return true;
    }
    
    // TODO: this is bad. The function serves 2 purposes based on a certain condition.
    if (threshold < 1) {
      double errRcrdPercent = errRowCount / totalRowCount;
      boolean result = errRcrdPercent <= threshold;
      logger.debug("Threshold is percentage: " + threshold + ", Error Record Percentage: " + errRcrdPercent + 
      		         ", result: " + result);
      return result;
    } else {
    	boolean result = errRowCount <= threshold; 
      logger.debug("Threshold is row count: " + threshold + ", Error Row Count: " + errRowCount + ", result:  " + result);
      return result;
    }
  }

  /**
   * This method is used to compare the source and target Hash Sum Value.
   * @param srcHash The Hash Sum from the Source File
   * @param destHash The Hash Sum computed in the target
   * @return true if the hash sum matches between source and target, false if it 
   *     doesnt match
   */
  private boolean validateHashSum(String srcHash, String destHash) {
    BigDecimal sh = new BigDecimal(srcHash);
    BigDecimal dh = new BigDecimal(destHash);
    boolean result = sh.compareTo(dh) == 0; 
    logger.info("Comparing Hash Sum: " + srcHash + " and " + destHash + ", result: " + result);
    return result;
  }
  
  /**
   * This method is used to retrieve the error count from the hive error table and control info.
   * @param procInsModel The Process Instance Model containing the Control Info
   * @param procModel The Process Model containing the metadata of the Process
   * @throws Exception when there is an error retrieving the error row count
   */
  public int runErrorThresholdValidation(ProcessInstanceModel procInsModel, ProcessModel procModel)  throws EDAGValidationException, EDAGMyBatisException {
    HadoopModel destModel = procModel.getDestInfo();
    ControlModel controlModel = procInsModel.getControlModel();

    // Get source row count & Error Threshold
    double errThreshold = destModel.getHiveErrorThreshold();
    int totalRowCount = controlModel.getTotalRecords();
    
    String queueName = useImpala ? null : destModel.getHadoopQueueName();
    // Get destination error row count
    HiveDao hiveDao = useImpala ? new ImpalaDao() : new HiveDao();
    int errRowCount;
    String tableName = destModel.getStagingTableName() + UobConstants.UNDERSCORE + "err";
	errRowCount = hiveDao.getErrorRowCount(destModel.getStagingDbName(), tableName, 
		                                       procInsModel.getBizDate(), queueName);
    controlModel.setTotalErrRecordsTarget(errRowCount);
   
    /*

    Refer PEDDAGML-1511, commented out. Code below does not have any relevant requirement.
    if(controlModel.getTotalRecordsStaging() == (controlModel.getTotalRecordsTarget() + errRowCount)) {
    	if(totalRowCount - controlModel.getTotalRecordsTarget() == 2) {
    		totalRowCount = controlModel.getTotalRecordsTarget() - 2;
    	}
    }
    */

    if (!validateErrRowCount(errThreshold, errRowCount, totalRowCount)) {
    	if(procInsModel.isRowCountValidationDisabled()) {
    		logger.info("Row Count Validation is disabled");
    		procInsModel.setRowCountValidationMessage(UobConstants.ROW_COUNT_VALIDATION_MESSAGE);
    	} else {
    		throw new EDAGValidationException(EDAGValidationException.ERROR_COUNT_ABOVE_THRESHOLD, 
                      procInsModel.getProcInstanceId(), errRowCount, errThreshold, totalRowCount);
    	}
    }

    logger.debug("Error Row Count Validation Successful for process instance ID " + procInsModel.getProcInstanceId() + 
    		         ", error count: " + errRowCount + ", error threshold: " + errThreshold + ", total row count: " + totalRowCount);
    return errRowCount;
  }

  /**
   * This method is used to retrieve the Row Count from the Input Source File and Target Hive table.
   * @param procInsModel The Process Instance Model containing the Control Info
   * @param procModel The Process Model containing the metadata of the Process
   * @throws Exception when there is an error retrieving the Row Count
   */
  public void runRowCountValidation(ProcessInstanceModel procInsModel, ProcessModel procModel, 
  		                              int errRowCnt) throws EDAGValidationException, EDAGMyBatisException {
    HadoopModel destModel = procModel.getDestInfo();
    ControlModel controlModel = procInsModel.getControlModel();
    String queueName = useImpala ? null : destModel.getHadoopQueueName();
    
    // Get source row count
    int srcRowCount = controlModel.getTotalRecords();
    int stagingRecordsCount = controlModel.getTotalRecordsStaging();

    // Get destination row count
    HiveDao hiveDao = useImpala ? new ImpalaDao() : new HiveDao();
    int destRowCount;
    
    String procInstanceId = (UobConstants.HISTORY_LOAD_CD.equals(destModel.getLoadTypeCd()) || UobConstants.ADDITIONAL_LOAD_CD.equals(destModel.getLoadTypeCd())) ? procInsModel.getProcInstanceId() : null; 
	destRowCount = hiveDao.getRowCount(destModel.getHiveDbName(), destModel.getHiveTableName(), 
				                               procInsModel.getCountryCd(), procInsModel.getBizDate(), procInstanceId, queueName);
		
    controlModel.setTotalRecordsTarget(destRowCount);

    if (!validateRowCount(srcRowCount, destRowCount, errRowCnt, stagingRecordsCount)) {
    	if(procInsModel.isRowCountReconciliationDisabled()) {
    		logger.info("Row Count Reconciliation is disabled");
    		procInsModel.setRowCountReconciliationMessage(UobConstants.ROW_COUNT_RECONCILIATION_MESSAGE);
    	} else {
    		throw new EDAGValidationException(EDAGValidationException.ROW_COUNT_RECONCILIATION_FAILURE, 
      		                              procInsModel.getProcInstanceId(), srcRowCount, destRowCount, errRowCnt);
    	}
    }

    logger.debug("Row Count Validation Successful for process instance ID " + procInsModel.getProcInstanceId() + 
    		        ", source row count: " + srcRowCount + ", target row count: " + destRowCount + ", error row count: " + errRowCnt);
  }

  /**
   * This method is used to retrieve the Row Count and Hash Sum from the Input Source File and Target Hive table.
   * @param procInsModel The Process Instance Model containing the Control Info
   * @param procModel The Process Model containing the metadata of the Process
   * @throws Exception when there is an error retrieving the Row Count or Hash Sum
   */
  public void runRowCountAndSumValidation(ProcessInstanceModel procInsModel, ProcessModel procModel, int errRowCnt)
      throws EDAGValidationException, EDAGMyBatisException {

    HadoopModel destModel = procModel.getDestInfo();
    ControlModel controlModel = procInsModel.getControlModel();
    String queueName = useImpala ? null : destModel.getHadoopQueueName();

    // Get source row count
    int srcRowCount = controlModel.getTotalRecords();
    int stagingRecordsCount = controlModel.getTotalRecordsStaging();

    // Get source hash sum
    String hashSumColumn = controlModel.getNormalizedHashSumCol();
    if (!procModel.getDestInfo().hasNormalizedField(hashSumColumn)) {
    	throw new EDAGValidationException(EDAGValidationException.MISSING_NORMALIZED_COLUMN, hashSumColumn, procModel.getDestInfo().getHiveTableName(), "Please check file specification of " + procModel.getSrcInfo().getSourceFileName());
    }
    
    String srcHashSum = controlModel.getHashSumVal();
    if (StringUtils.trimToNull(srcHashSum) == null) {
      throw new EDAGValidationException(EDAGValidationException.NULL_VALUE, "Hash sum value", "Hash sum value is empty in control model");
    }

    int precision = 0;
    if (srcHashSum.contains(".")) {
      String precisionStr = srcHashSum.substring(srcHashSum.indexOf(".") + 1, srcHashSum.length());
      precision = precisionStr.length();
    }
    logger.info("Control hash sum is: " + srcHashSum + ", precision is: " + precision);

    // Get destination row count and sum
    HiveDao hiveDao = useImpala ? new ImpalaDao() : new HiveDao();
    String processInstanceId = (UobConstants.HISTORY_LOAD_CD.equals(destModel.getLoadTypeCd()) || UobConstants.ADDITIONAL_LOAD_CD.equals(destModel.getLoadTypeCd()))  ? procInsModel.getProcInstanceId() : null; 
    CountAndSumResult csr = hiveDao.getCountAndSum(destModel.getHiveDbName(), destModel.getHiveTableName(),
                                                   procInsModel.getCountryCd(), procInsModel.getBizDate(), processInstanceId, 
                                                   hashSumColumn, queueName);
    int destRowCount = csr.getCount();
    controlModel.setTotalRecordsTarget(destRowCount);

    if (!validateRowCount(srcRowCount, destRowCount, errRowCnt, stagingRecordsCount)) {
    	if(procInsModel.isRowCountReconciliationDisabled()) {
    		logger.info("Row Count Reconciliation is disabled");
    		procInsModel.setRowCountReconciliationMessage(UobConstants.ROW_COUNT_RECONCILIATION_MESSAGE);
    	} else {
    		throw new EDAGValidationException(EDAGValidationException.ROW_COUNT_RECONCILIATION_FAILURE,
                    procInsModel.getProcInstanceId(), srcRowCount, destRowCount, errRowCnt);
    	}
    }

    logger.debug("Row Count Validation Successful for process instance ID " + procInsModel.getProcInstanceId() +
                 ", source row count: " + srcRowCount + ", target row count: " + destRowCount + ", error row count: " + errRowCnt);

    String destHashSum = csr.getSum();

    logger.debug("Hive hash sum before rounding or truncating is: " + destHashSum);
    String roundedDestHashSum = "0"; // If sum result is null force it to be 0
    if (precision > 0) {
    	roundedDestHashSum += ".";
    	for (int i = 0; i < precision; i++) {
    		roundedDestHashSum += "0";
    	}
    }
    
    if (destHashSum != null) {
      if (destHashSum.indexOf("E") > 0 || destHashSum.indexOf("e") > 0) {
        destHashSum = new BigDecimal(destHashSum).toPlainString();
      }

      // PEDAGML-572 fix - If it's control file then the hash sum given is truncated 2 decimals after the decimal point
      if (UobConstants.CTRL_INFO_C.equalsIgnoreCase(procModel.getSrcInfo().getControlInfo())) {
        logger.debug("Validation sum is in a control file thus destination sum will be truncated to 2 digit after decimal point");
        if (destHashSum.indexOf(".") >= 0) {
          // get precision
          String precisionDest = destHashSum.substring(destHashSum.indexOf(".") + 1, destHashSum.length());
          if (precisionDest.length() < 2) {
            roundedDestHashSum = new BigDecimal(destHashSum + "0").toPlainString();
          } else if (precisionDest.length() > 2) {
            roundedDestHashSum = new BigDecimal(destHashSum.substring(0, destHashSum.indexOf(".") + 3)).toPlainString();
          } else {
            roundedDestHashSum = new BigDecimal(destHashSum).toPlainString();
          }
        } else { // sum is returned as an integer (e.g. 123.00 becomes 123)
          roundedDestHashSum = new BigDecimal(destHashSum + ".00").toPlainString();
        }
      } else {
        logger.debug("Validation sum is in the file's header thus destination sum will be truncated rounded to " +
                     precision + " digit after decimal point");
        roundedDestHashSum = new BigDecimal(destHashSum).setScale(precision, RoundingMode.HALF_UP).toPlainString();
      }
    } else {
      if (UobConstants.CTRL_INFO_C.equalsIgnoreCase(procModel.getSrcInfo().getControlInfo())) { // Control file so precision is 2
        roundedDestHashSum = new BigDecimal("0.00").toPlainString();
      }
    }

    logger.info("Rounded Hash Sum Value is: " + roundedDestHashSum);

    controlModel.setHashSumValTarget(roundedDestHashSum);

    if (!validateHashSum(srcHashSum, roundedDestHashSum)) {
    	if(procInsModel.isHashSumValidationDisabled()) {
    		logger.info("Hash Sum Reconciliation is disabled");
    		procInsModel.setHashSumValidationMessage(UobConstants.HASH_SUM_RECONCILIATION_MESSAGE);
    	} else {
    		throw new EDAGValidationException(EDAGValidationException.HASH_SUM_RECONCILIATION_FAILURE,
                    procInsModel.getProcInstanceId(), srcHashSum, roundedDestHashSum);	
    	}
    }

    logger.debug("Hash Sum Validation Successful for process instance ID " + procInsModel.getProcInstanceId());
  }

  /**
   * This method is used to retrieve the Hash Sum Amount from the Source File and Target Hive table.
   * @param procInsModel The Process Instance Model containing the Control Info
   * @param procModel The Process Model containing the metadata of the Process
   * @throws Exception when there is an error retrieving the hash sum amount.
   */
  public void runHashSumValidation(ProcessInstanceModel procInsModel, ProcessModel procModel) throws EDAGValidationException, EDAGMyBatisException {
    HadoopModel destModel = procModel.getDestInfo();
    ControlModel controlModel = procInsModel.getControlModel();
    String queueName = useImpala ? null : destModel.getHadoopQueueName();

    // Get source hash sum
    String hashSumColumn = controlModel.getNormalizedHashSumCol();
    if (!procModel.getDestInfo().hasNormalizedField(hashSumColumn)) {
    	throw new EDAGValidationException(EDAGValidationException.MISSING_NORMALIZED_COLUMN, hashSumColumn, procModel.getDestInfo().getHiveTableName(), "Please check file specification of " + procModel.getSrcInfo().getSourceFileName());
    }
    
    String srcHashSum = controlModel.getHashSumVal();
    if (StringUtils.trimToNull(srcHashSum) == null) {
    	throw new EDAGValidationException(EDAGValidationException.NULL_VALUE, "Hash sum value", "Hash sum value is empty in control model");
    }
    
    int precision = 0;
    if (srcHashSum.contains(".")) {
      String precisionStr = srcHashSum.substring(srcHashSum.indexOf(".") + 1, srcHashSum.length());
      precision = precisionStr.length();
    }
    logger.info("Control hash sum is: " + srcHashSum + ", precision is: " + precision);

    // Get destination hash sum
    HiveDao hiveDao = useImpala ? new ImpalaDao() : new HiveDao();
    String procInstanceId = (UobConstants.HISTORY_LOAD_CD.equals(destModel.getLoadTypeCd()) || UobConstants.ADDITIONAL_LOAD_CD.equals(destModel.getLoadTypeCd())) ? procInsModel.getProcInstanceId() : null; 
    String destHashSum = hiveDao.getHashSum(destModel.getHiveDbName(), destModel.getHiveTableName(),
        																		procInsModel.getCountryCd(), procInsModel.getBizDate(), procInstanceId, hashSumColumn, queueName);
    logger.debug("Hive hash sum before rounding or truncating is: " + destHashSum);
    String roundedDestHashSum = "0"; // If sum result is null force it to be 0
    if (precision > 0) {
    	roundedDestHashSum += ".";
    	for (int i = 0; i < precision; i++) {
    		roundedDestHashSum += "0";
    	}
    }
    
    if (destHashSum != null) {
    	if (destHashSum.indexOf("E") > 0 || destHashSum.indexOf("e") > 0) {
    		destHashSum = new BigDecimal(destHashSum).toPlainString();
    	}
    	
      // PEDAGML-572 fix - If it's control file then the hash sum given is truncated 2 decimals after the decimal point
      if (UobConstants.CTRL_INFO_C.equalsIgnoreCase(procModel.getSrcInfo().getControlInfo())) {
        logger.debug("Validation sum is in a control file thus destination sum will be truncated to 2 digit after decimal point");
        if (destHashSum.indexOf(".") >= 0) {
          // get precision
          String precisionDest = destHashSum.substring(destHashSum.indexOf(".") + 1, destHashSum.length());
          if (precisionDest.length() < 2) {
            roundedDestHashSum = new BigDecimal(destHashSum + "0").toPlainString();
          } else if (precisionDest.length() > 2) {
            roundedDestHashSum = new BigDecimal(destHashSum.substring(0, destHashSum.indexOf(".") + 3)).toPlainString();
          } else {
            roundedDestHashSum = new BigDecimal(destHashSum).toPlainString();
          }
        } else { // sum is returned as an integer (e.g. 123.00 becomes 123)
          roundedDestHashSum = new BigDecimal(destHashSum + ".00").toPlainString();
        }
      } else {
        logger.debug("Validation sum is in the file's header thus destination sum will be truncated rounded to " + 
                     precision + " digit after decimal point");
        roundedDestHashSum = new BigDecimal(destHashSum).setScale(precision, RoundingMode.HALF_UP).toPlainString();
      }
    } else {
      if (UobConstants.CTRL_INFO_C.equalsIgnoreCase(procModel.getSrcInfo().getControlInfo())) { // Control file so precision is 2
        roundedDestHashSum = new BigDecimal("0.00").toPlainString();
      }
    }

    logger.info("Rounded Hash Sum Value is: " + roundedDestHashSum);

    controlModel.setHashSumValTarget(roundedDestHashSum);

    if (!validateHashSum(srcHashSum, roundedDestHashSum)) {
      throw new EDAGValidationException(EDAGValidationException.HASH_SUM_RECONCILIATION_FAILURE, 
      		                              procInsModel.getProcInstanceId(), srcHashSum, roundedDestHashSum);
    }

    logger.debug("Hash Sum Validation Successful for process instance ID " + procInsModel.getProcInstanceId());
  }
  
  public void setUseImpalaFlag(boolean flag) {
	  this.useImpala = flag;
  }
  public boolean getUseImpalaFlag() {
	  return this.useImpala;
  }
}
