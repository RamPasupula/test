package com.uob.edag.model;

import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.List;

import org.apache.commons.lang.StringUtils;
import org.apache.commons.lang3.builder.ToStringBuilder;

import com.uob.edag.constants.UobConstants;
import com.uob.edag.exception.EDAGException;

/**
 * @Author       : Daya Venkatesan.
 * @Date of Creation: 10/31/2016
 * @Description    : The class is the model for storing details of every process instance 
 *                   during execution.
 * 
 */

public class ProcessInstanceModel {
  private ProcessModel procModel;
  private String procInstanceId;  // Uniquely generated process instance id
  private String bizDate;      // Business Date for which the process is executed
  private String countryCd;    // Country of the Process
  private Timestamp startTime;      // Start Time of the Process
  private Timestamp endTime;      // End Time of the Process
  private String status;      // Status of the Process
  private EDAGException exception;    // Error Description , if any, of the process
  private String fileNm;       // Name of the File getting processed
  private ControlModel controlModel;     // Control Model of the Instance
  private String origFileNm;    // Original File Name
  private boolean dumped = false;
  private String errorTxt; 
  private String procId;
  private String sourceSystemId;
  private String tempFileNm;
  private int tempFileRowCount;
  private String hourToRun;
  private boolean isBizDateValidationDisabled = Boolean.FALSE;
  private boolean isRowCountReconciliationDisabled = Boolean.FALSE;
  private boolean isRowCountValidationDisabled = Boolean.FALSE;
  private boolean isHashSumReconciliationDisabled = Boolean.FALSE;
  private boolean isMD5SumValidationDisabled = Boolean.FALSE;
  private boolean isSkipErrRecordsEnabled = Boolean.FALSE;
  private boolean isSkipIndexingFlag = Boolean.FALSE;
  private String bizDateValidationMessage;
  private String rowCountReconciliationMessage;
  private String rowCountValidationMessage;
  private String hashSumReconciliationMessage;
  private String md5SumValidationMessage;
  private String skipErrRecordsMessage;

    // EDF - 236 - File Arrival/FileSize Op report
    private String srcFileArrivalTime = UobConstants.EMPTY;
    private String srcFileSizeBytes = UobConstants.EMPTY;
  
  public ProcessInstanceModel() {
	  
  }
  
  public ProcessInstanceModel(ProcessModel procModel) {
  	this.procModel = procModel;
  }

  @Override
  public String toString() {
    return new ToStringBuilder(this).append("procInstanceId", procInstanceId)
    		                            .append("bizDate", bizDate)
    		                            .append("procID", procModel == null ? procId: procModel.getProcId())
    		                            .append("countryCd", countryCd)
    		                            .append("startTime", startTime)
    		                            .append("endTime", endTime) 
    		                            .append("status", status)
    		                            .append("exception", exception)
    		                            .append("error", errorTxt)
    		                            .append("fileNm", fileNm)
    		                            .append("controlModel", controlModel)
    		                            .append("origFileNm", origFileNm)
    		                            .append("dumped", dumped)
    		                            .append("hourToRun", hourToRun)
    		                            .append("isBizDateValidationDisabled",isBizDateValidationDisabled)
    		                            .append("isRowCountReconciliationDisabled", isRowCountReconciliationDisabled)
    		                            .append("isRowCountValidationDisabled", isRowCountValidationDisabled)
    		                            .append("isHashSumReconciliationDisabled", isHashSumReconciliationDisabled)
    		                            .append("isMD5SumValidationDisabled",isMD5SumValidationDisabled)
                                        .append("isSkipErrRecordsDisabled", isSkipErrRecordsEnabled)
    		                            .append("isBizDateValidationDisabled",isBizDateValidationDisabled)
    		                            .append("isSkipIndexingFlag",isSkipIndexingFlag)
    		                            .append("bizDateValidationMessage", bizDateValidationMessage)
    		                            .append("rowCountReconciliationMessage", rowCountReconciliationMessage)
    		                            .append("rowCountValidationMessage",rowCountValidationMessage)
    		                            .append("hashSumReconciliationMessage", hashSumReconciliationMessage)
    		                            .append("md5SumValidationMessage",md5SumValidationMessage)
                                        .append("skipErrRecordsMesssage", skipErrRecordsMessage)
                                        .append("srcFileArrivalTime", srcFileArrivalTime)
                                        .append("srcFileSizeBytes", srcFileSizeBytes)
    		                            .toString();
  }

  public boolean isDumped() {
  	return dumped;
  }
  
  public void setDumped(boolean dumped) {
  	this.dumped = dumped;
  }
  
  public String getOrigFileNm() {
    return origFileNm;
  }

  public void setOrigFileNm(String origFileNm) {
    this.origFileNm = origFileNm;
  }
  
  public String getFileNm() {
    return fileNm;
  }

  public void setFileNm(String fileNm) {
    this.fileNm = fileNm;
  }

  public ControlModel getControlModel() {
    return controlModel;
  }
  
  public void setControlModel(ControlModel controlModel) {
    this.controlModel = controlModel;
  }
  
  public String getProcInstanceId() {
    return procInstanceId;
  }
  
  public void setProcInstanceId(String procInstanceId) {
    this.procInstanceId = procInstanceId;
  }
  
  public String getBizDate() {
    return bizDate;
  }
  
  public void setBizDate(String bizDate) {
    this.bizDate = bizDate;
  }
  
  public String getProcId() {
    return procModel == null? procId: procModel.getProcId();
  }
  
  public void setProcId(String procId) {
	  this.procId = procId;
  }
  
  public String getSourceSystemId() {
    return procModel == null? sourceSystemId: procModel.getSrcSysCd();
  }
  
  public void setSourceSystemId(String sourceSystemId) {
	  this.sourceSystemId = sourceSystemId;
  }
  
  public String getCountryCd() {
    return countryCd;
  }
  
  public void setCountryCd(String countryCd) {
    this.countryCd = countryCd;
  }
  
  public Timestamp getStartTime() {
    return startTime;
  }
  
  public void setStartTime(Timestamp startTime) {
    this.startTime = startTime;
  }
  
  public Timestamp getEndTime() {
    return endTime;
  }
  
  public void setEndTime(Timestamp endTime) {
    this.endTime = endTime;
  }
  
  public String getStatus() {
    return status;
  }
  
  public void setStatus(String status) {
    this.status = status;
  }
  
  public EDAGException getException() {
    return exception;
  }
  
  public void setException(EDAGException ex) {
    this.exception = ex;
  }
  
  public String getErrorTxt() {
  	return this.exception != null ? StringUtils.substring(StringUtils.trimToNull(this.exception.getMessage()), 0, 2000) : this.errorTxt;
  }
  
  public void setErrorTxt(String txt) {
    this.errorTxt = txt;
  }

  public String getTempFileNm() {
	return tempFileNm;
  }

  public void setTempFileNm(String tempFileNm) {
	this.tempFileNm = tempFileNm;
  }

  public int getTempFileRowCount() {
	return tempFileRowCount;
  }

  public void setTempFileRowCount(int tempFileRowCount) {
	this.tempFileRowCount = tempFileRowCount;
  }

  public void setHourToRun(String hourToRun) {
	this.hourToRun = hourToRun;
  }
  
  public String getHourToRun() {
	return this.hourToRun;
  }
  
  public boolean isBizDateValidationDisabled() {
	return isBizDateValidationDisabled;
  }

  public void setBizDateValidationDisabled(boolean isBizDateValidationDisabled) {
	this.isBizDateValidationDisabled = isBizDateValidationDisabled;
  }

  public boolean isRowCountReconciliationDisabled() {
	return isRowCountReconciliationDisabled;
  }

  public void setRowCountReconciliationDisabled(boolean isRowCountValidationDisabled) {
	this.isRowCountReconciliationDisabled = isRowCountValidationDisabled;
  }
  
  public boolean isRowCountValidationDisabled() {
	return isRowCountValidationDisabled;
  }

  public void setRowCountValidationDisabled(boolean isRowCountValidationDisabled) {
	this.isRowCountValidationDisabled = isRowCountValidationDisabled;
  }
  
  public boolean isHashSumValidationDisabled() {
	return isHashSumReconciliationDisabled;
  }

  public void setHashSumReconciliationDisabled(boolean isHashSumValidationDisabled) {
	this.isHashSumReconciliationDisabled = isHashSumValidationDisabled;
  }

  public boolean isMD5SumValidationDisabled() {
	return isMD5SumValidationDisabled;
  }

  public void setMD5SumValidationDisabled(boolean isMD5SumValidationDisabled) {
	this.isMD5SumValidationDisabled = isMD5SumValidationDisabled;
  }


  public boolean isSkipErrRecordsEnabled() {
	  return isSkipErrRecordsEnabled;
  }
 
  public void setSkipErrRecordsEnabled(boolean isSkipErrRecordsEnabled) {
	  this.isSkipErrRecordsEnabled = isSkipErrRecordsEnabled;
  }
  
  public boolean isSkipIndexingEnabled() {
	  return isSkipIndexingFlag;
  }
 
  public void setSkipIndexingEnabled(boolean isSkipIndexingFlag) {
	  this.isSkipIndexingFlag = isSkipIndexingFlag;
  }

  public String getBizDateValidationMessage() {
	return bizDateValidationMessage;
  }

  public void setBizDateValidationMessage(String bizDateValidationMessage) {
	this.bizDateValidationMessage = bizDateValidationMessage;
  }

  public String getRowCountReconciliationMessage() {
	return rowCountReconciliationMessage;
  }

  public void setRowCountReconciliationMessage(String rowCountValidationMessage) {
	this.rowCountReconciliationMessage = rowCountValidationMessage;
  }
  
  public String getRowCountValidationMessage() {
	return rowCountValidationMessage;
  }

  public void setRowCountValidationMessage(String rowCountValidationMessage) {
	this.rowCountValidationMessage = rowCountValidationMessage;
  }

  public String getHashSumValidationMessage() {
	return hashSumReconciliationMessage;
  }

  public void setHashSumValidationMessage(String hashSumValidationMessage) {
	this.hashSumReconciliationMessage = hashSumValidationMessage;
  }

  public String getMd5SumValidationMessage() {
	return md5SumValidationMessage;
  }

  public void setMd5SumValidationMessage(String md5SumValidationMessage) {
	this.md5SumValidationMessage = md5SumValidationMessage;
  }

    public String getSkipErrRecordsMessage() {
        return skipErrRecordsMessage;
    }

    public void setSkipErrRecordsMessage(String skipErrRecordsMessage) {
        this.skipErrRecordsMessage = skipErrRecordsMessage;
    }


    public String getSuppressionMessage() {
	List<String> messagesList = new ArrayList<>();
	if(this.getBizDateValidationMessage() != null) {
		messagesList.add(this.getBizDateValidationMessage());
	}
	if(this.getRowCountReconciliationMessage() != null) {
		messagesList.add(this.getRowCountReconciliationMessage());
	}
	if(this.getHashSumValidationMessage() != null) {
		messagesList.add(this.getHashSumValidationMessage());
	}
	if(this.getMd5SumValidationMessage() != null) {
		messagesList.add(this.getMd5SumValidationMessage());
	}
	
	if(this.getRowCountValidationMessage() != null) {
		messagesList.add(this.getRowCountValidationMessage());
	}
	if(this.getSkipErrRecordsMessage() != null){
	    messagesList.add(this.getSkipErrRecordsMessage());
	}
	return String.join(UobConstants.COMMA, messagesList);
  }

    public String getSrcFileArrivalTime() {
        return srcFileArrivalTime;
    }

    public void setSrcFileArrivalTime(String srcFileArrivalTime) {
        this.srcFileArrivalTime = srcFileArrivalTime;
    }

    public String getSrcFileSizeBytes() {
        return srcFileSizeBytes;
    }

    public void setSrcFileSizeBytes(String srcFileSizeBytes) {
        this.srcFileSizeBytes = srcFileSizeBytes;
    }
}
