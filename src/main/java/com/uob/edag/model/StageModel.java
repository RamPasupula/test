package com.uob.edag.model;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.sql.Timestamp;

import org.apache.commons.lang.StringUtils;
import org.apache.commons.lang3.builder.ToStringBuilder;

import com.uob.edag.model.CountryAttributes.FRREmptyBizDateControl;

/**
 * @Author      : Daya Venkatesan
 * @Date of Creation: 10/24/2016
 * @Description   : The file defines the stage information for every process instance.
 * 
 */

public class StageModel {
  
  //Possible Statuses of a Process
  public static enum Status {
    I,
    S,
    F,
  }

  public String getStageId() {
    return stageId;
  }
  
  public void setStageId(String stageId) {
    this.stageId = stageId;
  }
  
  public String getProcInstanceId() {
    return procInstanceId;
  }
  
  public void setProcInstanceId(String procInstanceId) {
    this.procInstanceId = procInstanceId;
  }
  
  public String getStatus() {
    return status;
  }
  
  public void setStatus(String status) {
    this.status = status;
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
  
  public String getError() {
    return error;
  }
  
  public void setError(String error) {
    this.error = StringUtils.substring(StringUtils.trimToNull(error), 0, 2000);
  }
  
  public int getSrcRowCount() {
    return srcRowCount;
  }
  
  public void setSrcRowCount(int srcRowCount) {
    this.srcRowCount = srcRowCount;
  }
  
  public BigDecimal getSrcHashsumAmt() {
    return srcHashsumAmt;
  }
  
  public void setSrcHashsumAmt(BigDecimal srcHashsumAmt) {
    this.srcHashsumAmt = srcHashsumAmt;
  }
  
  public int getDestRowCount() {
    return destRowCount;
  }
  
  public void setDestRowCount(int destRowCount) {
    this.destRowCount = destRowCount;
  }
  
  public int getETErrorRowCount() {
  	return this.etErrorRowCount;
  }
  
  public void setETErrorRowCount(int count) {
  	this.etErrorRowCount = count;
  }
  
  public int getUVErrorRowCount() {
  	return this.uvErrorRowCount;
  }
  
  public void setUVErrorRowCount(int count) {
  	this.uvErrorRowCount = count;
  }
  
  public BigDecimal getDestHashsumAmt() {
    return destHashsumAmt;
  }
  
  public void setDestHashsumAmt(BigDecimal destHashsumAmt) {
    this.destHashsumAmt = destHashsumAmt;
  }
  
  public String getToolProcId() {
    return toolProcId;
  }
  
  public void setToolProcId(String toolProcId) {
    this.toolProcId = toolProcId;
  }
  
  public String getHashsumFld() {
  	return this.hashsumFld;
  }
  
  public void setHashsumFld(String fld) {
  	this.hashsumFld = StringUtils.trimToNull(fld);
  }
  
  public FRREmptyBizDateControl getFRREmptyBizDateControl() {
  	return frrEmptyBizDateControl;
  }
  
  public void setFRREmptyBizDateControl(String val) {
  	this.frrEmptyBizDateControl = val == null ? FRREmptyBizDateControl.NORMAL : FRREmptyBizDateControl.valueOf(val.toUpperCase());
  }
  
  public String getActualBizDate() {
  	return this.actualBizDate;
  }
  
  public void setActualBizDate(String actual) {
  	this.actualBizDate = actual;
  }
  
// EDF-203
  public BigInteger getReferencedFileCount() {
	return referencedFileCount;
  }

  public void setReferencedFileCount(BigInteger referencedFileCount) {
 	this.referencedFileCount = referencedFileCount;
  }

  public BigInteger getReferencedFileCopied() {
	return referencedFileCopied;
  }

  public void setReferencedFileCopied(BigInteger referencedFileCopied) {
	this.referencedFileCopied = referencedFileCopied;
  }

  public BigInteger getReferencedFileMissing() {
	return referencedFileMissing;
  }

  public void setReferencedFileMissing(BigInteger referencedFileMissing) {
	this.referencedFileMissing = referencedFileMissing;
  }
	// EDF-203 end
  
  public String getDestDbName() {
  	return this.destDbName;
  }
  
  public void setDestDbName(String name) {
  	this.destDbName = name;
  }
  
  public String getDestTableName() {
  	return this.destTableName;
  }
  
  public void setDestTableName(String name) {
  	this.destTableName = name;
  }
  
  public String getDestErrorTableName() {
  	return this.destErrorTableName;
  }
  
  public void setDestErrorTableName(String name) {
  	this.destErrorTableName = name;
  }
  
  public int getTotalErrorRowCount() {
  	return this.totalErrorRowCount;
  }
  
  public void setTotalErrorRowCount(int errorCount) {
  	this.totalErrorRowCount = errorCount;
  }


  private String stageId;      // Unique Stage ID of a Stage in a Process
  private String procInstanceId;  // Uniquely generated process instance id
  private String status;      // Status of the Stage
  private Timestamp startTime;      // Start Time of the Stage
  private Timestamp endTime;      // End Time of the Stage
  private String error;      // Error Description , if any, of the Stage
  private String hashsumFld;
  private int srcRowCount;    // Row Count in the Source
  private BigDecimal srcHashsumAmt;  // Hashsum Amount in the Source
  private int destRowCount;    // Row Count in the Destination
  private BigDecimal destHashsumAmt;  // Hashsum Amount in the Destination
  private int etErrorRowCount;
  private int uvErrorRowCount;
  private String toolProcId;    // Informatica BDM Process ID  // TODO
  private FRREmptyBizDateControl frrEmptyBizDateControl;
  private String actualBizDate;
  private String destDbName;
  private String destTableName;
  private String destErrorTableName;
  private int totalErrorRowCount;
  
  // EDF-203
  private BigInteger referencedFileCount; 
  private BigInteger referencedFileCopied;
  private BigInteger referencedFileMissing;



  
  @Override
  public String toString() {
  	return new ToStringBuilder(this).append("stageID", stageId)
  			                            .append("procInstanceID", procInstanceId)
  			                            .append("status", status)
  			                            .append("startTime", startTime)
  			                            .append("endTime", endTime)
                                    .append("error", error)
                                    .append("hashsumFld", hashsumFld)
                                    .append("srcRowCount", srcRowCount)
                                    .append("srcHashsumAmt", srcHashsumAmt) 
                                    .append("destRowCount", destRowCount)
                                    .append("destHashsumAmt", destHashsumAmt)
                                    .append("ETErrorRowCount", etErrorRowCount)
                                    .append("UVErrorRowCount", uvErrorRowCount)
                                    .append("toolProcID", toolProcId)
                                    .append("frrEmptyBizDateControl", frrEmptyBizDateControl)
                                    .append("actualBizDate", actualBizDate)
                                    .append("referencedFileCount", referencedFileCount)
                                    .append("referencedFileCopied", referencedFileCopied)
                                    .append("referencedFileMissing", referencedFileMissing)
                                    .append("destDbName", this.destDbName)
                                    .append("destTableName", this.destTableName)
                                    .append("destErrorTableName", this.destErrorTableName)
                                    .append("totalErrorRowCount", this.totalErrorRowCount)
                                    .toString();
  }
}
