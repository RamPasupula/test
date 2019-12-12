package com.uob.edag.model;

import java.math.BigDecimal;
import java.text.ParseException;

import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.builder.HashCodeBuilder;
import org.apache.commons.lang3.builder.ToStringBuilder;
import org.apache.log4j.Logger;

import com.uob.edag.exception.EDAGValidationException;
import com.uob.edag.utils.DateUtils;

/**
 * @Author      : Daya Venkatesan.
 * @Date of Creation: 10/24/2016
 * @Description   : The file defines the execution instance information for every process
 * 
 */

public class ControlModel implements Cloneable {
	
  protected Logger logger = Logger.getLogger(getClass());
	
  private String bizDate;
  private String sysDate;
  private String srcSystemCd;
  private String ctryCd;
  private String fileName;
  private int totalRecords;
  private int totalRecordsDecimalPrecision = 0;
  private boolean explicitDecimalPoint = true;
  private String hashSumCol;
  private String hashSumVal;
  private int hashSumValDecimalPrecision;
  private int totalRecordsTarget;
  private int totalRecordsStaging;
  private int totalErrRecordsTarget;
  private int etErrorRecordCount;
  private int uvErrorRecordCount;
  private String hashSumValTarget;
  private String targetDbName;
  private String targetTableName;
  private String targetErrorTableName;
  
  // TODO header and footer are stored here as part of EDF-149 implementation. Refactor this in the future
  private String headerLine;
  private String footerLine;
  
  public String getTargetDbName() {
  	return this.targetDbName;
  }
  
  public void setTargetDbName(String name) {
  	this.targetDbName = name;
  }
  
  public String getTargetTableName() {
  	return this.targetTableName;
  }
  
  public void setTargetTableName(String name) {
  	this.targetTableName = name;
  }
  
  public String getTargetErrorTableName() {
  	return this.targetErrorTableName;
  }
  
  public void setTargetErrorTableName(String name) {
  	this.targetErrorTableName = name;
  }
  
  public boolean equals(Object obj) {
  	boolean result = obj instanceof ControlModel;
  	
  	if (result) {
  		ControlModel o = (ControlModel) obj;
  		
  		result = StringUtils.equals(this.bizDate, o.bizDate) &&
  				     StringUtils.equals(this.sysDate, o.sysDate) &&
  				     StringUtils.equals(this.srcSystemCd, o.srcSystemCd) &&
  				     StringUtils.equals(this.ctryCd, o.ctryCd) &&
  				     StringUtils.equals(this.fileName, o.fileName) &&
  				     this.totalRecords == o.totalRecords &&
  				     this.totalRecordsDecimalPrecision == o.totalRecordsDecimalPrecision && 
  				     this.explicitDecimalPoint == o.explicitDecimalPoint &&
  				     StringUtils.equals(this.hashSumCol, o.hashSumCol) &&
  				     StringUtils.equals(this.hashSumVal, o.hashSumVal) &&
  				     this.hashSumValDecimalPrecision == o.hashSumValDecimalPrecision &&
  				     this.totalRecordsTarget == o.totalRecordsTarget &&
  				     this.totalErrRecordsTarget == o.totalErrRecordsTarget &&
  				     this.etErrorRecordCount == o.etErrorRecordCount &&
  				     this.uvErrorRecordCount == o.uvErrorRecordCount &&
  				     StringUtils.equals(this.targetDbName, o.targetDbName) &&
  				     StringUtils.equals(this.targetTableName, o.targetTableName) &&
  				     StringUtils.equals(this.targetErrorTableName, o.targetErrorTableName) &&
  				     StringUtils.equals(this.hashSumValTarget, o.hashSumValTarget);
  	}
  	
  	return result;
  }
  
  /**
   * This method is used to set the control information into the Control Model
   * @param model The model definition of the control field
   * @param ctrlModel The control model into which the information is to be set.
   * @throws EDAGValidationException 
   * @throws ParseException when there is an error setting the control information.
   */
  public void setControlInfoFromFieldModel(FieldModel model) throws EDAGValidationException {
    // Set Biz Date
    if (model.isBusinessDateField()) {
      String fieldValue = StringUtils.trimToNull(model.getFieldValue());
      String dateFormat = model.getDataFormat();
      String formattedFieldValue = DateUtils.getFormattedDate(fieldValue, dateFormat, true);
      setBizDate(formattedFieldValue);
      logger.debug("Control Information: Biz Date is: " + formattedFieldValue);
    }

    // Set Source System
    if (model.isSourceSystemField()) {
      String fieldValue = StringUtils.trimToNull(model.getFieldValue());
      setSrcSystemCd(fieldValue);
      logger.debug("Control Information: Src System is: " + fieldValue);
    }

    // Set Country
    if (model.isCountryField()) {
      String fieldValue = StringUtils.trimToNull(model.getFieldValue());
      setCtryCd(fieldValue);
      logger.debug("Control Information: Country is: " + fieldValue);
    }

    // Set File Name
    if (model.isFileNameField()) {
      String fieldValue = StringUtils.trimToNull(model.getFieldValue());
      setFileName(fieldValue);
      logger.debug("Control Information: File Name is: " + fieldValue);
    }

    // Set Number of Records
    if (model.isRecordCountField()) {
    	String val = StringUtils.trimToNull(model.getFieldValue());
    	BigDecimal fieldValue = new BigDecimal(0);
    	try {
    		if (val != null) {
    			fieldValue = new BigDecimal(val);
    		}
    		setTotalRecords(fieldValue.intValueExact());
    	} catch (ArithmeticException e) {
    	  throw new EDAGValidationException(EDAGValidationException.INVALID_VALUE, fieldValue.toPlainString(), "Record count should not have non-zero fractional part");
    	} catch (NumberFormatException e) {
    		throw new EDAGValidationException(EDAGValidationException.INVALID_VALUE, val, e.getMessage());
    	}
      setTotalRecordsDecimalPrecision(model.getDecimalPrecision());
      logger.debug("Control Information: Record Count is: " + fieldValue);
    }

    // Set Hashsum Column Name
    if (model.isHashsumColumnField()) {
      String fieldValue = StringUtils.trimToNull(model.getFieldValue());
      setHashSumCol(fieldValue);
      logger.debug("Control Information: Hash Column Name is: " + fieldValue);
    }

    // Set Hashsum Value Name
    if (model.isHashsumValueField()) {
      String fieldValue = StringUtils.isNotBlank(model.getFieldValue()) ? model.getFieldValue().trim() : "0";
      setHashSumVal(fieldValue);
      setHashSumValDecimalPrecision(model.getDecimalPrecision());
      logger.debug("Control Information: Hash Sum Value is: " + fieldValue);
    }

    // Set System Date
    if (model.isSystemDateField()) {
      String fieldValue = StringUtils.trimToNull(model.getFieldValue());
      String dateFormat = model.getDataFormat();
      String formattedFieldValue = DateUtils.getFormattedDate(fieldValue, dateFormat, true);
      setSysDate(formattedFieldValue);
      logger.debug("Control Information: System Date is: " + fieldValue);
    }
  }
  
  public int hashCode() {
  	return new HashCodeBuilder().append(this.bizDate)
  			                        .append(this.sysDate)
  			                        .append(this.srcSystemCd)
  			                        .append(this.ctryCd)
  			                        .append(this.fileName)
  			                        .append(this.totalRecords)
  			                        .append(this.totalRecordsDecimalPrecision)
  			                        .append(this.explicitDecimalPoint)
  			                        .append(this.hashSumCol)
  			                        .append(this.hashSumVal)
  			                        .append(this.hashSumValDecimalPrecision)
  			                        .append(this.totalRecordsTarget)
  			                        .append(this.totalErrRecordsTarget)
  			                        .append(this.etErrorRecordCount)
  			                        .append(this.uvErrorRecordCount)
  			                        .append(this.hashSumValTarget)
  			                        .append(this.targetDbName)
  			                        .append(this.targetTableName)
  			                        .append(this.targetErrorTableName)
  			                        .toHashCode();
  }
  
  public String getHeaderLine() {
  	return this.headerLine;
  }
  
  public void setHeaderLine(String line) {
  	this.headerLine = line;
  }
  
  public String getFooterLine() {
  	return this.footerLine;
  }
  
  public void setFooterLine(String line) {
  	this.footerLine = line;
  }
  
  public int getUVErrorRecordCount() {
  	return this.uvErrorRecordCount;
  }
  
  public void setUVErrorRecordCount(int count) {
  	this.uvErrorRecordCount = count;
  }
  
  public void setETErrorRecordCount(int count) {
  	this.etErrorRecordCount = count;
  }
  
  public int getETErrorRecordCount() {
  	return this.etErrorRecordCount;
  }

  public int getTotalErrRecordsTarget() {
    return totalErrRecordsTarget;
  }

  public void setTotalErrRecordsTarget(int totalErrRecordsTarget) {
    this.totalErrRecordsTarget = totalErrRecordsTarget;
  }

  public String getHashSumValTarget() {
    return hashSumValTarget;
  }

  public void setHashSumValTarget(String hashSumValTarget) {
    this.hashSumValTarget = hashSumValTarget;
  }

  public String getSysDate() {
    return sysDate;
  }

  public void setSysDate(String sysDate) {
    this.sysDate = sysDate;
  }

  public String getBizDate() {
    return bizDate;
  }

  public void setBizDate(String bizDate) {
    this.bizDate = bizDate;
  }

  public String getSrcSystemCd() {
    return srcSystemCd;
  }

  public void setSrcSystemCd(String srcSystemCd) {
    this.srcSystemCd = srcSystemCd;
  }

  public String getCtryCd() {
    return ctryCd;
  }

  public void setCtryCd(String ctryCd) {
    this.ctryCd = ctryCd;
  }

  public String getFileName() {
    return fileName;
  }

  public void setFileName(String fileName) {
    this.fileName = fileName;
  }

  public int getTotalRecords() {
  	return explicitDecimalPoint ? totalRecords 
  			                        : this.totalRecords / Double.valueOf(Math.pow(10, this.totalRecordsDecimalPrecision)).intValue(); 
  }

  public void setTotalRecords(int totalRecords) {
    this.totalRecords = totalRecords;
  }
  
  public int getTotalRecordsDecimalPrecision() {
  	return this.totalRecordsDecimalPrecision;
  }
  
  public boolean getExplicitDecimalPoint() {
  	return this.explicitDecimalPoint;
  }
  
  public void setExplicitDecimalPoint(boolean explicit) {
  	this.explicitDecimalPoint = explicit;
  }
  
  public void setTotalRecordsDecimalPrecision(int precision) {
  	this.totalRecordsDecimalPrecision = precision;
  }

  public String getHashSumCol() {
    return hashSumCol;
  }
  
  public String getNormalizedHashSumCol() {
  	return com.uob.edag.utils.StringUtils.normalizeForHive(getHashSumCol());
  }

  public void setHashSumCol(String hashSumCol) {
    this.hashSumCol = hashSumCol;
  }

  public String getHashSumVal() throws EDAGValidationException {
  	if (explicitDecimalPoint || StringUtils.isBlank(hashSumVal)) {
  		return hashSumVal;
  	} else {
  		BigDecimal bigValue = null;
  		try {
  			bigValue = new BigDecimal(hashSumVal);
  		} catch (NumberFormatException e) {
  			logger.warn("Hash sum value (" + bigValue + ") is not numeric: " + e.getMessage());
  			return hashSumVal;
  		}
  		
//  		try {
//  			// check if the value is integer, if it's not integer, throw exception
//  			bigValue.longValueExact();
//  		} catch (ArithmeticException e) {
//  			throw new EDAGValidationException(EDAGValidationException.INVALID_VALUE, hashSumVal, "Decimal point in hash sum value is declared as implicit, it cannot have explicit decimal point");
//  		}
  		// SIT 1404 fixes: if implicit hashsum contains dot then throw exception as implicit hashsum should not includes dot.
  	    if (hashSumVal.contains(".")) {
  	    	throw new EDAGValidationException(EDAGValidationException.INVALID_VALUE, hashSumVal, "Decimal point in hash sum value is declared as implicit, it cannot have explicit decimal point");
  	    }
  		
  		return bigValue.scaleByPowerOfTen(0 - this.hashSumValDecimalPrecision).toPlainString();
  	}
  }

  public void setHashSumVal(String hashSumVal) {
    this.hashSumVal = hashSumVal;
  }
  
  public int getHashSumValDecimalPrecision() {
  	return this.hashSumValDecimalPrecision;
  }
  
  public void setHashSumValDecimalPrecision(int precision) {
  	this.hashSumValDecimalPrecision = precision;
  }

  @Override
  public String toString() {
    return new ToStringBuilder(this).append("bizDate", bizDate)
    		                            .append("sysDate", sysDate)
    		                            .append("srcSystemCd", srcSystemCd)
    		                            .append("ctryCd", ctryCd)
    		                            .append("fileName", fileName)
    		                            .append("totalRecords", totalRecords)
    		                            .append("totalRecordsDecimalPrecision", totalRecordsDecimalPrecision)
    		                            .append("explicitDecimalPoint", explicitDecimalPoint)
    		                            .append("hashSumCol", hashSumCol)
    		                            .append("hashSumVal", hashSumVal)
    		                            .append("hashSumValDecimalPrecision", hashSumValDecimalPrecision)
                                    .append("totalRecordsTarget", totalRecordsTarget)
                                    .append("totalErrRecordsTarget", totalErrRecordsTarget)
                                    .append("etErrorRecordCount", etErrorRecordCount)
                                    .append("uvErrorRecordCount", uvErrorRecordCount)
                                    .append("hashSumValTarget", hashSumValTarget)
                                    .append("targetDbName", this.targetDbName)
                                    .append("targetTableName", this.targetTableName)
                                    .append("targetErrorTableName", this.targetErrorTableName)
                                    .append("totalRecordsStaging", this.totalRecordsStaging)
                                    .toString();
  }

  public int getTotalRecordsTarget() {
    return totalRecordsTarget;
  }

  public void setTotalRecordsTarget(int totalRecordsTarget) {
    this.totalRecordsTarget = totalRecordsTarget;
  }
  
  public int getTotalRecordsStaging() {
	return totalRecordsStaging;
  }

  public void setTotalRecordsStaging(int totalRecordsStaging) {
	this.totalRecordsStaging = totalRecordsStaging;
  }

  /** 
   * This method is used to clone the Control Model object into a new object instance.
   */
  public ControlModel clone() {
  	ControlModel clone = new ControlModel();
  	clone.bizDate = this.bizDate;
  	clone.ctryCd = this.ctryCd;
  	clone.fileName = this.fileName;
  	clone.hashSumCol = this.hashSumCol;
  	clone.hashSumVal = this.hashSumVal;
  	clone.hashSumValDecimalPrecision = this.hashSumValDecimalPrecision;
  	clone.hashSumValTarget = this.hashSumValTarget;
  	clone.srcSystemCd = this.srcSystemCd;
  	clone.sysDate = this.sysDate;
  	clone.totalErrRecordsTarget = this.totalErrRecordsTarget;
  	clone.etErrorRecordCount = this.etErrorRecordCount;
  	clone.uvErrorRecordCount = this.uvErrorRecordCount;
  	clone.totalRecords = this.totalRecords;
  	clone.totalRecordsDecimalPrecision = this.totalRecordsDecimalPrecision;
  	clone.explicitDecimalPoint = this.explicitDecimalPoint;
  	clone.totalRecordsTarget = this.totalRecordsTarget;
  	clone.targetDbName = this.targetDbName;
  	clone.targetTableName = this.targetTableName;
  	clone.targetErrorTableName = this.targetErrorTableName;
  	clone.totalRecordsStaging = this.totalRecordsStaging;
  	return clone;
  }
  
}
