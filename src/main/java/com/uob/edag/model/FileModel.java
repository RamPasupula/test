package com.uob.edag.model;

import org.apache.commons.lang3.builder.ToStringBuilder;
import org.apache.log4j.Logger;
import org.codehaus.jackson.annotate.JsonIgnoreProperties;

import com.uob.edag.constants.UobConstants;
import com.uob.edag.utils.UobUtils;

import java.text.MessageFormat;
import java.util.List;


/**
 * @Author : Daya Venkatesan
 * @Date of Creation: 10/21/2016
 * @Description : The file defines the File Source format. This file will be
 *              consumed in the Data Ingestion process. This file contains all
 *              the properties which will help in consuming the input file.
 * 
 */

@JsonIgnoreProperties(ignoreUnknown = true)
public class FileModel extends AbstractFileModel {
	
	private static final MessageFormat INSERT_CONTROL_FILE_DETAIL_TEMPLATE = new MessageFormat(
			UobUtils.ltrim("INSERT INTO EDAG_CONTROL_FILE_DETAIL(CTRL_FILE_ID, CTRL_FILE_DIR_TXT, CTRL_FILE_NM, ") +
	    UobUtils.ltrim("                                     CTRL_FILE_LAYOUT_CD, CTRL_FILE_COL_DELIM_TXT, CTRL_FILE_TXT_DELIM_TXT, CTRL_FILE_EXPLICIT_DEC_POINT) ") +
      UobUtils.ltrim("VALUES({0}, ''{1}'', ''{2}'', {3}, {4}, {5}, {6}); ")
	);
	
	private static final MessageFormat INSERT_FILE_DETAIL_TEMPLATE = new MessageFormat(
			UobUtils.ltrim("INSERT INTO EDAG_FILE_DETAIL(FILE_ID, PROC_ID, DIR_NM, FILE_NM, FILE_EXTN_NM, CTRL_FILE_ID, ") +
      UobUtils.ltrim("                             CTRL_INFO_CD, FILE_TYPE_CD, FILE_LAYOUT_CD, FILE_COL_DELIM_TXT, ") +
      UobUtils.ltrim("                             FILE_TXT_DELIM_TXT, HDR_LINE_NUM, FTR_LINE_NUM, ACHV_DIR_NM, FILE_EXPLICIT_DEC_POINT, ") +
      UobUtils.ltrim("                             CRT_DT, CRT_USR_NM, USE_SPARK_BASED_INGESTION) ") +
      UobUtils.ltrim("VALUES({0, number, #}, ''{1}'', ''{2}'', ''{3}'', ''{4}'', {5}, ''{6}'', ''{7}'', ''{8}'', ''{9}'', ''{10}'', {11, number, #}, {12, number, #}, ") +
      UobUtils.ltrim("       ''{13}'', {14}, DEFAULT, ''{15}'', ''{16}''); ")
	);
	
	protected Logger logger = Logger.getLogger(getClass());
	
	public String getInsertControlFileDetailSql() {
		String result = INSERT_CONTROL_FILE_DETAIL_TEMPLATE.format(new Object[] {getControlFileId(), 
    																												 								 getControlFileDir(),
    																												 								 getControlFileName(),
    																												 								 UobUtils.quoteValue(getControlFileLayoutCd()),
    																												 								 UobUtils.quoteValue(getControlFileColumnDelimiter()),
    																												 								 UobUtils.quoteValue(getControlFileTextDelimiter()),
    																												 								 UobUtils.quoteValue(getCtrlFileExplicitDecimalPoint())});
		logger.debug("Insert control file detail statement: " + result);
		return result;
	}
	
	public String getInsertFileDetailSql() {
		String result = INSERT_FILE_DETAIL_TEMPLATE.format(new Object[] {getFileId(), getProcessId(),
    																												 				 getSourceDirectory(), getSourceFileName(),
    																												 				 getSourceFileExtn(), getControlFileId(),
    																												 				 getControlInfo(), getSourceFileTypeCd(),
    																												 				 getSourceFileLayoutCd(), getColumnDelimiter(),
    																												 				 getTextDelimiter(), getHeaderLines(),
    																												 				 getTrailerLines(), getSourceArchivalDir(), 
    																												 				 UobUtils.quoteValue(getExplicitDecimalPoint()),
    																												 				 UobConstants.SYS_USER_NAME,
    																												 				 UobConstants.N});
		logger.debug("Insert file detail statement: " + result);
		return result;
	}
	
  public String getSourceDirectory() {
    return sourceDirectory;
  }

  public void setSourceDirectory(String sourceDirectory) {
    this.sourceDirectory = sourceDirectory == null ? null : sourceDirectory.trim();
  }

  public String getSourceFileName() {
    return sourceFileName;
  }

  public void setSourceFileName(String sourceFileName) {
    this.sourceFileName = sourceFileName == null ? null : sourceFileName.trim();
  }

  public String getSourceFileExtn() {
    return sourceFileExtn;
  }

  public void setSourceFileExtn(String sourceFileExtn) {
    this.sourceFileExtn = sourceFileExtn == null ? null : sourceFileExtn.trim();
  }

  public String getSourceFileTypeCd() {
    return sourceFileTypeCd;
  }

  public void setSourceFileTypeId(String sourceFileTypeCd) {
    this.sourceFileTypeCd = sourceFileTypeCd == null ? null : sourceFileTypeCd.trim();
  }

  public String getSourceFileLayoutCd() {
    return sourceFileLayoutCd;
  }

  public void setSourceFileLayoutCd(String sourceFileLayoutCd) {
    this.sourceFileLayoutCd = sourceFileLayoutCd == null ? null : sourceFileLayoutCd.trim();
  }

  public String getColumnDelimiter() {
    return columnDelimiter;
  }

  public void setColumnDelimiter(String columnDelimiter) {
    this.columnDelimiter = columnDelimiter;
  }

  public String getTextDelimiter() {
    return textDelimiter;
  }

  public void setTextDelimiter(String textDelimiter) {
    this.textDelimiter = textDelimiter; 
  }

  public int getHeaderLines() {
    return headerLines;
  }

  public void setHeaderLines(int headerLines) {
    this.headerLines = headerLines;
  }

  public int getTrailerLines() {
    return trailerLines;
  }

  public void setTrailerLines(int trailerLines) {
    this.trailerLines = trailerLines;
  }

  public String getSourceArchivalDir() {
    return sourceArchivalDir;
  }

  public void setSourceArchivalDir(String sourceArchivalDir) {
    this.sourceArchivalDir = sourceArchivalDir == null ? null : sourceArchivalDir.trim();
  }
  
  public String getExplicitDecimalPoint() {
  	return this.explicitDecimalPoint == null ? null :
  			   this.explicitDecimalPoint.booleanValue() ? UobConstants.Y : UobConstants.N;  
  }
  
  public void setExplicitDecimalPoint(String explicit) {
  	this.explicitDecimalPoint = explicit == null ? null : UobConstants.Y.equalsIgnoreCase(explicit);
  }

  public String getControlFileDir() {
    return controlFileDir;
  }

  public void setControlFileDir(String controlFileDir) {
    this.controlFileDir = controlFileDir == null ? null : controlFileDir.trim();
  }

  public String getControlFileName() {
    return controlFileName;
  }

  public void setControlFileName(String controlFileName) {
    this.controlFileName = controlFileName == null ? null : controlFileName.trim();
  }

  public String getControlFileExtn() {
    return controlFileExtn;
  }

  public void setControlFileExtn(String controlFileExtn) {
    this.controlFileExtn = controlFileExtn == null ? null : controlFileExtn.trim();
  }

  public String getControlFileId() {
    return controlFileId;
  }

  public void setControlFileId(String controlFileId) {
    this.controlFileId = controlFileId == null ? null : controlFileId.trim();
  }

  public String getControlInfo() {
    return controlInfo;
  }

  public void setControlInfo(String controlInfo) {
    this.controlInfo = controlInfo == null ? null : controlInfo.trim();
  }

  public int getFileId() {
    return fileId;
  }

  public void setFileId(int fileId) {
    this.fileId = fileId;
  }

  public List<FieldModel> getSrcFieldInfo() {
    return srcFieldInfo;
  }

  public void setSrcFieldInfo(List<FieldModel> srcFieldInfo) {
    this.srcFieldInfo = srcFieldInfo;
  }

  public String getProcessId() {
    return processId;
  }

  public void setProcessId(String processId) {
    this.processId = processId == null ? null : processId.trim();
  }

  public List<FieldModel> getCtrlInfo() {
    return ctrlInfo;
  }
  
  public FieldModel getHashSumField() {
  	for (FieldModel fieldModel : this.getSrcFieldInfo()) {
  		if (fieldModel.isHashSumField()) {
  			return fieldModel;
  		}
  	}
  	
  	for (FieldModel controlFieldModel : this.ctrlInfo) {
  		if (controlFieldModel.isHashSumField()) {
  			return controlFieldModel;
  		}
  	}

  	return null;
  }

  public void setCtrlInfo(List<FieldModel> ctrlInfo) {
    this.ctrlInfo = ctrlInfo;
  }

  public String getUserNm() {
    return userNm;
  }

  public void setUserNm(String userNm) {
    this.userNm = userNm == null ? null : userNm.trim();
  }
  
  public String getControlFileLayoutCd() {
  	return this.controlFileLayoutCd;
  }
  
  public void setControlFileLayoutCd(String layout) {
  	this.controlFileLayoutCd = layout == null ? null : layout.trim();
  }
  
  public String getControlFileColumnDelimiter() {
  	return this.controlFileColumnDelimiter;
  }
  
  public void setControlFileColumnDelimiter(String delim) {
  	this.controlFileColumnDelimiter = delim == null ? null : delim.trim();
  }
  
  public String getControlFileTextDelimiter() {
  	return this.controlFileTextDelimiter;
  }
  
  public void setControlFileTextDelimiter(String delim) {
  	this.controlFileTextDelimiter = delim == null ? null : delim.trim();
  }
  
  public String getCtrlFileExplicitDecimalPoint() {
  	return this.controlFileExplicitDecimalPoint == null ? null :
  		     this.controlFileExplicitDecimalPoint.booleanValue() ? UobConstants.Y : UobConstants.N;
  }
  
  public void setCtrlFileExplicitDecimalPoint(String explicit) {
  	this.controlFileExplicitDecimalPoint = explicit == null ? null : UobConstants.Y.equalsIgnoreCase(explicit);
  }
  
  public String getReferenceFolderName() {
	return referenceFolderName;
  }
	
  public void setReferenceFolderName(String referenceFolderName) {
	this.referenceFolderName = referenceFolderName;
  }
	
  public String getHdfsTargetFolder() {
	return hdfsTargetFolder;
  }
	
  public void setHdfsTargetFolder(String hdfsTargetFolder) {
	this.hdfsTargetFolder = hdfsTargetFolder;
  }
	
  public String getHdfsPathSuffix() {
	return hdfsPathSuffix;
  }
	
  public void setHdfsPathSuffix(String hdfsPathSuffix) {
    this.hdfsPathSuffix = hdfsPathSuffix;
  }

  public boolean getIsWebLogsProcessing() {
	  return this.isWebLogsProcessing;
  }
  
  public void setIsWebLogsProcessing(boolean isWebLogProcessing) {
	this.isWebLogsProcessing = isWebLogProcessing;	
  }
  
  public boolean getIsUseSparkBasedIngestion() {
	return isUseSparkBasedIngestion;
  }

  public void setUseSparkBasedIngestion(String isUseSparkBasedIngestion) {
	this.isUseSparkBasedIngestion = UobConstants.Y.equalsIgnoreCase(isUseSparkBasedIngestion);
  }

  /**
   * This method is used to print the File Model object as a plain text string.
   */
  public String toString() {
    return new ToStringBuilder(this).append("processId", processId)
    		                            .append("fileID", fileId)
    		                            .append("sourceDirectory", sourceDirectory)
    		                            .append("sourceFileName", sourceFileName)
    		                            .append("sourceFileExtn", sourceFileExtn)
    		                            .append("sourceFileTypeCd", sourceFileTypeCd)
    		                            .append("sourceFileLayoutID", sourceFileLayoutCd)
    		                            .append("columnDelimiter", columnDelimiter)
    		                            .append("textDelimiter", textDelimiter)
    		                            .append("headerLines", headerLines)
    		                            .append("trailerLines", trailerLines)
    		                            .append("sourceArchivalDir", sourceArchivalDir)
    		                            .append("explicitDecimalPoint", explicitDecimalPoint)
    		                            .append("controlFileID", controlFileId) 
                                    .append("controlFileDir", controlFileDir)
                                    .append("controlFileName", controlFileName)
                                    .append("controlFileExtn", controlFileExtn)
                                    .append("controlFileLayoutCd", controlFileLayoutCd)
                                    .append("controlFileColumnDelimiter", controlFileColumnDelimiter)
                                    .append("controlFileTextDelimiter", controlFileTextDelimiter)
                                    .append("controlFileExplicitDecimalPoint", controlFileExplicitDecimalPoint)
                                    .append("controlInfo", controlInfo)
                                    .append("srcFieldInfo", srcFieldInfo)
                                    .append("ctrlInfo", ctrlInfo)
                                    .append("userNm", userNm)
                                    .append("referenceFolderName", referenceFolderName)
                                    .append("hdfsTargetFolder", hdfsTargetFolder)
                                    .append("hdfsPathSuffix", hdfsPathSuffix)
                                    .append("isUseSparkBasedIngestion", isUseSparkBasedIngestion)
                                    .toString();
  }

  private String processId; // Uniquely generated process id corresponding to this model
  private int fileId; // Uniquely generated file ID
  private String sourceDirectory; // Directory in landing area in which source file is copied
  private String sourceFileName; // Name of the source file in landing area
  private String sourceFileExtn; // Extension of the source file in landing area
  private String sourceFileTypeCd; // Types of Source File - Master, Mapping, Transaction
  // , Parameter, etc; Refer Constants for mapping
  private String sourceFileLayoutCd; // Layout of the Source File - Fixed, Delimited, etc; Refer 
  // Constants for mapping
  private String columnDelimiter; // Delimiter between columns in the source file
  private String textDelimiter; // Delimiter to enclose the column values - To handle scenarios
  // when column delimiter is a part of the value
  private int headerLines; // Number of header lines in the source file
  private int trailerLines; // Number of trailer lines in the source file
  private String sourceArchivalDir; // Directory in which the source file will 
  // be archived after processing
  private Boolean explicitDecimalPoint = null;
  private String controlFileId; // Uniquely generated ID for control file
  private String controlFileDir; // Directory in which the control file will be copied
  private String controlFileName; // Name of the source file in landing area
  private String controlFileExtn; // Extension of the source file in landing area
  private String controlInfo; // Whether control Information is going to be in separate control file
  private String controlFileLayoutCd;
  private String controlFileColumnDelimiter;
  private String controlFileTextDelimiter;
  private Boolean controlFileExplicitDecimalPoint = null;
  // or in Header/Footer in the same file
  private List<FieldModel> srcFieldInfo; // Source Field Definitions
  private List<FieldModel> ctrlInfo; // Control Fields Definitions
  private String userNm; // User Name
  //EDF-203
  private String referenceFolderName;
  private String hdfsTargetFolder;
  private String hdfsPathSuffix;
  // PIB/GEB WebLogs Processing
  private boolean isWebLogsProcessing = Boolean.FALSE;
  // For Spark Based Ingestion
  private boolean isUseSparkBasedIngestion = Boolean.TRUE;
  

}