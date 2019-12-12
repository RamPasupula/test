package com.uob.edag.model;

import java.util.Date;
import java.util.Map;

import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.builder.ToStringBuilder;
import org.apache.log4j.Logger;

import com.uob.edag.constants.UobConstants;
import com.uob.edag.utils.PropertyLoader;

public class DestModel implements HadoopModel, ExportModel {
	
	public static final String DEFAULT_REFERENCED_FILE_TARGET_FOLDER = PropertyLoader.getProperty("ReferencedFile.Target.Folder");
	
	private static final String DDS_HIVE_DUMP_TBL_NM_SUFFIX = PropertyLoader.getProperty(UobConstants.DDS_HIVE_DUMP_TBL_NM_SUFFIX) == null ? "_dump" : PropertyLoader.getProperty(UobConstants.DDS_HIVE_DUMP_TBL_NM_SUFFIX);
	private static final String ENVIRONMENT = PropertyLoader.getProperty(UobConstants.ENVIRONMENT);
  private static final String ENVIRONMENT_NUM = PropertyLoader.getProperty(UobConstants.ENVIRONMENT_NUM);
  private static final String DUMP_STAGING_DIR_PATTERN = PropertyLoader.getProperty(UobConstants.TIER_1_DUMP_HDFS_PATH);
  private static final String DUMP_STAGING_HIVE_LOCATION_PATTERN = PropertyLoader.getProperty(UobConstants.TIER_1_HIVE_DUMP_LOCATION);
	
	public enum Type {HADOOP, EXPORT};
	
	protected Logger logger = Logger.getLogger(getClass());

	private Type type;
	private String processId;
	private String userNm;
	
	// Hadoop part
	private String hadoopDir;        // Destination Directory on Hadoop
  private String hadoopFormatCd = UobConstants.PARQUET_CD;        // Format of the file in Hadoop - Parquet, ORC,etc
  private String hadoopCompressCd = UobConstants.SNAPPY_CD;      // Compression of the file in Hadoop - Snappy, Zlib, etc
  private String hiveDbName;        // Name of the Hive Schema
  private String hiveTableName;      // Name of the Hive Table
  private String hivePartition;      // Hive Partition Key Name
  private double hiveErrorThreshold;    // Error Threshold in the Hive Table
  private String loadTypeCd;          // Load Type - Full Replace, Append, etc
  private String stagingDir;        // Staging Directory on Hadoop
  private String stagingDbName;      // Name of the Hive Staging Schema
  private String stagingTableName;    // Name of the Hive Staging Table
  private String stagingHivePartition;  // Hive Staging Table Partition Key Name
  private String stagingHiveLocation;    // Hive Staging Table Location
  private String dumpStagingHiveLocation; // EDF-209
  private Map<Integer, FieldModel> destFieldInfo;  // Hadoop Field Definitions for usage in Hive
  private String hadoopQueueName;      // Queue Name in Hadoop
  private String bdmConnName;          // BDM Connection in Hadoop
  private Date createdDatetime;
  private String createdBy;
  private Date lastUpdatedDatetime;
  private String lastUpdatedBy;
  
  // Export part
  private String srcDbName;
  private String srcTblName;
  private String tgtDirName;
  private String tgtFileName;
  private String tgtFileExtn;
  private String tgtColDelim;
  private String tgtTxtDelim;
  private String ctrlFileName;
  private String ctrlFileExtn;
  
  // Compaction Part
  private String targetTablePartitionInfo;

  public DestModel() {
  	// default constructor
  }
  
  public void setType(String type) {
  	this.type = Type.valueOf(type);
  }
  
  public DestModel(Type type) {
  	this.type = type;
  }
	
	public String getProcessId() {
    return processId;
  }

  public void setProcessId(String processId) {
    this.processId = processId;
  }
  
  public String getUserNm() {
    return userNm;
  }

  public void setUserNm(String userNm) {
    this.userNm = userNm;
  }
  
  public String toString() {
  	ToStringBuilder sb = new ToStringBuilder(this).append("processId", processId)
			  																				  .append("userNm", userNm);
  	
  	if (this.type == Type.HADOOP) {
  		sb.append("hadoopDir", hadoopDir)
      	.append("hadoopFormatCd", hadoopFormatCd)
      	.append("hadoopCompressCd", hadoopCompressCd)
      	.append("hiveDbName", hiveDbName)
      	.append("hiveTableName", hiveTableName)
      	.append("hivePartition", hivePartition)
      	.append("hiveErrorThreshold", hiveErrorThreshold)
      	.append("loadTypeCd", loadTypeCd)
      	.append("stagingDir", stagingDir)
      	.append("stagingDbName", stagingDbName)
      	.append("stagingTableName", stagingTableName)
      	.append("stagingHivePartition", stagingHivePartition)
      	.append("stagingHiveLocation", stagingHiveLocation)
      	.append("dumpStagingHiveLocation", this.dumpStagingHiveLocation)
      	.append("destFieldInfo", destFieldInfo)
      	.append("hadoopQueueName", hadoopQueueName)
      	.append("bdmConnName", bdmConnName)
      	.append("createdBy", createdBy)
      	.append("createdDatetime", createdDatetime)
      	.append("lastUpdatedBy", lastUpdatedBy)
      	.append("lastUpdatedDatetime", lastUpdatedDatetime);
  	} else if (this.type == Type.EXPORT) {
  		sb.append("srcDbName", srcDbName)
      	.append("srcTblName", srcTblName)
      	.append("tgtDirName", tgtDirName)
      	.append("tgtFileName",  tgtFileName)
      	.append("tgtFileExtn", tgtFileExtn)
      	.append("tgtColDelim", tgtColDelim)
      	.append("tgtTxtDelim", tgtTxtDelim)
      	.append("ctrlFileName", ctrlFileName)
      	.append("ctrlFileExtn", ctrlFileExtn);
  	}
  			            
  	return sb.toString();
  }
  
  public String getInsertLoadProcessSql() {
		String hivePartition = getHivePartition().replaceAll("'", "''");
    String stagingHivePartition = getStagingHivePartition().replaceAll("'", "''");
    
    String result = INSERT_LOAD_PROCESS_TEMPLATE.format(new Object[] {getProcessId(), getHadoopFormatCd(),
				 																														  getHadoopCompressCd(), getHiveDbName(),
				 																														  getHiveTableName(), hivePartition,
				 																														  getLoadTypeCd(), getStagingDir(),
				 																														  getStagingDbName(), getStagingTableName(),
				 																														  stagingHivePartition, getHiveErrorThreshold(),
				 																														  UobConstants.SYS_USER_NAME});
    logger.debug("Insert load process statement: " + result);
    return result;
	}
  
  public String getHadoopDir() {
    return hadoopDir;
  }
  
  public void setHadoopDir(String hadoopDir) {
    this.hadoopDir = hadoopDir == null ? null : hadoopDir.trim();
  }
  
  public String getHadoopFormatCd() {
    return hadoopFormatCd;
  }
  
  public void setHadoopFormatCd(String hadoopFormatCd) {
    this.hadoopFormatCd = hadoopFormatCd == null ? null : hadoopFormatCd.trim();
  }
  
  public String getHadoopCompressCd() {
    return hadoopCompressCd;
  }
  
  public void setHadoopCompressCd(String hadoopCompressCd) {
    this.hadoopCompressCd = hadoopCompressCd == null ? null : hadoopCompressCd.trim();
  }
  
  public String getHiveDbName() {
    return hiveDbName;
  }
  
  public void setHiveDbName(String hiveDbName) {
    this.hiveDbName = hiveDbName == null ? null : hiveDbName.trim();
  }
  
  public String getHiveTableName() {
    return hiveTableName;
  }
  
  public void setHiveTableName(String hiveTableName) {
    this.hiveTableName = hiveTableName == null ? null : hiveTableName.trim();
  }
  
  public String getHiveDumpTableName() {
  	return getHiveTableName() + DDS_HIVE_DUMP_TBL_NM_SUFFIX;
  }
  
  public String getLoadTypeCd() {
    return loadTypeCd;
  }
  
  public void setLoadTypeCd(String loadTypeCd) {
    this.loadTypeCd = loadTypeCd == null ? null : loadTypeCd.trim();
  }
  
  public String getStagingDir() {
    return stagingDir;
  }
  
  public void setStagingDir(String stagingDir) {
    this.stagingDir = stagingDir == null ? null : stagingDir.trim();
  }
  
  public String getDumpStagingDir(ProcessModel procModel, ProcessInstanceModel procInstanceModel) {
  	return DUMP_STAGING_DIR_PATTERN.replace(UobConstants.SRC_SYS_PARAM, procModel.getSrcSysCd())
	       		 						           .replace(UobConstants.ENVIRONMENT_STR_PARAM, ENVIRONMENT + ENVIRONMENT_NUM)
	       													 .replace(UobConstants.ENVIRONMENT, ENVIRONMENT)
	       													 .replace(UobConstants.ENV_NUM_PARAM, ENVIRONMENT_NUM)
	       													 .replace(UobConstants.FREQ_PARAM, procModel.getProcFreqCd())
	       													 .replace(UobConstants.FILENM_PARAM, procModel.getSrcInfo().getSourceFileName())
	       													 .replace(UobConstants.COUNTRY_PARAM, procInstanceModel.getCountryCd())
	       													 .replace(UobConstants.BIZ_DATE_PARAM, procInstanceModel.getBizDate());		
  }
  
  public String getDumpStagingHiveLocation(ProcessModel procModel, ProcessInstanceModel procInstanceModel) {
    return DUMP_STAGING_HIVE_LOCATION_PATTERN.replace(UobConstants.SRC_SYS_PARAM, procModel.getSrcSysCd())
    																				 .replace(UobConstants.ENVIRONMENT_STR_PARAM, ENVIRONMENT + ENVIRONMENT_NUM)
    		                                     .replace(UobConstants.ENV_PARAM, ENVIRONMENT)
				 																		 .replace(UobConstants.ENV_NUM_PARAM, ENVIRONMENT_NUM)
				 																		 .replace(UobConstants.FREQ_PARAM, procModel.getProcFreqCd())
				 																		 .replace(UobConstants.FILENM_PARAM, procModel.getSrcInfo().getSourceFileName());
  }
  
  public String getStagingDbName() {
    return stagingDbName;
  }
  
  public void setStagingDbName(String stagingDbName) {
    this.stagingDbName = stagingDbName == null ? null : stagingDbName.trim();
  }
  
  public String getDumpStagingTableName() {
  	return getStagingTableName() + "_dump";
  }
  
  public String getStagingTableName() {
    return stagingTableName;
  }
  
  public String getStagingErrorTableName() {
  	return getStagingTableName() + "_err";
  }
  
  public void setStagingTableName(String stagingTableName) {
    this.stagingTableName = stagingTableName == null ? null : stagingTableName.trim();
  }
  
  public String getHivePartition() {
    return hivePartition;
  }
  
  public void setHivePartition(String hivePartition) {
    this.hivePartition = hivePartition == null ? null : hivePartition.trim();
  }
  
  public String getStagingHivePartition() {
    return stagingHivePartition;
  }
  
  public void setStagingHivePartition(String stagingHivePartition) {
    this.stagingHivePartition = stagingHivePartition == null ? null : stagingHivePartition.trim();
  }
  
  public Map<Integer, FieldModel> getDestFieldInfo() {
    return destFieldInfo;
  }
  
  public void setDestFieldInfo(Map<Integer, FieldModel> destFieldInfo) {
    this.destFieldInfo = destFieldInfo;
  }
  
  public String getStagingHiveLocation() {
    return stagingHiveLocation;
  }
  
  public void setStagingHiveLocation(String stagingHiveLocation) {
    this.stagingHiveLocation = stagingHiveLocation == null ? null : stagingHiveLocation.trim();
  }
  
  public String getDumpStagingHiveLocation() {
		return dumpStagingHiveLocation;
  }
  
  public double getHiveErrorThreshold() {
    return hiveErrorThreshold;
  }
  
  public void setHiveErrorThreshold(double hiveErrorThreshold) {
    this.hiveErrorThreshold = hiveErrorThreshold;
  }
  
  public String getHadoopQueueName() {
    return hadoopQueueName;
  }
  
  public void setHadoopQueueName(String hadoopQueueName) {
    this.hadoopQueueName = hadoopQueueName == null ? null : hadoopQueueName.trim();
  }
  
  public String getBdmConnName() {
    return bdmConnName;
  }
  
  public void setBdmConnName(String bdmConnName) {
    this.bdmConnName = bdmConnName == null ? null : bdmConnName.trim();
  }
  
  public Date getCreatedDatetime() {
  	return this.createdDatetime;
  }
  
  public void setCreatedDatetime(Date dt) {
  	this.createdDatetime = dt;
  }
  
  public String getCreatedBy() {
  	return this.createdBy;
  }
  
  public void setCreatedBy(String by) {
  	this.createdBy = by;
  }
  
  public Date getLastUpdatedDatetime() {
  	return this.lastUpdatedDatetime;
  }
  
  public void setLastUpdatedDatetime(Date dt) {
  	this.lastUpdatedDatetime = dt;
  }
  
  public String getLastUpdatedBy() {
  	return this.lastUpdatedBy;
  }
  
  public void setLastUpdatedBy(String by) {
  	this.lastUpdatedBy = by;
  }
  
  public String getInsertExportProcessSql() {
  	String result = INSERT_EXPORT_PROCESS_TEMPLATE.format(new Object[] {getProcessId(), getSrcDbName(),
    																												 					  getSrcTblName(), getTgtDirName(),
    																												 					  getTgtFileName(), getTgtFileExtn(),
    																												 					  getTgtColDelim(), getTgtTxtDelim(),
    																												 					  getCtrlFileName(), getCtrlFileExtn(),
    																												 					  UobConstants.SYS_USER_NAME});
  	logger.debug("Insert export process template: " + result);
  	return result;
  }
  
  public String getSrcDbName() {
    return srcDbName;
  }
  
  public void setSrcDbName(String srcDbName) {
    this.srcDbName = srcDbName == null ? null : srcDbName.trim();
  }
  
  public String getSrcTblName() {
    return srcTblName;
  }
  
  public void setSrcTblName(String srcTblName) {
    this.srcTblName = srcTblName == null ? null : srcTblName.trim();
  }
  
  public String getTgtDirName() {
    return tgtDirName;
  }
  
  public void setTgtDirName(String tgtDirName) {
    this.tgtDirName = tgtDirName == null ? null : tgtDirName.trim();
  }
  
  public String getTgtFileName() {
    return tgtFileName;
  }
  
  public void setTgtFileName(String tgtFileName) {
    this.tgtFileName = tgtFileName == null ? null : tgtFileName.trim();
  }
  
  public String getTgtFileExtn() {
    return tgtFileExtn;
  }
  
  public void setTgtFileExtn(String tgtFileExtn) {
    this.tgtFileExtn = tgtFileExtn == null ? null : tgtFileExtn.trim();
  }
  
  public String getTgtColDelim() {
    return tgtColDelim;
  }
  
  public void setTgtColDelim(String tgtColDelim) {
    this.tgtColDelim = tgtColDelim == null ? null : tgtColDelim.trim();
  }
  
  public String getTgtTxtDelim() {
    return tgtTxtDelim;
  }
  
  public void setTgtTxtDelim(String tgtTxtDelim) {
    this.tgtTxtDelim = tgtTxtDelim == null ? null : tgtTxtDelim.trim();
  }
  
  public String getCtrlFileName() {
    return ctrlFileName;
  }
  
  public void setCtrlFileName(String ctrlFileName) {
    this.ctrlFileName = ctrlFileName == null ? null : ctrlFileName.trim();
  }
  
  public String getCtrlFileExtn() {
    return ctrlFileExtn;
  }
  
  public void setCtrlFileExtn(String ctrlFileExtn) {
    this.ctrlFileExtn = ctrlFileExtn == null ? null : ctrlFileExtn.trim();
  }
  
  public void setTargetTablePartitionString(String targetTablePartitionString) {
	  this.targetTablePartitionInfo = targetTablePartitionString;
  }
  
  public String getTargetTablePartitionInfo() {
	  return this.targetTablePartitionInfo;
  }

	@Override
	public boolean hasField(String fieldName) {
		boolean result = false;
		
		if (StringUtils.isNotBlank(fieldName)) {
			for (FieldModel fieldModel : this.getDestFieldInfo().values()) {
				if (fieldName.equals(fieldModel.getFieldName())) {
					result = true;
					break;
				}
			}
		}
		
		return result;
	}

	@Override
	public boolean hasNormalizedField(String normalizedFieldName) {
		boolean result = false;
		
		if (StringUtils.isNotBlank(normalizedFieldName)) {
			for (FieldModel fieldModel : this.getDestFieldInfo().values()) {
				if (normalizedFieldName.equals(fieldModel.getNormalizedFieldName())) {
					result = true;
					break;
				}
			}
		}
		
		return result;
	}
}
