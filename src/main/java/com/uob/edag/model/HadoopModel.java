package com.uob.edag.model;

import java.text.MessageFormat;
import java.util.Date;
import java.util.Map;

import org.codehaus.jackson.annotate.JsonIgnoreProperties;

import com.uob.edag.utils.UobUtils;


/**
 * @Author       : Daya Venkatesan
 * @Date of Creation: 10/24/2016
 * @Description    : The file defines the Target Hadoop Definitions. This file will be consumed in 
 *                   the  Data Ingestion process. This file contains all the properties which will 
 *                   help in  ingesting the file into Hadoop.
 * 
 */

@JsonIgnoreProperties(ignoreUnknown = true)
public interface HadoopModel extends TargetModel {
	
	static final MessageFormat INSERT_LOAD_PROCESS_TEMPLATE = new MessageFormat(
			UobUtils.ltrim("INSERT INTO EDAG_LOAD_PROCESS(PROC_ID, TGT_FORMAT_CD, TGT_COMPR_TYPE_CD, ") +
      UobUtils.ltrim("                              TGT_DB_NM, TGT_TBL_NM, TGT_TBL_PART_TXT, TGT_APLY_TYPE_CD, ") + 
      UobUtils.ltrim("                              STG_DIR_NM, STG_DB_NM, STG_TBL_NM, STG_TBL_PART_TXT, ") + 
      UobUtils.ltrim("                              ERR_THRESHOLD, CRT_DT, CRT_USR_NM) ") +
      UobUtils.ltrim("VALUES(''{0}'', ''{1}'', ''{2}'', ''{3}'', ''{4}'', ''{5}'', ''{6}'', ''{7}'', ''{8}'', ''{9}'', ''{10}'', {11, number, #.##}, DEFAULT, ''{12}''); ")
  );
	
	String getInsertLoadProcessSql();
	String getHadoopDir();
	void setHadoopDir(String hadoopDir);
  String getHadoopFormatCd();
  void setHadoopFormatCd(String hadoopFormatFormatCd);
  String getHadoopCompressCd();
  void setHadoopCompressCd(String hadoopCompressCd);
  String getHiveDbName();
  void setHiveDbName(String hiveDbName);
  String getHiveTableName();
  void setHiveTableName(String hiveTableName);
  String getHiveDumpTableName();
  String getLoadTypeCd();
  void setLoadTypeCd(String loadTypeCd);
  String getStagingDir();
  void setStagingDir(String stagingDir);
  String getDumpStagingDir(ProcessModel procModel, ProcessInstanceModel procInstanceModel);
  String getDumpStagingHiveLocation(ProcessModel procModel, ProcessInstanceModel procInstanceModel);
  String getStagingDbName();
  void setStagingDbName(String stagingDbName);
  String getDumpStagingTableName();
  String getStagingTableName();
  void setStagingTableName(String stagingTableName);
  String getHivePartition();
  void setHivePartition(String hivePartition);
  String getStagingHivePartition();
  void setStagingHivePartition(String stagingHivePartition);
  Map<Integer, FieldModel> getDestFieldInfo();
  boolean hasField(String fieldName);
  boolean hasNormalizedField(String normalizedFieldName);
  void setDestFieldInfo(Map<Integer, FieldModel> destFieldInfo);
  String getStagingHiveLocation();
  void setStagingHiveLocation(String stagingHiveLocation);
  //String getDumpStagingHiveLocation();
  //void setDumpStagingHiveLocation(String location);
  double getHiveErrorThreshold();
  void setHiveErrorThreshold(double hiveErrorThreshold);
  String getHadoopQueueName();
  void setHadoopQueueName(String hadoopQueueName);
  String getBdmConnName();
  void setBdmConnName(String bdmConnName);
  Date getCreatedDatetime();
  void setCreatedDatetime(Date dt);
  String getCreatedBy();
  void setCreatedBy(String by);
  Date getLastUpdatedDatetime();
  void setLastUpdatedDatetime(Date dt);
  String getLastUpdatedBy();
  void setLastUpdatedBy(String by);
	String getStagingErrorTableName();
}
