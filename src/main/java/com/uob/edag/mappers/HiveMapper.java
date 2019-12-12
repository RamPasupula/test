package com.uob.edag.mappers;

import java.util.List;
import java.util.Map;

import com.uob.edag.dao.TableMetaData;
import com.uob.edag.model.BizDateRowCount;
import com.uob.edag.model.HiveColumn;
import com.uob.edag.model.UNSFileTypeModel;
import com.uob.edag.mybatis.CountAndSumResult;

/**
 * @Author : Daya Venkatesan
 * @Date of Creation: 10/28/2016
 * @Description : The file is the MyBatis mapper class for the Hive DAO class.
 */

public interface HiveMapper {
	
  int createDatabase(String schemaName);

  int createStagingHiveTable(String createTableSql);

  int createHiveTable(String createTableSql);

  int renameHiveTable(Map<String, String> params);
  
  void dropHiveTable(String tableName);

  int dropHivePartition(Map<String, String> params);
  
  int dropHivePartitionNoCheck(Map<String, String> params);

  int attachHivePartition(Map<String, String> params);

  int getRowCount(Map<String, String> params);
  
  int getRowCountByMetadata(TableMetaData meta);
  
  int setQueueName(String queueName);
  
  int getErrorRowCount(Map<String, String> params);
  
  int getOtherProcInstancesRowCount(Map<String, String> params);

  String getHashSum(Map<String, String> params);

  CountAndSumResult getCountAndSum(Map<String, String> params);

  String checkSchemaExists(String schemaName);

  String checkTableExists(Map<String, String> params);
  
  List<HiveColumn> checkPartitionExists(Map<String, String> params);
  
  List<String> getTablePartitions(String tableName);
  
  List<BizDateRowCount> getBizDateRowCount(Map<String, String> params);

  void runStatistics(Map<String, String> params);
  
  String getMaxProcInstanceId(Map<String, String> params);
 
  void compactPartition(Map<String, String> params);
  
  void renamePartition(Map<String, String> params);
  
    List<UNSFileTypeModel> getAttachmentNamesWithDocType(Map<String, String> params);

}