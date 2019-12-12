package com.uob.edag.dao;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.SortedSet;
import java.util.TreeSet;

import com.uob.edag.exception.EDAGException;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.fs.Path;
import org.apache.ibatis.exceptions.PersistenceException;
import org.apache.ibatis.session.SqlSession;

import com.uob.edag.connection.HiveConnectionFactory;
import com.uob.edag.connection.MyBatisConnectionFactory;
import com.uob.edag.exception.EDAGException.PredefinedException;
import com.uob.edag.exception.EDAGIOException;
import com.uob.edag.exception.EDAGMyBatisException;
import com.uob.edag.mappers.HiveMapper;
import com.uob.edag.model.BizDateRowCount;
import com.uob.edag.model.HiveColumn;
import com.uob.edag.mybatis.CountAndSumResult;
import com.uob.edag.utils.HadoopUtils;
import com.uob.edag.utils.PropertyLoader;
import com.uob.edag.model.UNSFileTypeModel;

/**
 * @Author : Daya Venkatesan
 * @Date of Creation: 10/28/2016
 * @Description : The file is used for performing Database operations on Hive.
 * 
 */

public class HiveDao extends AbstractDao {
	
	public static final String PARTITION_CHECK_METHOD = PropertyLoader.getProperty(HiveDao.class.getName() + ".PartitionCheckMethod");
	public static final String DESCRIBE = "DESCRIBE";
	public static final String DIRECT_HDFS = "DIRECT_HDFS";
	public static final String DDS_HDFS_PATH = "DDS_HDFS_PATH";
	
  public static final PredefinedException HQL_EXCEPTION = new PredefinedException(HiveDao.class, "HQL_EXCEPTION", "Unable to execute {0}: {1}");
  public static final PredefinedException CANNOT_GET_MAX_PARTITION = new PredefinedException(HiveDao.class, "CANNOT_GET_MAX_PARTITION", "Unable to get maximum partition of table {0}, site ID {1}, business date {2}: {3}");
  public static final PredefinedException CANNOT_GET_PARTITIONS = new PredefinedException(HiveDao.class, "CANNOT_GET_PARTITIONS", "Unable to get partitions of table {0}: {1}");
  public static final PredefinedException CANNOT_DROP_PARTITION = new PredefinedException(HiveDao.class, "CANNOT_DROP_PARTITION", "Unable to drop partition {0} for table {1}: {2}");
  public static final PredefinedException CANNOT_DROP_TABLE = new PredefinedException(HiveDao.class, "CANNOT_DROP_TABLE", "Unable to drop table {0}: {1}");
  public static final PredefinedException CANNOT_GET_ROW_COUNT = new PredefinedException(HiveDao.class, "CANNOT_GET_ROW_COUNT", "Unable to get row count from {0} for country {1}, business date {2}, queue {3}: {4}");
  public static final PredefinedException CANNOT_GET_ROW_COUNT_BY_PARTITION = new PredefinedException(HiveDao.class, "CANNOT_GET_ROW_COUNT_BY_PARTITION", "Unable to get row count from {0} using partition {1}: {2}");
  public static final PredefinedException CANNOT_GET_OTHER_PROC_INSTANCES_ROW_COUNT = new PredefinedException(HiveDao.class, "CANNOT_GET_OTHER_PROC_INSTANCES_ROW_COUNT", "Unable to get row count from {0} for country {1}, business date {2} and process instances other than {3}, queue {4}: {5}");
  public static final PredefinedException CANNOT_GET_ERROR_ROW_COUNT = new PredefinedException(HiveDao.class, "CANNOT_GET_ERROR_ROW_COUNT", "Unable to get error row count from {0} for business date {1}, queue {2}: {3}");
  public static final PredefinedException CANNOT_GET_HASH_SUM = new PredefinedException(HiveDao.class, "CANNOT_GET_HASH_SUM", "Unable to get hash sum from {0}.{1} for country {2}, business date {3}: {4}");
  public static final PredefinedException CANNOT_ATTACH_PARTITION = new PredefinedException(HiveDao.class, "CANNOT ATTACH PARTITION", "Unable to attach partition {0} to {1}: {2}");
  public static final PredefinedException CANNOT_RUN_STATISTICS = new PredefinedException(HiveDao.class, "CANNOT_RUN_STATISTICS", "Unable to run statistics for {0}, partition clause {1}: {2}");
  public static final PredefinedException CANNOT_COMPACT_TABLE = new PredefinedException(HiveDao.class, "CANNOT_COMPACT_TABLE", "Unable to compact table {0} into partition {1} from source table {2}: {3}") ;
  public static final PredefinedException CANNOT_RENAME_PARTITION = new PredefinedException(HiveDao.class, "CANNOT_RENAME_PARTITION", "Unable to rename partition {0} into partition {1} from source table {2}: {3}") ;
  public static final PredefinedException CANNOT_FETCH_ATTACH_FILE_NAMES_DOCTYPE = new PredefinedException(HiveDao.class, "CANNOT_FETCH_ATTACH_FILE_NAMES_DOCTYPE", "Unable to fetch files name : {0}");

	public HiveDao() {
		super(HiveConnectionFactory.getFactory());
	}
	
	protected HiveDao(MyBatisConnectionFactory factory) {
		super(factory);
	}
	
  public int createHiveTable(String statement) throws EDAGMyBatisException {
  	try (SqlSession session = openSession(true)) {
  		int result = session.getMapper(HiveMapper.class).createHiveTable(statement);
  		logger.debug(statement + " execution result is " + result);
  		return result;
  	} catch (PersistenceException e) {
  		throw new EDAGMyBatisException(HiveDao.HQL_EXCEPTION, statement, e.getMessage());
   	}
  }
  
  public String getMaxProcInstanceId(String fullTableName, String ctryCd, String bizDate) throws EDAGMyBatisException {
  	try (SqlSession session = openSession(true)) {
  		Map<String, String> params = new HashMap<String, String>();
  		params.put("tableName", fullTableName);
  		params.put("ctryCd", ctryCd);
  		params.put("bizDate", bizDate);
  		String result = session.getMapper(HiveMapper.class).getMaxProcInstanceId(params);
  		logger.debug("Max process instance ID in table " + fullTableName + " for country " + ctryCd + " and biz date " + bizDate + " is " + result);
  		return result;
  	} catch (PersistenceException e) {
  		throw new EDAGMyBatisException(HiveDao.CANNOT_GET_MAX_PARTITION, fullTableName, ctryCd, bizDate, e.getMessage());
  	}
  }

	/**
   * This method is used to drop a partition from a hive table
   * @param schemaName The schema name in which the table is found
   * @param tableName The name of the Hive table
   * @param partition The partition which needs to be dropped
   * @throws Exception when there is an error dropping the hive table.
   */
  public boolean dropHivePartition(String schemaName, String tableName, String partition) throws EDAGMyBatisException {
  	boolean dropped = false;
  	
    Map<String, String> params = new HashMap<String, String>();
    String fullTableName = schemaName + "." + tableName;
    params.put("schema", schemaName.toLowerCase());
    params.put("table", tableName.toLowerCase());
    params.put("tableName", fullTableName);
    params.put("partition", partition);
    params.put("partitionPath", partition.replaceAll(",", "/")
    		                                 .replaceAll("'", "")); // TODO we assume partition values don't contain commas and single quotes
    try (SqlSession session = openSession(true)) {
    	HiveMapper mapper = session.getMapper(HiveMapper.class);
    			
    	if (DESCRIBE.equalsIgnoreCase(PARTITION_CHECK_METHOD)) {
    		List<HiveColumn> hiveColumns = null;
    		try {
    			hiveColumns = mapper.checkPartitionExists(params);
    			logger.info(fullTableName + " partition(" + partition + ") found");
    		} catch (Exception e) {
    			// if partition doesn't exist it'll throw exception
    			logger.info(fullTableName + " partition(" + partition + ") not found, not dropping partition: " + e.getClass().getName() + ": " + e.getMessage());
    		}
    		
    		if (hiveColumns != null) {
    		  // partition exists, just drop the partition without checking
    			mapper.dropHivePartitionNoCheck(params);
    			logger.info(fullTableName + " partition(" + partition + ") dropped");
    			dropped = true;
    		}
    	} else if (DIRECT_HDFS.equalsIgnoreCase(PARTITION_CHECK_METHOD)) {
    		Map<String, Object> ctx = new HashMap<String, Object>(params);
    		String ddsPath = PropertyLoader.getProperty(DDS_HDFS_PATH, ctx);
    		List<Path> files = null;
    		try {
					files = HadoopUtils.listFiles(ddsPath, false, false);
					logger.info(fullTableName + " partition(" + partition + (files == null ? ") not found" : ") found"));
				} catch (EDAGIOException e) {
					// partition directory not found
					logger.info("Partition directory " + ddsPath + " not found, not dropping partition: " + e.getMessage());
				}
    		
    		if (files != null && !files.isEmpty()) {
    		  // partition exists, just drop the partition without checking
					mapper.dropHivePartitionNoCheck(params);
					logger.info(fullTableName + " partition(" + partition + ") dropped");
					dropped = true;
    		}
      } else {
    		mapper.dropHivePartition(params);
    		logger.info(fullTableName + " partition(" + partition + ") dropped");
    		dropped = true;
    	}
    } catch (PersistenceException e) {
    	throw new EDAGMyBatisException(HiveDao.CANNOT_DROP_PARTITION, partition, fullTableName, e.getMessage());
    } 
    
    return dropped;
  }
  
  public boolean dropHivePartitionRecursive(String schemaName, String tableName, String partition) throws EDAGMyBatisException {
	  	boolean dropped = false;
	  	
	    Map<String, String> params = new HashMap<String, String>();
	    String fullTableName = schemaName + "." + tableName;
	    params.put("schema", schemaName.toLowerCase());
	    params.put("table", tableName.toLowerCase());
	    params.put("tableName", fullTableName);
	    params.put("partition", partition);
	    params.put("partitionPath", partition.replaceAll(",", "/")
	    		                                 .replaceAll("'", "")); // TODO we assume partition values don't contain commas and single quotes
	    try (SqlSession session = openSession(true)) {
	    	HiveMapper mapper = session.getMapper(HiveMapper.class);
	    			
	    	if (DESCRIBE.equalsIgnoreCase(PARTITION_CHECK_METHOD)) {
	    		List<HiveColumn> hiveColumns = null;
	    		try {
	    			hiveColumns = mapper.checkPartitionExists(params);
	    			logger.info(fullTableName + " partition(" + partition + ") found");
	    		} catch (Exception e) {
	    			// if partition doesn't exist it'll throw exception
	    			logger.info(fullTableName + " partition(" + partition + ") not found, not dropping partition: " + e.getClass().getName() + ": " + e.getMessage());
	    		}
	    		
	    		if (hiveColumns != null) {
	    		  // partition exists, just drop the partition without checking
	    			mapper.dropHivePartitionNoCheck(params);
	    			logger.info(fullTableName + " partition(" + partition + ") dropped");
	    			dropped = true;
	    		}
	    	} else if (DIRECT_HDFS.equalsIgnoreCase(PARTITION_CHECK_METHOD)) {
	    		Map<String, Object> ctx = new HashMap<String, Object>(params);
	    		String ddsPath = PropertyLoader.getProperty(DDS_HDFS_PATH, ctx);
	    		List<Path> files = null;
	    		try {
						files = HadoopUtils.listFiles(ddsPath, true, false);
						logger.info(fullTableName + " partition(" + partition + (files == null ? ") not found" : ") found"));
					} catch (EDAGIOException e) {
						// partition directory not found
						logger.info("Partition directory " + ddsPath + " not found, not dropping partition: " + e.getMessage());
					}
	    		
	    		if (files != null && !files.isEmpty()) {
	    		  // partition exists, just drop the partition without checking
						mapper.dropHivePartitionNoCheck(params);
						logger.info(fullTableName + " partition(" + partition + ") dropped");
						dropped = true;
	    		}
	      } else {
	    		mapper.dropHivePartition(params);
	    		logger.info(fullTableName + " partition(" + partition + ") dropped");
	    		dropped = true;
	    	}
	    } catch (PersistenceException e) {
	    	throw new EDAGMyBatisException(HiveDao.CANNOT_DROP_PARTITION, partition, fullTableName, e.getMessage());
	    } 
	    
	    return dropped;
	  }
  
  public boolean checkTableExists(String tableName) throws EDAGMyBatisException {
  	try {
  		getTableMetadata(tableName);
  		return true;
  	} catch (PersistenceException e) {
  		if (StringUtils.trimToEmpty(e.getMessage()).toLowerCase().contains(getTableNotFoundMessage().toLowerCase())) {
  			return false;
  		} else {
  			throw e;
  		}
  	}
  }
  
  protected String getTableNotFoundMessage() {
  	return "Table not found";
  }
  
  public TableMetaData getTableMetadata(String tableName) throws EDAGMyBatisException {
  	String[] splitNames = tableName.split(".", -1);
  	TableMetaData tableMeta = new TableMetaData(splitNames.length > 1 ? splitNames[1] : tableName);
  	tableMeta.setSchemaName(splitNames.length > 1 ? splitNames[0] : null);
  	String statement = getMetadataSelectStatement(tableName);
  	try (SqlSession session = openSession(true)) {
			Connection conn = session.getConnection();
			boolean closingStatement = false;
			try (PreparedStatement ps = conn.prepareStatement(statement)) {
				try (ResultSet rs = ps.executeQuery()) {
					while (rs.next()) {
						String colName = StringUtils.trimToEmpty(rs.getString(1));
						if (colName.toLowerCase().contains("# partition information")) {
							break; // stop here
						} else if ("".equals(colName)) {
							continue; // skip empty col name
						}
						
						ColumnMetaData colMeta = new ColumnMetaData(colName);
						colMeta.setTypeName(rs.getString(2));
						
						tableMeta.getColumnMetadata().add(colMeta);
					}
				}
			} catch (SQLException e) {
				if (closingStatement) {
					logger.warn("Unable to close preparedStatement " + statement + ": " + e.getMessage());
				} else {
					throw new PersistenceException(e);
				}
			}
		}
		
		logger.debug("Metadata of table " + tableName + ": " + tableMeta);
		return tableMeta;
  }
  
  protected String getMetadataSelectStatement(String tableName) {
  	return "describe " + tableName;
	}
  
  public List<Map<String, String>> getTablePartitions(String tableName) throws EDAGMyBatisException {
  	List<Map<String, String>> result = new ArrayList<Map<String, String>>();
  	
  	try (SqlSession session = openSession(true)) {
  		List<String> partitions = session.getMapper(HiveMapper.class).getTablePartitions(tableName);
  		if (partitions != null) {
	  		for (String partition : partitions) {
	  			Map<String, String> partitionMap = new HashMap<String, String>();
	  			result.add(partitionMap);
	  			
	  			for (String directory : partition.split("/", -1)) {
	  				String[] kvPair = directory.split("=", -1);
	  				partitionMap.put(kvPair[0], kvPair[1]);
	  			}
	  		}
  		}
  	} catch (PersistenceException e) {
  		throw new EDAGMyBatisException(HiveDao.CANNOT_GET_PARTITIONS, tableName, e.getMessage());
  	}
  	
  	logger.debug("Partitions of table " + tableName + ": " + result);
  	return result;
  }
  
  public void dropHiveTable(String schemaName, String tableName) throws EDAGMyBatisException {
  	String fullTableName = schemaName + "." + tableName;
  	try (SqlSession session = openSession(true)) {
  		session.getMapper(HiveMapper.class).dropHiveTable(fullTableName);
  		logger.info(fullTableName + " dropped");
  	} catch (PersistenceException e) {
  		throw new EDAGMyBatisException(HiveDao.CANNOT_DROP_TABLE, fullTableName, e.getMessage());
  	} 
  }
  
  public int getOtherProcInstancesRowCount(String schemaName, String tableName, String ctryCd, String bizDate, 
  		                                     String procInstanceId, String queueName) throws EDAGMyBatisException {
  	String fullTableName = schemaName + "." + tableName;
    try (SqlSession session = openSession(true)) {
      Map<String, String> params = new HashMap<String, String>();
      params.put("tableName", fullTableName);
      params.put("ctryCd", ctryCd);
      params.put("bizDate", bizDate);
      params.put("procInstanceId", procInstanceId);
      params.put("queueName", queueName);
      
      HiveMapper mapper = session.getMapper(HiveMapper.class);
      if (queueName != null) {
      	mapper.setQueueName(queueName);
      }
      int rowCount = mapper.getOtherProcInstancesRowCount(params);
      logger.debug("Other proc instances row count for " + params + " is " + rowCount);
      return rowCount;
    } catch (PersistenceException e) {
    	throw new EDAGMyBatisException(HiveDao.CANNOT_GET_ROW_COUNT, fullTableName, ctryCd, bizDate, queueName, e.getMessage());
    }
  }

  /**
   * This method is used to calculate the row count from Hive table for the given filters.
   * @param schemaName The name of the schema
   * @param tableName The name of the Hive table
   * @param ctryCd The Country Code Value
   * @param bizDate Business Date 
   * @param queueName The Hadoop Queue Name
   * @returns the Row Count calculated from the table
   * @throws Exception when there is an error getting the row count from the table
   */
  public int getRowCount(String schemaName, String tableName, String ctryCd, String bizDate,
  		                   String processInstanceId, String queueName) throws EDAGMyBatisException {
  	String fullTableName = schemaName + "." + tableName;
    try (SqlSession session = openSession(true)) {
      HiveMapper mapper = session.getMapper(HiveMapper.class);
      if (queueName != null) {
      	mapper.setQueueName(queueName);
      }
      Map<String, String> params = getReconciliationParams(fullTableName, ctryCd, bizDate, processInstanceId, null);
      int rowCount = mapper.getRowCount(params);
      logger.debug("Row count for " + params + " is " + rowCount);
      return rowCount;
    } catch (PersistenceException e) {
    	throw new EDAGMyBatisException(HiveDao.CANNOT_GET_ROW_COUNT, fullTableName, ctryCd, bizDate, queueName, e.getMessage());
    }
  }
  
  public int getRowCountByPartition(String schemaName, String tableName, String partition, String queueName) throws EDAGMyBatisException {
  	try (SqlSession session = openSession(true)) {
  		TableMetaData meta = new TableMetaData(tableName);
  		meta.setSchemaName(schemaName);
  		meta.setPartition(partition);
  		
  		HiveMapper mapper = session.getMapper(HiveMapper.class);
  		if (queueName != null) {
  			mapper.setQueueName(queueName);
  		}
  		
  		int rowCount = mapper.getRowCountByMetadata(meta);
  		logger.debug("Row count of " + schemaName + "." + tableName + " based on partition " + partition + " is " + rowCount);
  		return rowCount;
  	} catch (PersistenceException e) {
  		throw new EDAGMyBatisException(HiveDao.CANNOT_GET_ROW_COUNT_BY_PARTITION, schemaName + "." + tableName, partition, e.getMessage());
  	}
  }
  
  public SortedSet<BizDateRowCount> getBizDateRowCountByCountry(String schemaName, String tableName, String countryCode, 
  		                                                          String maxBizDate, String queueName) throws EDAGMyBatisException {
  	String fullTableName = schemaName + "." + tableName;
  	try (SqlSession session = openSession(true)) {
  		Map<String, String> params = new HashMap<String, String>();
  		params.put("tableName",  fullTableName);
  		params.put("maxBizDate", maxBizDate);
  		params.put("countryCode", countryCode);
  		HiveMapper mapper = session.getMapper(HiveMapper.class);
  		if (queueName != null) {
  			mapper.setQueueName(queueName);
  		}
  		
  		List<BizDateRowCount> list = mapper.getBizDateRowCount(params);
  		SortedSet<BizDateRowCount> result = new TreeSet<BizDateRowCount>(list);
  		logger.debug("Row counts from " + fullTableName + ", country " + countryCode + ", max biz date " + maxBizDate + " are " + result);
  		return result;
  	} catch (PersistenceException e) {
  		throw new EDAGMyBatisException(HiveDao.CANNOT_GET_ROW_COUNT, fullTableName, countryCode, maxBizDate, queueName, e.getMessage());
  	}
  }
  
  /**
   * This method is used to calculate the error row count from Hive table for the given filters.
   * @param schemaName The name of the schema
   * @param tableName The name of the Hive table
   * @param bizDate Business Date 
   * @param queueName The Hadoop Queue Name
   * @returns the Row Count calculated from the table
   * @throws Exception when there is an error getting the row count from the table
   */
  public int getErrorRowCount(String schemaName, String tableName, String bizDate, String queueName) throws EDAGMyBatisException {
  	String fullTableName = schemaName + "." + tableName;
    try (SqlSession session = openSession(true)) {
      Map<String, String> params = new HashMap<String, String>();
      params.put("tableName", fullTableName);
      params.put("bizDate", bizDate);
      HiveMapper mapper = session.getMapper(HiveMapper.class);
      if (queueName != null) {
      	mapper.setQueueName(queueName);
      }
      int result = mapper.getErrorRowCount(params);
      logger.debug("Error row count for " + fullTableName + ", biz date " + bizDate + " is " + result);
      return result;
    } catch (PersistenceException e) {
      throw new EDAGMyBatisException(HiveDao.CANNOT_GET_ERROR_ROW_COUNT, fullTableName, bizDate, queueName, e.getMessage());
    }
  }

  /**
   * This method is used to get the hashsum from hive table for the given filters.
   * @param schemaName The name of the schema
   * @param tableName The name of the Hive table
   * @param ctryCd Country Code Value
   * @param bizDate Business Date Value
   * @param columnName The column name on which the sum is to be computed
   * @returns the hash sum value calculated
   * @throws Exception when there is an error generating the hash sum
   */
  public String getHashSum(String schemaName, String tableName, String ctryCd, String bizDate, 
  		                     String processInstanceId, String columnName, String queueName) throws EDAGMyBatisException {
    String fullTableName = schemaName + "." + tableName;
    try (SqlSession session = openSession(true)) {
      HiveMapper mapper = session.getMapper(HiveMapper.class);
      if (queueName != null) {
      	mapper.setQueueName(queueName);
      }
      String hashSum = mapper.getHashSum(getReconciliationParams(fullTableName, ctryCd, bizDate, processInstanceId, columnName));
      logger.debug("Hash sum for " + fullTableName + "." + columnName + ", country " + ctryCd + " for business date " + 
                   bizDate + " and process instance ID " + processInstanceId + " is " + hashSum);
      return hashSum;
    } catch (PersistenceException e) {
      throw new EDAGMyBatisException(HiveDao.CANNOT_GET_HASH_SUM, fullTableName, columnName, ctryCd, bizDate, e.getMessage());
    }
  }
  
  protected Map<String, String> getReconciliationParams(String fullTableName, String ctryCd, String bizDate, 
      																								  String processInstanceId, String columnName) throws EDAGMyBatisException {
  	Map<String, String> params = new HashMap<String, String>();
    params.put("tableName", fullTableName);
    
    if (ctryCd != null) {
	    params.put("ctryCd", ctryCd);
	    params.put("bizDate", bizDate);
    }
    
    if (columnName != null) {
    	params.put("columnName", columnName);
    }
    
    if (processInstanceId != null) {
    	params.put("procInstanceId", processInstanceId);
    }
    
    logger.debug("Reconciliation parameters: " + params);
    return params;
  }

    /**
     * This method is used used to get the count as well as sum within the same select statement
     * @param schemaName
     * @param tableName
     * @param ctryCd
     * @param bizDate
     * @param columnName
     * @param queueName
     * @return an object with 2 attributes: count and sum
     * @throws EDAGMyBatisException
     */
  public CountAndSumResult getCountAndSum(String schemaName, String tableName, String ctryCd, String bizDate, 
  		                                    String processInstanceId, String columnName, String queueName) throws EDAGMyBatisException {
      String fullTableName = schemaName + "." + tableName;
      try (SqlSession session = openSession(true)) {
          HiveMapper mapper = session.getMapper(HiveMapper.class);
          if (queueName != null) {
          	mapper.setQueueName(queueName);
          }
          CountAndSumResult cs = mapper.getCountAndSum(getReconciliationParams(fullTableName, ctryCd, bizDate, processInstanceId, columnName));
          logger.debug("Count and Sum for " + fullTableName + "." + columnName + ", country " + ctryCd + " for business date " +
                       bizDate + (processInstanceId == null ? "" : " and proc instance ID " + processInstanceId) + " is " + cs.toString());
          return cs;
      } catch (PersistenceException e) {
          throw new EDAGMyBatisException(HiveDao.CANNOT_GET_HASH_SUM, fullTableName, columnName, ctryCd, bizDate, e.getMessage());
      }
  }

  /**
   * This method is used to attach a partition to a external hive table
   * @param schemaName The name of the schema
   * @param tableName The table name on which the partition is to be attached.
   * @param partitionValue The Partition Column to be attached
   * @throws Exception when there is an error attaching the hive partition
   */
  public void attachHivePartition(String schemaName, String tableName, String partitionValue) throws EDAGMyBatisException {
    String fullTableName = schemaName + "." + tableName;
    try (SqlSession session = openSession(true)) {
      Map<String, String> params = new HashMap<String, String>();
      params.put("tableName", fullTableName);
      params.put("partitionValue", partitionValue);

      session.getMapper(HiveMapper.class).attachHivePartition(params);
      logger.info("Partition " + partitionValue + " attached to " + fullTableName);
    } catch (PersistenceException e) {
      throw new EDAGMyBatisException(HiveDao.CANNOT_ATTACH_PARTITION, partitionValue, fullTableName, e.getMessage());
    }
  }

  /**
   * This method is used to run statistics on the Hive table to make query executions faster.
   * @param schemaName The schema name of the table on which statistics is to be run
   * @param tableName The table name on which the statistics is to be run
   * @param partitionClause The partition on which the statistics is to be run
   * @param queueName The Hadoop Queue Name
   * @throws Exception when there is an error running the statistics on the table
   */
  public void runStatistics(String schemaName, String tableName, String partitionClause, String queueName) throws EDAGMyBatisException {
  	String fullTableName = schemaName + "." + tableName;
	  SqlSession session = null;
	  try {
		  session = openSession(true);
	  } catch (PersistenceException e) {
		  throw new EDAGMyBatisException(HiveDao.CANNOT_RUN_STATISTICS, fullTableName, partitionClause, e.getMessage());
	  }

	  Map<String, String> params = new HashMap<String, String>();
      params.put("schemaName", schemaName);
      params.put("tableName", tableName);
      params.put("partitionClause", partitionClause);

	  HiveMapper mapper = session.getMapper(HiveMapper.class);

	  if (queueName != null) {
      	mapper.setQueueName(queueName);
      }

	  try { // PEDDAGML_1285, separate out the try-catch
	  	mapper.runStatistics(params);
	  	logger.debug("Statistics ran for " + fullTableName + ", partition clause " + partitionClause);
	  }
	  catch(PersistenceException e){
	  	logger.error("Failed to run Statistics for " + fullTableName + ", partition clause " + partitionClause);
	  }


  }
  
  
  public void compactPartition(String sourceTable,  String compactionTable, String condition, String columns, String partition) throws EDAGMyBatisException {
	try (SqlSession session = openSession(true)) {
		Map<String, String> params = new HashMap<String, String>();
		params.put("compactionTable", compactionTable);
		params.put("partition", partition);
		params.put("sourceTable", sourceTable);
		params.put("condition", condition);
		params.put("columns", columns);

		HiveMapper mapper = session.getMapper(HiveMapper.class);
		mapper.compactPartition(params);
		logger.debug("Compaction ran for " + compactionTable + ", records inserted into partition " + partition);
	} catch (PersistenceException e) {
		throw new EDAGMyBatisException(HiveDao.CANNOT_COMPACT_TABLE, compactionTable, partition, sourceTable, e.getMessage());
	}
	  
  }
  
  public void dropHivePartition(String tableName, String partitionInfo) throws EDAGMyBatisException {
	  try (SqlSession session = openSession(true)) {
		  Map<String, String> params = new HashMap<String, String>();
		  params.put("tableName", tableName);
		  params.put("partition", partitionInfo);

		  HiveMapper mapper = session.getMapper(HiveMapper.class);
		  mapper.dropHivePartition(params);
		  logger.info(tableName + " partition(" + partitionInfo + ") dropped");
		} catch (PersistenceException e) {
			throw new EDAGMyBatisException(HiveDao.CANNOT_DROP_PARTITION, tableName, partitionInfo, e.getMessage());
		}
  }
  
  public void renamePartition(String tableName, String originalPartition, String targetPartition) throws EDAGMyBatisException {
	try (SqlSession session = openSession(true)) {
		Map<String, String> params = new HashMap<String, String>();
		params.put("tableName", tableName);
		params.put("originalPartition", originalPartition);
		params.put("targetPartition", targetPartition);

		HiveMapper mapper = session.getMapper(HiveMapper.class);
		mapper.renamePartition(params);
		logger.debug("Renamed source partition " + originalPartition + ", to " + targetPartition);
	} catch (PersistenceException e) {
		throw new EDAGMyBatisException(HiveDao.CANNOT_RENAME_PARTITION, originalPartition, targetPartition, tableName, e.getMessage());
	}
  }
  
  
  public List<UNSFileTypeModel> getAttachmentNamesWithDocType (String hiveSQL) throws EDAGMyBatisException {
	  	try (SqlSession session = openSession(true)) {
	  		Map<String, String> params = new HashMap<String, String>();
	  		params.put("hiveSQL", hiveSQL);
	  		List<UNSFileTypeModel> result = session.getMapper(HiveMapper.class).getAttachmentNamesWithDocType(params);
	  		logger.debug("HiveDao: getAttachmentNamesWithDocType files number is " + result.size());
	  		return result;
	  	} catch (PersistenceException e) {
	  		throw new EDAGMyBatisException(HiveDao.CANNOT_FETCH_ATTACH_FILE_NAMES_DOCTYPE, e.getMessage());
	  	}
	  }
  
}