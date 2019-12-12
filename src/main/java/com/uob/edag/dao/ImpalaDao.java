package com.uob.edag.dao;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.lang.StringUtils;
import org.apache.commons.lang3.exception.ExceptionUtils;
import org.apache.ibatis.exceptions.PersistenceException;
import org.apache.ibatis.session.SqlSession;

import com.uob.edag.connection.ImpalaConnectionFactory;
import com.uob.edag.exception.EDAGException.PredefinedException;
import com.uob.edag.exception.EDAGMyBatisException;
import com.uob.edag.mappers.HiveMapper;
import com.uob.edag.mappers.ImpalaMapper;
import com.uob.edag.model.UNSFileTypeModel;

/**
 * @Author : Daya Venkatesan
 * @Date of Creation: 12/01/2016
 * @Description : The file is used for performing Database operations on Impala.
 * 
 */

public class ImpalaDao extends HiveDao {
	
  public static final PredefinedException CANNOT_INVALIDATE_METADATA = new PredefinedException(ImpalaDao.class, "CANNOT_INVALIDATE_METADATA", "Unable to invalidate metadata of {0}: {1}");
  public static final PredefinedException CANNOT_FETCH_ATTACH_FILE_NAMES_DOCTYPE = new PredefinedException(ImpalaDao.class, "CANNOT_FETCH_ATTACH_FILE_NAMES_DOCTYPE", "Unable to fetch files name from T11 tables : {0}");
  
  private HiveDao hiveDao = new HiveDao();

	public ImpalaDao() {
		super(ImpalaConnectionFactory.getFactory());
	}
	
	protected String getTableNotFoundMessage() {
  	return "Could not resolve path";
  }
	
	protected Map<String, String> getReconciliationParams(String fullTableName, String ctryCd, String bizDate, 
																												String processInstanceId, String columnName) throws EDAGMyBatisException {
		Map<String, String> params = super.getReconciliationParams(fullTableName, ctryCd, bizDate, processInstanceId, columnName);
		
		params.put("useImpala", Boolean.toString(true));
		
		if (columnName != null) {
			TableMetaData tableMeta = hiveDao.getTableMetadata(fullTableName);
			for (ColumnMetaData columnMeta : tableMeta.getColumnMetadata()) {
				if (columnName.equalsIgnoreCase(columnMeta.getName())) {
					if (StringUtils.containsIgnoreCase(columnMeta.getTypeName(), "string")) {
						params.put("castHashColumn", Boolean.toString(true));
					}
						
					break;	
				}
			}
		}
		
		logger.debug("Reconciliation parameters: " + params);
		return params;
	}

	/**
   * This method is used to run refresh on the Impala table to make the data available in Impala.
   * @param schemaName The schema name of the table on which statistics is to be run
   * @param tableName The table name on which the statistics is to be run
   * @throws Exception when there is an error running the statistics on the table
   */
  public void runRefresh(String schemaName, String tableName, String partitionInfo) throws EDAGMyBatisException {
    try (SqlSession session = openSession(true)) {
      Map<String, String> params = new HashMap<String, String>();
      params.put("schemaName", schemaName);
      params.put("tableName", tableName);
      params.put("partitionInfo", partitionInfo);
      
      try {
    	  session.getMapper(ImpalaMapper.class).runRefresh(params);
    	  logger.info(schemaName + "." + tableName + " metadata refreshed");
      } catch (PersistenceException e) {
    	  logger.info(String.format("Exception in refreshing impala metadata for %s.%s. Hence invalidating the metadata and refreshing again",schemaName, tableName));
    	  logger.info(ExceptionUtils.getStackTrace(e));
    	  session.getMapper(ImpalaMapper.class).invalidate(params);
    	  session.getMapper(ImpalaMapper.class).runRefresh(params);
      }

    } catch (PersistenceException e) {
    	throw new EDAGMyBatisException(ImpalaDao.CANNOT_INVALIDATE_METADATA, schemaName + "." + tableName, e.getMessage());
    }
  }
  
  public List<UNSFileTypeModel> getAttachNamesWithDocTypeImp (String hiveSQL) throws EDAGMyBatisException {
	  	try (SqlSession session = openSession(true)) {
	  		Map<String, String> params = new HashMap<String, String>();
	  		params.put("hiveSQL", hiveSQL);
	  		List<UNSFileTypeModel> result = session.getMapper(ImpalaMapper.class).getAttachNamesWithDocTypeImp(params);
	  		logger.debug("ImpalaDao: getAttachNamesWithDocTypeImp files number is " + result.size());
	  		return result;
	  	} catch (PersistenceException e) {
	  		throw new EDAGMyBatisException(ImpalaDao.CANNOT_FETCH_ATTACH_FILE_NAMES_DOCTYPE, e.getMessage());
	  	}
	  }
}
