package com.uob.edag.dao;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import org.apache.commons.lang.StringUtils;
import org.apache.ibatis.annotations.Param;
import org.apache.ibatis.exceptions.PersistenceException;
import org.apache.ibatis.session.SqlSession;

import com.uob.edag.connection.DBMSConnectionFactory;
import com.uob.edag.constants.UobConstants;
import com.uob.edag.exception.EDAGException;
import com.uob.edag.exception.EDAGException.PredefinedException;
import com.uob.edag.exception.EDAGMyBatisException;
import com.uob.edag.exception.EDAGValidationException;
import com.uob.edag.mappers.IngestionMapper;
import com.uob.edag.model.AttMasterFileModel;
import com.uob.edag.model.CountryAttributes;
import com.uob.edag.model.DestModel;
import com.uob.edag.model.FieldModel;
import com.uob.edag.model.FileModel;
import com.uob.edag.model.HadoopModel;
import com.uob.edag.model.ProcessInstanceModel;
import com.uob.edag.model.ProcessModel;
import com.uob.edag.model.ProcessParam;
import com.uob.edag.model.SolrCollectionModel;
import com.uob.edag.model.SourceTableDetail;
import com.uob.edag.model.SparkExecParamsModel;
import com.uob.edag.model.StageModel;
import com.uob.edag.model.TDUnsupportedCharacterReplacement;

/**
 * @Author : Daya Venkatesan
 * @Date of Creation: 10/31/2016
 * @Description : The class is used for database operations to retrieve details
 *              for File Ingestion Process.
 * 
 */

public class IngestionDao extends AbstractDao {
	
  public static final PredefinedException CANNOT_RETRIEVE_PROCESS_MASTER = new PredefinedException(IngestionDao.class, "CANNOT_RETRIEVE_PROCESS_MASTER", "Unable to retrieve process master for process ID {0}: {1}");
	public static final PredefinedException CANNOT_RETRIEVE_PROCESS_COUNTRY = new PredefinedException(IngestionDao.class, "CANNOT_RETRIEVE_PROCESS_COUNTRY", "Unable to retrieve countries for process ID {0}: {1}");
	public static final PredefinedException CANNOT_RETRIEVE_SOURCE_TABLE_DETAIL = new PredefinedException(IngestionDao.class, "CANNOT_RETRIEVE_SOURCE_TABLE_DETAIL", "Unable to retrieve source table detail for process ID {0}: {1}");
	public static final PredefinedException CANNOT_RETRIEVE_LOAD_PROCESS = new PredefinedException(IngestionDao.class, "CANNOT_RETRIEVE_LOAD_PROCESS", "Unable to retrieve load process for process ID {0}: {1}");
	public static final PredefinedException CANNOT_RETRIEVE_LOAD_PROCESS_BY_TARGET_TABLE = new PredefinedException(IngestionDao.class, "CANNOT_RETRIEVE_LOAD_PROCESS_BY_TARGET_TABLE", "Unable to retrieve load process by target table {0}: {1}");
	public static final PredefinedException CANNOT_RETRIEVE_PROCESS_FILENAME = new PredefinedException(IngestionDao.class, "CANNOT_RETRIEVE_PROCESS_FILENAME", "Unable to retrieve process file name for process instance ID {0}: {1}");
	public static final PredefinedException CANNOT_RETRIEVE_FILE_DETAILS = new PredefinedException(IngestionDao.class, "CANNOT_RETRIEVE_FILE_DETAILS", "Unable to retrieve file details for process ID {0}: {1}");
	public static final PredefinedException CANNOT_RETRIEVE_FIELD_DETAILS = new PredefinedException(IngestionDao.class, "CANNOT_RETRIEVE_FIELD_DETAILS", "Unable to retrieve field details for file ID {0}: {1}");
	public static final PredefinedException CANNOT_RETRIEVE_CONTROL_FIELD_DETAILS = new PredefinedException(IngestionDao.class, "CANNOT_RETRIEVE_CONTROL_FIELD_DETAILS", "Unable to retrieve control field details for file ID {0}: {1}");
	public static final PredefinedException CANNOT_RETRIEVE_STD_RULES = new PredefinedException(IngestionDao.class, "CANNOT_RETRIEVE_STD_RULES", "Unable to retrieve std rules for file ID {0}, field {1}, record type {2}: {3}");
	public static final PredefinedException CANNOT_RETRIEVE_LOAD_PARAMS = new PredefinedException(IngestionDao.class, "CANNOT_RETRIEVE_LOAD_PARAMS", "Unable to retrieve load params for process ID {0}: {1}");
	public static final PredefinedException CANNOT_INSERT_PROCESS_LOG = new PredefinedException(IngestionDao.class, "CANNOT_INSERT_PROCESS_LOG", "Unable to insert process log for instance ID {0}, process ID {1}: {2}");
	public static final PredefinedException CANNOT_UPDATE_PROCESS_LOG_FILENAME = new PredefinedException(IngestionDao.class, "CANNOT_UPDATE_PROCESS_LOG_FILENAME", "Unable to update file name of process log instance ID {0} to {1}: {2}");
	public static final PredefinedException CANNOT_UPDATE_PROCESS_LOG = new PredefinedException(IngestionDao.class, "CANNOT_UPDATE_PROCESS_LOG", "Unable to update process log for instance ID {0}, process ID {1}: {2}");
	public static final PredefinedException CANNOT_INSERT_STAGE_LOG = new PredefinedException(IngestionDao.class, "CANNOT_INSERT_STAGE_LOG", "Unable to insert stage log for process instance ID {0}: {1}");
	public static final PredefinedException CANNOT_UPDATE_STAGE_LOG = new PredefinedException(IngestionDao.class, "CANNOT_UPDATE_STAGE_LOG", "Unable to update stage log for process instance ID {0}, stage ID {1}: {2}");
	public static final PredefinedException CANNOT_EVAL_BIZ_DATE_EXPR = new PredefinedException(IngestionDao.class, "CANNOT_EVAL_BIZ_DATE_EXPR", "{0} cannot be evaluated as a valid business date: {1}");
	public static final PredefinedException CANNOT_GET_PROCESS_INSTANCE = new PredefinedException(IngestionDao.class, "CANNOT_GET_PROCESS_INSTANCE", "Unable to get process instance for instance ID {0}: {1}");
	public static final PredefinedException CANNOT_GET_PREVIOUS_RUN_STATUS = new PredefinedException(IngestionDao.class, "CANNOT_GET_PREVIOUS_RUN_STATUS", "Unable to get previous run status for process ID {0}, business date {1}, country {2}: {3}");
	public static final PredefinedException CANNOT_GET_STAGE_INFO = new PredefinedException(IngestionDao.class, "CANNOT_GET_STAGE_INFO", "Unable to get stage log entries for process instance ID {0}: {1}");
	public static final PredefinedException CANNOT_GET_STAGE_INFO_BY_PROCESS_AND_STAGE_ID = new PredefinedException(IngestionDao.class, "CANNOT_GET_STAGE_INFO_BY_PROCESS_AND_STAGE_ID", "Unable to get stage {0} log entries for process {1}, country {2} on {3}: {4}");
	public static final PredefinedException CANNOT_GET_BIZ_DATE = new PredefinedException(IngestionDao.class, "CANNOT_GET_BIZ_DATE", "Unable to get business date for process ID {0}, country {1}: {2}");
	public static final PredefinedException CANNOT_GET_TD_UNSUPPORTED_CHAR_REPLACEMENT = new PredefinedException(IngestionDao.class, "CANNOT_GET_TD_UNSUPPORTED_CHAR_REPLACEMENT", "Unable to get replacements for characters not supported by Teradata for process {0}, country {1}: {2}");
	public static final PredefinedException CANNOT_GET_CONTROL_CHAR_REPLACEMENT = new PredefinedException(IngestionDao.class, "CANNOT_GET_CONTROL_CHAR_REPLACEMENT", "Unable to get control characters replacements for process {0}, country {1}: {2}");
	public static final PredefinedException CANNOT_GET_FIELD_NAME_PATTERNS = new PredefinedException(IngestionDao.class, "CANNOT_GET_FIELD_NAME_PATTERNS", "Unable to get field name patterns for process {0}: {1}");
	public static final PredefinedException CANNOT_INSERT_LOADED_FILE_LOG = new PredefinedException(IngestionDao.class, "CANNOT_INSERT_LOADED_FILE_LOG", "Unable to insert process log for instance ID {0}, process ID {1}: {2}");
	public static final PredefinedException CANNOT_RETRIEVE_SOLR_COLLECTION = new PredefinedException(IngestionDao.class, "CANNOT_RETRIEVE_SOLR_COLLECTION", "Unable to retrieve the solr collection details from metastore. {0}");
	public static final PredefinedException CANNOT_RETRIEVE_T11HIVEQL_FROM_METASTORE = new PredefinedException(IngestionDao.class, "CANNOT_RETRIEVE_T11HIVEQL_FROM_METASTORE", "Unable to retrieve T11 HQL from metastore. {0}");
	public static final PredefinedException CANNOT_RETRIEVE_PRE_PROCESS_FLAG = new PredefinedException(IngestionDao.class, "CANNOT_RETRIEVE_PRE_PROCESS_FLAG", "Unable to retrieve Pre Process Flag from metastore. {0}");
	public static final PredefinedException CANNOT_RETRIEVE_PRE_PROCESS_CLASS_NAME = new PredefinedException(IngestionDao.class, "CANNOT_RETRIEVE_PRE_PROCESS_CLASS_NAME", "Unable to retrieve Pre Process Class Name from metastore. {0}");
	public static final PredefinedException CANNOT_UPDATE_PROCESS_LOG_FILE_SIZE_TIME = new PredefinedException(IngestionDao.class, "CANNOT_UPDATE_PROCESS_LOG_FILE_SIZE_TIME",
			"Unable to update process log with Source File Size and Source File Arrival Time for instance ID {0}, process ID {1}: {2}" );
	public static final PredefinedException CANNOT_RETRIEVE_SPARK_EXEC_PROPERTIES = new PredefinedException(IngestionDao.class, "CANNOT_RETRIEVE_SPARK_EXEC_PROPERTIES", "Unable to retrieve Spark Execution Properties. {0} - {1}");
	
	public IngestionDao() {
		super(DBMSConnectionFactory.getFactory());
	}

	/**
   * This method is used to retrieve the metadata of the Process Master table.
   * @param procId The Process ID to be retrieved
   * @throws Exception when there is an error retrieving the Process Master
   */
  public ProcessModel retrieveProcessMaster(String procId) throws EDAGMyBatisException {
    try (SqlSession session = openSession()) {
    	ProcessModel processModel = session.getMapper(IngestionMapper.class).retrieveProcessMaster(procId);
      logger.debug("Process model for process ID " + procId + " is " + processModel);
      return processModel;
    } catch (PersistenceException e) {
    	throw new EDAGMyBatisException(IngestionDao.CANNOT_RETRIEVE_PROCESS_MASTER, procId, e.getMessage());
    }
  }

  /**
   * This method is used to retrieve the metadata of the Process Master table.
   * @throws Exception when there is an error retrieving the Process Master
   */
	public List<ProcessModel> retrieveProcessList() throws EDAGMyBatisException {
    SqlSession session = openSession();
    try {
      logger.info("Retrieving process list");
      return session.getMapper(IngestionMapper.class).retrieveProcessList();
    } finally {
      if (session != null) {
        session.close();
      }
    }
  }

  /**
   * This method is used to retrieve metadata from the Process Country table.
   * @param procId The Process ID to be retrieved
   * @throws Exception when there is an error retrieving the metadata
   */
  public Map<String, CountryAttributes> retrieveProcessCountry(String procId) throws EDAGMyBatisException {
    try (SqlSession session = openSession()) {
    	List<CountryAttributes> countries = session.getMapper(IngestionMapper.class).retrieveProcessCountry(procId); 
      logger.debug("Process countries for process ID " + procId + " are " + countries);
      
      Map<String, CountryAttributes> map = new HashMap<String, CountryAttributes>();
      for (CountryAttributes attrs : countries) {
      	map.put(attrs.getCountryCode(), attrs);
      }
      
      return map;
    } catch (PersistenceException e) {
    	throw new EDAGMyBatisException(IngestionDao.CANNOT_RETRIEVE_PROCESS_COUNTRY, procId, e.getMessage());
    }
  }
  
  public SourceTableDetail retrieveSourceTableDetail(String procId) throws EDAGMyBatisException {
    try (SqlSession session = openSession()) {
    	SourceTableDetail result = session.getMapper(IngestionMapper.class).retrieveSourceTableDetail(procId); 
    	if (result != null) {
    		logger.info("Source table detail for process ID " + procId + " is " + result);
    	}
      return result;
    } catch (PersistenceException e) {
      throw new EDAGMyBatisException(IngestionDao.CANNOT_RETRIEVE_SOURCE_TABLE_DETAIL, procId, e.getMessage());
    }
  }

  /**
   * This method is used to retrieve metadata from the Load Process table.
   * @param procId The Process ID to be retrieved
   * @throws Exception when there is an error retrieving the metadata
   */
  public DestModel retrieveLoadProcess(String procId) throws EDAGMyBatisException {
    try (SqlSession session = openSession()) {
    	DestModel result = session.getMapper(IngestionMapper.class).retrieveLoadProcess(procId);
      logger.debug("Load process for process ID " + procId + " is " + result);
      return result;
    } catch (PersistenceException e) {
    	throw new EDAGMyBatisException(IngestionDao.CANNOT_RETRIEVE_LOAD_PROCESS, procId, e.getMessage());
    }
  }
  
  public HadoopModel retrieveLoadProcessByTargetTable(String targetDatabaseName, String targetTableName) throws EDAGMyBatisException {
  	String fullTableName = targetDatabaseName + "." + targetTableName;
    try (SqlSession session = openSession()) {
      Map<String, String> params = new HashMap<String, String>();
      params.put("targetSchemaName", targetDatabaseName.toLowerCase());
      params.put("targetTableName", targetTableName.toLowerCase());
      HadoopModel result = session.getMapper(IngestionMapper.class).retrieveLoadProcessByTargetTable(params);
      logger.debug("Load process populating " + fullTableName + " is " + result);
      return result;
    } catch (PersistenceException e) {
      throw new EDAGMyBatisException(IngestionDao.CANNOT_RETRIEVE_LOAD_PROCESS_BY_TARGET_TABLE, fullTableName, e.getMessage());
    }
  }
  
  /**
   * This method is used to retrieve the process name for a process instance.
   * @param procInstanceId The Process Instance ID 
   * @throws Exception when there is an error retrieving the process name
   */
  public String retrieveProcessFileName(String procInstanceId) throws EDAGMyBatisException {
    try (SqlSession session = openSession()) {
    	String result = session.getMapper(IngestionMapper.class).retrieveProcessFileName(procInstanceId); 
      logger.debug("Process file name for process instance ID " + procInstanceId + " is " + result);
      return result;
    } catch (PersistenceException e) {
      throw new EDAGMyBatisException(IngestionDao.CANNOT_RETRIEVE_PROCESS_FILENAME, procInstanceId, e.getMessage());
    }
  }

  /**
   * This method is used to retrieve metadata from the File Details table.
   * @param procId The Process ID to be retrieved
   * @throws Exception when there is an error retrieving the metadata
   */
  public FileModel retrieveFileDetails(String procId) throws EDAGMyBatisException {
    try (SqlSession session = openSession()) {
    	FileModel result = session.getMapper(IngestionMapper.class).retrieveFileDetails(procId); 
      logger.debug("File details for process ID " + procId + " is " + result);
      return result;
    } catch (PersistenceException e) {
    	throw new EDAGMyBatisException(IngestionDao.CANNOT_RETRIEVE_FILE_DETAILS, procId, e.getMessage());
    }
  }

  /**
   * This method is used to retrieve metadata from the Field Details table.
   * @param fileId The File ID to be retrieved
   * @throws Exception when there is an error retrieving the metadata
   */
  public List<FieldModel> retrieveFieldDetails(int fileId) throws EDAGMyBatisException {
    try (SqlSession session = openSession()) {
    	List<FieldModel> result = session.getMapper(IngestionMapper.class).retrieveFieldDetails(fileId); 
      logger.info("File ID " + fileId + " has " + (result == null ? 0 : result.size()) + " fields");
      return result;
    } catch (PersistenceException e) {
      throw new EDAGMyBatisException(IngestionDao.CANNOT_RETRIEVE_FIELD_DETAILS, fileId, e.getMessage());
    }
  }

  /**
   * This method is used to retrieve metadata from the Control File details table.
   * @param fileId The File ID to be retrieved
   * @throws Exception when there is an error retrieving the metadata
   */
  public List<FieldModel> retrieveControlFieldDetails(int fileId) throws EDAGMyBatisException {
    try (SqlSession session = openSession()) {
    	List<FieldModel> result = session.getMapper(IngestionMapper.class).retrieveControlFieldDetails(fileId); 
      logger.info("File ID " + fileId + " has " + (result == null ? 0 : result.size()) + " control fields");
      return result;
    } catch (PersistenceException e) {
      throw new EDAGMyBatisException(IngestionDao.CANNOT_RETRIEVE_CONTROL_FIELD_DETAILS, fileId, e.getMessage());
    }
  }

  /**
   * This method is used to retrieve metadata from the Field Standardization Rules table.
   * @param fldModel The Field Model containing the metadata of the field for which the rules
   *     have to be retrieved
   * @throws Exception when there is an error retrieving the standardization rules.
   */
  public List<Integer> retrieveFieldStdRules(FieldModel fldModel) throws EDAGMyBatisException {
    try (SqlSession session = openSession()) {
    	List<Integer> result = session.getMapper(IngestionMapper.class).retrieveFieldStdRules(fldModel);
      logger.info("File ID " + fldModel.getFileId() + ", field " + fldModel.getFieldName() + ", record type " + 
    	             fldModel.getRecordTypeInd() + " has " + (result == null ? 0 : result.size()) + " rules");
      return result;
    } catch (PersistenceException e) {
    	throw new EDAGMyBatisException(IngestionDao.CANNOT_RETRIEVE_STD_RULES, fldModel.getFileId(), 
    			                           fldModel.getFieldName(), fldModel.getRecordTypeInd(), e.getMessage());
    }
  }

  /**
   * This method is used to retrieve metadata from the Load Params table.
   * @param procId The Process ID to be retrieved
   * @throws Exception when there is an error retrieving the metadata
   */
  public List<ProcessParam> retrieveLoadParams(String procId) throws EDAGMyBatisException {
    try (SqlSession session = openSession()) {
    	List<ProcessParam> result = session.getMapper(IngestionMapper.class).retrieveLoadParams(procId);
      logger.info("Process ID " + procId + " has " + (result == null ? 0 : result.size()) + " execution parameters");
      return result;
    } catch (PersistenceException e) {
      throw new EDAGMyBatisException(IngestionDao.CANNOT_RETRIEVE_LOAD_PARAMS, procId, e.getMessage());
    }
  }
  
  public ProcessInstanceModel getProcessInstanceModel(String procInstanceId) throws EDAGMyBatisException {
  	try (SqlSession session = openSession(true)) {
  		ProcessInstanceModel result = session.getMapper(IngestionMapper.class). getProcessInstance(procInstanceId);
  		logger.debug("Process instance ID " + procInstanceId + " is " + result);
  		return result;
  	} catch (PersistenceException e) {
  		throw new EDAGMyBatisException(IngestionDao.CANNOT_GET_PROCESS_INSTANCE, procInstanceId, e.getMessage());
  	}
  }

  /**
   * This method is used to insert an entry into the Process Log table.
   * @param procInstance The Process Instance Model with the metadata to be inserted
   * @throws Exception when there is an error inserting the entry
   */
  public void insertProcessLog(ProcessInstanceModel procInstance) throws EDAGMyBatisException {
    try (SqlSession session = openSession(true)) {
      session.getMapper(IngestionMapper.class).insertProcessLog(procInstance);
      logger.debug("Process log for instance ID " + procInstance.getProcInstanceId() + " for process ID " + 
                   procInstance.getProcId() + " inserted into metastore");
    } catch (PersistenceException e) {
      throw new EDAGMyBatisException(IngestionDao.CANNOT_INSERT_PROCESS_LOG, procInstance.getProcInstanceId(), procInstance.getProcId(), e.getMessage());
    }
  }

	/**
	 * EDF 236
	 * This method is used to update the process log entry with File Size and File Arrival Time if any.
	 * @param procInstance The Process Instance Model with the metadata to be inserted
	 * @throws Exception when there is an error updating the process log entry
	 */
	public void updateProcessLogFileSizeTime(ProcessInstanceModel procInstance) throws EDAGMyBatisException {
		try (SqlSession session = openSession(true)) {
			session.getMapper(IngestionMapper.class).updateProcessLogFileSizeTime(procInstance);
			logger.debug("Process log instance ID " + procInstance.getProcInstanceId() + " source file size set to "
					+ procInstance.getSrcFileSizeBytes() + " (bytes), and source file arrival time set to "
					+ procInstance.getSrcFileArrivalTime());
		} catch (PersistenceException e) {
			throw new EDAGMyBatisException(IngestionDao.CANNOT_UPDATE_PROCESS_LOG_FILE_SIZE_TIME,
					procInstance.getProcInstanceId(), procInstance.getFileNm(), e.getMessage());
		}
	}
  
  /**
   * This method is used to update the process log entry with file name if any.
   * @param procInstance The Process Instance Model with the metadata to be inserted
   * @throws Exception when there is an error updating the process log entry
   */
  public void updateProcessLogFileName(ProcessInstanceModel procInstance) throws EDAGMyBatisException {
    try (SqlSession session = openSession(true)) {
      session.getMapper(IngestionMapper.class).updateProcessLogFileName(procInstance);
      logger.debug("Process log instance ID " + procInstance.getProcInstanceId() + " file name updated to " + procInstance.getFileNm());
    } catch (PersistenceException e) {
      throw new EDAGMyBatisException(IngestionDao.CANNOT_UPDATE_PROCESS_LOG_FILENAME, procInstance.getProcInstanceId(), procInstance.getFileNm(), e.getMessage());
    }
  }

  /**
   * This method is used to update the process log entry with end time and error if any.
   * @param procInstance The Process Instance Model with the metadata to be inserted
   * @throws Exception when there is an error updating the process log entry
   */
  public void updateProcessLog(ProcessInstanceModel procInstance) throws EDAGMyBatisException {
    try (SqlSession session = openSession(true)) {
      String suppressionMessage = procInstance.getSuppressionMessage();
      String errorTxt = procInstance.getErrorTxt();
      procInstance.setErrorTxt(StringUtils.isNotBlank(errorTxt) ? errorTxt: suppressionMessage);
      session.getMapper(IngestionMapper.class).updateProcessLog(procInstance);
      logger.debug("Process log ID " + procInstance.getProcInstanceId() + " updated to " + procInstance);
    } catch (PersistenceException e) {
      throw new EDAGMyBatisException(IngestionDao.CANNOT_UPDATE_PROCESS_LOG, procInstance.getProcInstanceId(), procInstance.getProcId(), e.getMessage());
    }
  }

  /**
   * This method is used to insert an entry into the Stage Log table.
   * @param stgModel The Stage Model containing the metadata to be inserted
   * @throws Exception when there is an error inserting the entry
   */
  public void insertStageLog(StageModel stgModel) throws EDAGMyBatisException {
    try (SqlSession session = openSession(true)) {
      session.getMapper(IngestionMapper.class).insertStageLog(stgModel);
      logger.debug("Stage model inserted: " + stgModel);
    } catch (PersistenceException e) {
      throw new EDAGMyBatisException(IngestionDao.CANNOT_INSERT_STAGE_LOG, stgModel.getProcInstanceId(), e.getMessage());
    }
  }

  /**
   * This method is used to update the stage log entry with end time, statistics and error if any.
   * @param stgModel The Stage Model containing the metadata to be inserted
   * @throws Exception when there is an error updating the stage log entry
   */
  public void updateStageLog(StageModel stgModel) throws EDAGMyBatisException {
    try (SqlSession session = openSession(true)) {
      session.getMapper(IngestionMapper.class).updateStageLog(stgModel);
      logger.debug("Stage model for process instance ID " + stgModel.getProcInstanceId() + " updated to " + stgModel);
    } catch (PersistenceException e) {
      throw new EDAGMyBatisException(IngestionDao.CANNOT_UPDATE_STAGE_LOG, stgModel.getProcInstanceId(), stgModel.getStageId(), e.getMessage());
    }
  }

  /**
   * This method is used to evaluate the business date expression on Oracle.
   * @param expression The SQL Expression to be evaluated
   * @throws Exception when there is an error evaluating the expression
   */
  public String evaluateBizDtExpr(String expression) throws EDAGMyBatisException {
    try (SqlSession session = openSession(true)) {
    	String result = session.getMapper(IngestionMapper.class).evaluateBizDtExpr(expression);
			logger.debug(expression + " can be evaluated to " + result);		
      return result;
    } catch (PersistenceException e) {
      throw new EDAGMyBatisException(IngestionDao.CANNOT_EVAL_BIZ_DATE_EXPR, expression, e.getMessage());
    }
  }

  /**
   * This method is used to retrieve the previous run status of any given process.
   * @param procInsModel The Process Instance Model
   * @throws Exception when there is an error retrieving the previous run status
   */
  public ProcessInstanceModel getPrevRunStatus(ProcessInstanceModel procInsModel) throws EDAGMyBatisException {
    try (SqlSession session = openSession(true)) {
    	logger.debug(procInsModel);
    	ProcessInstanceModel result = session.getMapper(IngestionMapper.class).getPrevRunStatus(procInsModel);
      logger.debug("Previous run status of process ID " + procInsModel.getProcId() + ", biz date " + procInsModel.getBizDate() +
      		         ", country " + procInsModel.getCountryCd() + (result == null ? " cannot be found" 
      		        		                                                          : " has process instance ID " + result.getProcInstanceId() + 
      		        		                                                            " and the status is " + result.getStatus()));
      return result;
    } catch (PersistenceException e) {
      throw new EDAGMyBatisException(IngestionDao.CANNOT_GET_PREVIOUS_RUN_STATUS, procInsModel.getProcId(), 
      		                           procInsModel.getBizDate(), procInsModel.getCountryCd(), e.getMessage());
    }
  }
  
  public ProcessInstanceModel getDLIngestionInstanceByEDWLoadInstance(ProcessInstanceModel loadInstance, String bizDate,
  		                                                                boolean successfulIngestionOnly) throws EDAGException {
  	ProcessInstanceModel result = null;
  	
  	String edwProcessId = loadInstance.getProcId();
  	SourceTableDetail edwSourceTable = retrieveSourceTableDetail(edwProcessId);
  	HadoopModel fiLoadProcess = retrieveLoadProcessByTargetTable(edwSourceTable.getSrcSchemaNm(), edwSourceTable.getSrcTblNm());
  	if(fiLoadProcess == null){
  		String fullTableName = edwSourceTable.getSrcSchemaNm() + "." + edwSourceTable.getSrcTblNm();
  		throw new EDAGException("Load process populating " + fullTableName + " is " + fiLoadProcess);
  	}
  	List<StageModel> stageModels = getStageInfoByProcessAndStageID(fiLoadProcess.getProcessId(), bizDate, 
  			                                                           loadInstance.getCountryCd(), Integer.parseInt(UobConstants.STAGE_INGEST_PROC_FINAL));
  	
  	if (stageModels != null) {
  		for (StageModel stageModel : stageModels) {
  			if (!successfulIngestionOnly || UobConstants.SUCCESS.equals(stageModel.getStatus())) {
  				result = getProcessInstanceModel(stageModel.getProcInstanceId());
  				break;
  			}
  		}
  	}
  	
  	return result;
  }

  /**
   * This method is used to retrieve the stage information for any previously 
   * existing process instance.
   * @param procInsModel The Process Instance Model
   * @throws Exception when there is an error retrieving the stage information
   */
  public List<StageModel> getStageInfo(ProcessInstanceModel procInsModel) throws EDAGMyBatisException {
    try (SqlSession session = openSession(true)) {
    	List<StageModel> result = session.getMapper(IngestionMapper.class).getStageInfo(procInsModel); 
      logger.debug("Process instance ID " + procInsModel.getProcInstanceId() + " has " + (result == null ? 0 : result.size()) + " stage(s)");
      return result;
    } catch (PersistenceException e) {
      throw new EDAGMyBatisException(IngestionDao.CANNOT_GET_STAGE_INFO, procInsModel.getProcInstanceId(), e.getMessage());
    }
  }
  
  public List<StageModel> getStageInfoByProcessAndStageID(String processID, String bizDate, String countryCode, int stageID) throws EDAGMyBatisException {
  	try (SqlSession session = openSession(true)) {
  		Map<String, Object> params = new HashMap<String, Object>();
  		params.put("processID", processID);
  		params.put("businessDate", bizDate);
  		params.put("countryCode", countryCode);
  		params.put("stageID", stageID);
  		List<StageModel> result = session.getMapper(IngestionMapper.class).getStageInfoByProcessAndStageID(params);
  		logger.debug("Process " + processID + " for country " + countryCode + " on " + bizDate + " has " + (result == null ? 0 : result.size()) + Integer.toString(stageID) + " stage(s)");
  		return result;
  	} catch (PersistenceException e) {
  		throw new EDAGMyBatisException(IngestionDao.CANNOT_GET_STAGE_INFO_BY_PROCESS_AND_STAGE_ID, stageID, processID, countryCode, bizDate, e.getMessage());
  	}
  }
  
  /**
	 * This method is used to retrieve the business date from the EDAG_BUSINESS_DATE table
	 * 
	 * @param countryCd
	 *            given country code
	 * @procInstanceId
	 * 				given procId
	 * @throws Exception
	 *             when there is an error retrieving the stage information
	 */
	public String getBizDate(@Param("countryCd") String countryCd, @Param("procId") String procId) throws EDAGMyBatisException {
		try (SqlSession session = openSession(true)) {
			String result = session.getMapper(IngestionMapper.class).getBizDate(countryCd, procId); 
			logger.debug("Biz date of process ID " + procId + ", country " + countryCd + " is " + result);
			return result;
		} catch (PersistenceException e) {
			throw new EDAGMyBatisException(IngestionDao.CANNOT_GET_BIZ_DATE, procId, countryCd, e.getMessage());
		}
	}
	
	public Map<String, Character> getTDUnsupportedCharacterReplacement(String procId, String ctryCd) throws EDAGMyBatisException {
		try (SqlSession session = openSession(true)) {
			List<TDUnsupportedCharacterReplacement> unsupportedChars = session.getMapper(IngestionMapper.class).getTDUnsupportedCharacterReplacement(procId, ctryCd);
			logger.debug("Unsupported chars for process " + procId + ", country " + ctryCd + ": " + unsupportedChars);
			
			Map<String, Character> result = TDUnsupportedCharacterReplacement.toMap(unsupportedChars);
			logger.debug("Replacement chars for process " + procId + ", country " + ctryCd + " are " + result);
			return result;
		} catch (PersistenceException | EDAGValidationException e) {
			throw new EDAGMyBatisException(IngestionDao.CANNOT_GET_TD_UNSUPPORTED_CHAR_REPLACEMENT, procId, ctryCd, e.getMessage());
		}
	}
	
	public Map<String, String> getControlCharacterReplacement(String procId, String ctryCd) throws EDAGMyBatisException {
		try (SqlSession session = openSession(true)) {
			Map<String, String> result = new HashMap<String, String>();
			
			List<Map<String, String>> listOfMap = session.getMapper(IngestionMapper.class).getControlCharsReplacement(procId, ctryCd);
			if (listOfMap != null) {
				for (Map<String, String> map : listOfMap) {
					result.put(map.get("key"), map.get("value"));
				}
			}
			
			logger.info("Control chars replacement for process " + procId + ", country " + ctryCd + ": " + result);
			return result;
		} catch (PersistenceException e) {
			throw new EDAGMyBatisException(IngestionDao.CANNOT_GET_CONTROL_CHAR_REPLACEMENT, procId, ctryCd, e.getMessage());
		}
	}
	
	public void addTDUnsupportedCharacterReplacement(TDUnsupportedCharacterReplacement toAdd) throws EDAGMyBatisException {
		try (SqlSession session = openSession(true)) {
			
		}
	}
	
	public Map<String, List<String>> getFieldNamePatterns(String procId) throws EDAGMyBatisException {
		try (SqlSession session = openSession(true)) {
			Map<String, List<String>> result = new HashMap<>();
			
			List<Map<String, String>> listOfMap = session.getMapper(IngestionMapper.class).getFieldNamePatterns(procId);
			if (listOfMap != null) {
				for (Map<String, String> map : listOfMap) {
					String value = map.get("value");
					result.put(map.get("key"), value != null ? Arrays.asList(
							value.split("\\|")).stream().map( c -> StringUtils.trim(c.toLowerCase())).collect(Collectors.toList()) 
							: new ArrayList<String>());
				}
			}
			
			logger.info("Field Name patters is retrieved for process id " + procId + result);
			return result;
		} catch (PersistenceException e) {
			throw new EDAGMyBatisException(IngestionDao.CANNOT_GET_FIELD_NAME_PATTERNS, procId, e.getMessage());
		}
	}
	
	  /**
	   * This method is used to insert an entry into the Stage Log table.
	   * @param stgModel The Stage Model containing the metadata to be inserted
	   * @throws Exception when there is an error inserting the entry
	   */
	  public void insertStageLogForAttachments(StageModel stgModel) throws EDAGMyBatisException {
	    try (SqlSession session = openSession(true)) {
	      session.getMapper(IngestionMapper.class).insertStageLogForAttachments(stgModel);
	      logger.debug("Stage model inserted: " + stgModel);
	    } catch (PersistenceException e) {
	      throw new EDAGMyBatisException(IngestionDao.CANNOT_INSERT_STAGE_LOG, stgModel.getProcInstanceId(), e.getMessage());
	    }
	  }
	  
	  /**
	   * This method is used to insert an entry into the loaded file Log table.
	   * @param procInstance The Process Instance Model with the metadata to be inserted
	   * @throws Exception when there is an error inserting the entry
	   */
	  public void insertLoadedFileLog(ProcessInstanceModel procInstance) throws EDAGMyBatisException {
	    try (SqlSession session = openSession(true)) {
	      session.getMapper(IngestionMapper.class).insertLoadedFileLog(procInstance);
	      logger.debug("Loaded File log for instance ID " + procInstance.getProcInstanceId() + " for process ID " + 
	                   procInstance.getProcId() + " inserted into metastore");
	    } catch (PersistenceException e) {
	      throw new EDAGMyBatisException(IngestionDao.CANNOT_INSERT_LOADED_FILE_LOG, procInstance.getProcInstanceId(), procInstance.getProcId(), e.getMessage());
	    }
	  }
	
	  /**
	   * This method is used to retrieve the metadata of the Adobe Process Sub Process Map table.
	   * @param procId The Process ID to be retrieved
	   * @throws Exception when there is an error retrieving the Process Master
	   */
	  public String retrieveAdobeProcSubProcMap(String procId) throws EDAGMyBatisException {
	    try (SqlSession session = openSession()) {
	    	String procIdList = session.getMapper(IngestionMapper.class).retrieveAdobeProcSubProcMap(procId);
	      logger.debug("Sub Process Id list for process ID " + procId + " is " + procIdList);
	      return procIdList;
	    } catch (PersistenceException e) {
	    	throw new EDAGMyBatisException(IngestionDao.CANNOT_RETRIEVE_PROCESS_MASTER, procId, e.getMessage());
	    }
	  }
	  
	  public String retrieveSourceDirectory(String procId) throws EDAGMyBatisException {
		    try (SqlSession session = openSession()) {
		    	String srcDir = session.getMapper(IngestionMapper.class).retrieveSourceDirectory(procId);
		      logger.debug("Source Directory for process ID " + procId + " is " + srcDir);
		      return srcDir;
		    } catch (PersistenceException e) {
		    	throw new EDAGMyBatisException(IngestionDao.CANNOT_RETRIEVE_PROCESS_MASTER, procId, e.getMessage());
		    }
		  }
	  
	  public String getAdobeProcStatus(List<String> procIdList) throws EDAGMyBatisException {
			try (SqlSession session = openSession(true)) {
				String result = "";
				result = session.getMapper(IngestionMapper.class).getAdobeProcStatus(procIdList);
						
				logger.info("Status for process " + UobConstants.ADOBE_SITE_CATALYST_MASTER_PROCESS + ": " + result);
				return result;
			} catch (PersistenceException e) {
				throw new EDAGMyBatisException(e.getMessage());
			}
		}
	  public void updateFinalProcessLog(@Param("status") String status,@Param("errorTxt") String errorTxt, @Param("procIdList") List<String> procIdList) throws EDAGMyBatisException {
			try (SqlSession session = openSession(true)) {
				session.getMapper(IngestionMapper.class).updateFinalProcessLog(status, errorTxt, procIdList);
						
			} catch (PersistenceException e) {
				throw new EDAGMyBatisException(e.getMessage());
			}
		}


	/**
	 * This method is used to retrieve all field columns in ASCENDING ORDER.
	 * @param procId the procId to be retrieved
	 * @throws Exception when there is an error retrieving the metadata
	 */
	public List<String> getOrderedFieldNames(String procId) throws EDAGMyBatisException {
		try (SqlSession session = openSession()) {
			List<String> result = session.getMapper(IngestionMapper.class).getOrderedFieldNames(procId);
			logger.info("File ID " + procId + " has " + (result == null ? 0 : result.size()) + " fields");
			return result;
		} catch (PersistenceException e) {
			throw new EDAGMyBatisException(IngestionDao.CANNOT_RETRIEVE_FIELD_DETAILS, procId, e.getMessage());
		}
	}

	public void updateStageInfo(StageModel stgModel) throws EDAGMyBatisException {
		try (SqlSession session = openSession(true)) {
			session.getMapper(IngestionMapper.class).updateStageNewStatus(stgModel);
			logger.debug("Stage model for process instance ID " + stgModel.getProcInstanceId() + " updated to " + stgModel);
		} catch (PersistenceException e) {
			throw new EDAGMyBatisException(IngestionDao.CANNOT_UPDATE_STAGE_LOG, stgModel.getProcInstanceId(), stgModel.getStageId(), e.getMessage());
		}
	}
	
		  public List<SolrCollectionModel> retrieveSolrCollection() throws EDAGMyBatisException {
		    try (SqlSession session = openSession()) {
		    	List<SolrCollectionModel> solrCollectionModelList = session.getMapper(IngestionMapper.class).retrieveSolrCollection();
		      logger.debug("SolrCollectionModel records number is " + solrCollectionModelList.size());
		      return solrCollectionModelList;
		    } catch (PersistenceException e) {
		    	throw new EDAGMyBatisException(IngestionDao.CANNOT_RETRIEVE_SOLR_COLLECTION, e.getMessage());
		    }
		  }

	  public List<AttMasterFileModel> retrieveT11HiveQL(String processId) throws EDAGMyBatisException {
		    try (SqlSession session = openSession()) {
		    	List<AttMasterFileModel> masterFileModelList = session.getMapper(IngestionMapper.class).retrieveT11HiveQL(processId);
		      logger.debug("masterFileModelList records number is " + masterFileModelList.size());
		      return masterFileModelList;
		    } catch (PersistenceException e) {
		    	throw new EDAGMyBatisException(IngestionDao.CANNOT_RETRIEVE_T11HIVEQL_FROM_METASTORE, e.getMessage());
		    }
		  }
	  public String retrievePreProcessFlag(String processId, String ctryCd) throws EDAGMyBatisException {
		  logger.debug("Retrieving Pre Processing Flag...");
		    try (SqlSession session = openSession()) {
		    	String flag = session.getMapper(IngestionMapper.class).retrievePreProcessFlag(ctryCd,processId);
		      logger.debug("Pre-Process Flag is set to " + flag);
		      return flag == null? "N": flag;
		    } catch (PersistenceException e) {
		    	throw new EDAGMyBatisException(IngestionDao.CANNOT_RETRIEVE_PRE_PROCESS_FLAG, e.getMessage());
		    }
		}
	  
	  public String retrievePreProcessClassName(String processId) throws EDAGMyBatisException {
		  logger.debug("Retrieving Pre Processing Class Name...");
		    try (SqlSession session = openSession()) {
		    	String flag = session.getMapper(IngestionMapper.class).retrievePreProcessClassName(processId);
		      logger.debug("Pre Process Class Name is set to " + flag);
		      return flag;
		    } catch (PersistenceException e) {
		    	throw new EDAGMyBatisException(IngestionDao.CANNOT_RETRIEVE_PRE_PROCESS_CLASS_NAME, e.getMessage());
		    }
		}
	  
	  public String retrievePrevRunProcInstanceId(ProcessInstanceModel procInsModel) throws EDAGMyBatisException {
		   try (SqlSession session = openSession(true)) {
		   	 logger.debug(procInsModel);
		   	 String result = session.getMapper(IngestionMapper.class).getPrevRunProcInstanceId(procInsModel);
		     logger.debug("Previous run process instance id of process ID is " + procInsModel.getProcId());
		     return result;
		    } catch (PersistenceException e) {
		      throw new EDAGMyBatisException(IngestionDao.CANNOT_GET_PREVIOUS_RUN_STATUS, procInsModel.getProcId(), 
		    		                           procInsModel.getBizDate(), procInsModel.getCountryCd(), e.getMessage());
		    }
	  	}
	
	public ProcessInstanceModel retrieveProcInstanceModelForIndexing(String procId, String bizDate, String countryCd, String sourceSystemId) throws EDAGMyBatisException {
		   try (SqlSession session = openSession(true)) {
		   	 logger.debug(procId);
		   	 ProcessInstanceModel result = session.getMapper(IngestionMapper.class).getProcInstanceModelForIndexing(bizDate, procId, sourceSystemId, countryCd,null);
		     logger.debug("Previous run process instance id of process ID is " + procId + " bizDate " + bizDate + " countryCd "+countryCd+" sourceSystemId "+sourceSystemId);
		     return result;
		    } catch (PersistenceException e) {
		    	e.printStackTrace();
		      logger.debug("Previous run process instance id of process ID is " + procId + " bizDate " + bizDate + " countryCd "+countryCd+" sourceSystemId "+sourceSystemId);
		      throw new EDAGMyBatisException(IngestionDao.CANNOT_GET_PREVIOUS_RUN_STATUS, procId);
		    }
	  }
	
	public SparkExecParamsModel getSparkExecutionProperties(String procId, String ctryCd, String paramNm) throws EDAGMyBatisException{
		try (SqlSession session = openSession(true)) {
			 SparkExecParamsModel result = session.getMapper(IngestionMapper.class).getSparkExecutionProperties(procId, ctryCd, paramNm);
		     logger.debug("Fetched Spark Properties for process ID is " + procId + " countryCd "+ctryCd);
		     return result;
		    } catch (PersistenceException e) {
		    	e.printStackTrace();
		      throw new EDAGMyBatisException(IngestionDao.CANNOT_RETRIEVE_SPARK_EXEC_PROPERTIES, procId, ctryCd);
		    }
	}
	
}
