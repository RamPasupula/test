package com.uob.edag.dao;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.ibatis.exceptions.PersistenceException;
import org.apache.ibatis.session.SqlSession;

import com.uob.edag.connection.DBMSConnectionFactory;
import com.uob.edag.exception.EDAGException.PredefinedException;
import com.uob.edag.exception.EDAGMyBatisException;
import com.uob.edag.mappers.ExportMapper;
import com.uob.edag.model.DestModel;
import com.uob.edag.model.ProcessInstanceModel;
import com.uob.edag.model.ProcessModel;
import com.uob.edag.model.CountryAttributes;
import com.uob.edag.model.ProcessParam;
import com.uob.edag.model.StageModel;

/**
 * @Author : Daya Venkatesan
 * @Date of Creation: 10/31/2016
 * @Description : The class is used for database operations to retrieve details
 *              for File Export Process.
 * 
 */

public class ExportDao extends AbstractDao {
	
  public static final PredefinedException CANNOT_GET_PROCESS_MASTER = new PredefinedException(ExportDao.class, "CANNOT_GET_PROCESS_MASTER", "Unable to retrieve process master for process ID {0}: {1}");
  public static final PredefinedException CANNOT_GET_PROCESS_COUNTRIES = new PredefinedException(ExportDao.class, "CANNOT_GET_PROCESS_COUNTRIES", "Unable to retrieve countries for process ID {0}: {1}");
	public static final PredefinedException CANNOT_GET_EXPORT_PROCESS = new PredefinedException(ExportDao.class, "CANNOT_GET_EXPORT_PROCESS", "Unable to retrieve export process for process ID {0}: {1}");
	public static final PredefinedException CANNOT_GET_PROCESS_FILENAME = new PredefinedException(ExportDao.class, "CANNOT_GET_PROCESS_FILENAME", "Unable to retrieve filename for process instance ID {0}: {1}");
	public static final PredefinedException CANNOT_GET_LOAD_PARAMS = new PredefinedException(ExportDao.class, "CANNOT_GET_LOAD_PARAMS", "Unable to retrieve load parameters for process ID {0}: {1}");
	public static final PredefinedException CANNOT_SAVE_PROCESS_LOG = new PredefinedException(ExportDao.class, "CANNOT_SAVE_PROCESS_LOG", "Unable to create log entry for process instance {0} for process ID {1}: {2}");
	public static final PredefinedException CANNOT_UPDATE_PROCESS_LOG_FILENAME = new PredefinedException(ExportDao.class, "CANNOT_UPDATE_PROCESS_LOG_FILENAME", "Unable to update process instance {0} filename to {1}: {2}");
	public static final PredefinedException CANNOT_UPDATE_PROCESS_LOG = new PredefinedException(ExportDao.class, "CANNOT_UPDATE_PROCESS_LOG", "Unable to update log for process instance {0}: {1}");
	public static final PredefinedException CANNOT_INSERT_STAGE_LOG = new PredefinedException(ExportDao.class, "CANNOT_INSERT_STAGE_LOG", "Unable to insert stage {0} for process instance {1}: {2}");
	public static final PredefinedException CANNOT_UPDATE_STAGE_LOG = new PredefinedException(ExportDao.class, "CANNOT_UPDATE_STAGE_LOG", "Unable to update stage {0} for process instance {1}: {2}");
	public static final PredefinedException CANNOT_EVALUATE_BIZ_DATE_EXPRESSION = new PredefinedException(ExportDao.class, "CANNOT_EVALUATE_BIZ_DATE_EXPRESSION", "{0} cannot be evaluated as date: {1}");
	public static final PredefinedException CANNOT_GET_PREVIOUS_RUN_STATUS = new PredefinedException(ExportDao.class, "CANNOT_GET_PREVIOUS_RUN_STATUS", "Unable to retrieve previous run status of process instance {0}: {1}");
	public static final PredefinedException CANNOT_GET_PROCESS_INSTANCE_STAGES = new PredefinedException(ExportDao.class, "CANNOT_GET_PROCESS_INSTANCE_STAGES", "Unable to retrieve process instance {0} stages: {1}");

	public ExportDao() {
		super(DBMSConnectionFactory.getFactory());
	}

	/**
   * This method is used to retrieve the metadata of the Process Master table.
   * @param procId The Process ID to be retrieved
	 * @throws EDAGMyBatisException 
   * @throws UobException when there is an error retrieving the Process Master
   */
  public ProcessModel retrieveProcessMaster(String procId) throws EDAGMyBatisException {
    try (SqlSession session = openSession()) {
    	ProcessModel result = session.getMapper(ExportMapper.class).retrieveProcessMaster(procId); 
      logger.debug(procId + " process master is " + result);
      return result;
    } catch (PersistenceException e) {
    	throw new EDAGMyBatisException(ExportDao.CANNOT_GET_PROCESS_MASTER, procId, e.getMessage());
    }
  }

  /**
   * This method is used to retrieve metadata from the Process Country table.
   * @param procId The Process ID to be retrieved
   * @throws EDAGMyBatisException 
   * @throws UobException when there is an error retrieving the metadata
   */
  public Map<String, CountryAttributes> retrieveProcessCountry(String procId) throws EDAGMyBatisException {
    try (SqlSession session = openSession()) { 
    	List<CountryAttributes> result = session.getMapper(ExportMapper.class).retrieveProcessCountry(procId); 
      logger.debug(procId + " process countries are " + result);
      
      Map<String, CountryAttributes> map = new HashMap<String, CountryAttributes>();
      for (CountryAttributes attrs : result) {
      	map.put(attrs.getCountryCode(), attrs);
      }
      
      return map;
    } catch (PersistenceException e) {
    	throw new EDAGMyBatisException(ExportDao.CANNOT_GET_PROCESS_COUNTRIES, procId, e.getMessage());
    } 
  }

  /**
   * This method is used to retrieve metadata from the Export Process table.
   * @param procId The Process ID to be retrieved
   * @throws EDAGMyBatisException 
   * @throws UobException when there is an error retrieving the metadata
   */
  public DestModel retrieveExportProcess(String procId) throws EDAGMyBatisException {
    try (SqlSession session = openSession()) {
    	DestModel result = session.getMapper(ExportMapper.class).retrieveExportProcess(procId); 
      logger.debug(procId + " export process is " + result);
      return result;
    } catch (PersistenceException e) {
    	throw new EDAGMyBatisException(ExportDao.CANNOT_GET_EXPORT_PROCESS, procId, e.getMessage());
    }
  }

  /**
   * This method is used to retrieve the process name for a process instance.
   * @param procInstanceId The Process Instance ID 
   * @throws UobException when there is an error retrieving the process name
   */
  public String retrieveProcessFileName(String procInstanceId) throws EDAGMyBatisException {
    try (SqlSession session = openSession()) {
    	String result = session.getMapper(ExportMapper.class).retrieveProcessFileName(procInstanceId);
      logger.debug(result + " is process file name for process instance ID " + procInstanceId);
      return result;
    } catch (PersistenceException e) {
    	throw new EDAGMyBatisException(ExportDao.CANNOT_GET_PROCESS_FILENAME, procInstanceId, e.getMessage());
    } 
  }

  /**
   * This method is used to retrieve metadata from the Load Params table.
   * @param procId The Process ID to be retrieved
   * @throws EDAGMyBatisException 
   * @throws UobException when there is an error retrieving the metadata
   */
  public List<ProcessParam> retrieveLoadParams(String procId) throws EDAGMyBatisException {
    try (SqlSession session = openSession()) {
    	List<ProcessParam> result = session.getMapper(ExportMapper.class).retrieveLoadParams(procId); 
      logger.debug(procId + " has " + (result == null ? 0 : result.size()) + " load params");
      return result;
    } catch (PersistenceException e) {
    	throw new EDAGMyBatisException(ExportDao.CANNOT_GET_LOAD_PARAMS, procId, e.getMessage());
    } 
  }

  /**
   * This method is used to insert an entry into the Process Log table.
   * @param procInstance The Process Instance Model with the metadata to be inserted
   * @throws UobException when there is an error inserting the entry
   */
  public void insertProcessLog(ProcessInstanceModel procInstance) throws EDAGMyBatisException {
    try (SqlSession session = openSession(true)) {
      session.getMapper(ExportMapper.class).insertProcessLog(procInstance);
      logger.debug("Process instance " + procInstance.getProcInstanceId() + " inserted for process ID " + procInstance.getProcId());
    } catch (PersistenceException e) {
    	throw new EDAGMyBatisException(ExportDao.CANNOT_SAVE_PROCESS_LOG, procInstance.getProcInstanceId(), 
    			                           procInstance.getProcId(), e.getMessage());
    } 
  }
  
  /**
   * This method is used to update the process log entry with file name if any.
   * @param procInstance The Process Instance Model with the metadata to be inserted
   * @throws UobException when there is an error updating the process log entry
   */
  public void updateProcessLogFileName(ProcessInstanceModel procInstance) throws EDAGMyBatisException {
    try (SqlSession session = openSession(true)) {
      session.getMapper(ExportMapper.class).updateProcessLogFileName(procInstance);
      logger.debug("Process instance " + procInstance.getProcInstanceId() + " filename updated to " + procInstance.getFileNm());
    } catch (PersistenceException e) {
    	throw new EDAGMyBatisException(ExportDao.CANNOT_UPDATE_PROCESS_LOG_FILENAME, procInstance.getProcInstanceId(), procInstance.getFileNm(), e.getMessage());
    }
  }

  /**
   * This method is used to update the process log entry with end time and error if any.
   * @param procInstance The Process Instance Model with the metadata to be inserted
   * @throws UobException when there is an error updating the process log entry
   */
  public void updateProcessLog(ProcessInstanceModel procInstance) throws EDAGMyBatisException {
    try (SqlSession session = openSession(true)) {
      session.getMapper(ExportMapper.class).updateProcessLog(procInstance);
      logger.debug("Process instance " + procInstance.getProcInstanceId() + " log updated");
    } catch (PersistenceException e) {
    	throw new EDAGMyBatisException(ExportDao.CANNOT_UPDATE_PROCESS_LOG, procInstance.getProcInstanceId(), e.getMessage());
    } 
  }

  /**
   * This method is used to insert an entry into the Stage Log table.
   * @param stgModel The Stage Model containing the metadata to be inserted
   * @throws UobException when there is an error inserting the entry
   */
  public void insertStageLog(StageModel stgModel) throws EDAGMyBatisException {
    try (SqlSession session = openSession(true)) {
      session.getMapper(ExportMapper.class).insertStageLog(stgModel);
      logger.debug("Stage log " + stgModel.getStageId() + " inserted for process instance " + stgModel.getProcInstanceId());
    } catch (PersistenceException e) {
    	throw new EDAGMyBatisException(ExportDao.CANNOT_INSERT_STAGE_LOG, stgModel.getStageId(), stgModel.getProcInstanceId(), e.getMessage());
    } 
  }

  /**
   * This method is used to update the stage log entry with end time, statistics and error if any.
   * @param stgModel The Stage Model containing the metadata to be inserted
   * @throws UobException when there is an error updating the stage log entry
   */
  public void updateStageLog(StageModel stgModel) throws EDAGMyBatisException {
    try (SqlSession session = openSession(true)) {
      session.getMapper(ExportMapper.class).updateStageLog(stgModel);
      logger.debug("Stage log " + stgModel.getStageId() + " updated for process instance " + stgModel.getProcInstanceId());
    } catch (PersistenceException e) {
    	throw new EDAGMyBatisException(ExportDao.CANNOT_UPDATE_STAGE_LOG, stgModel.getStageId(), stgModel.getProcInstanceId(), e.getMessage());
    } 
  }

  /**
   * This method is used to evaluate the business date expression on Oracle.
   * @param expression The SQL Expression to be evaluated
   * @throws UobException when there is an error evaluating the expression
   */
  public String evaluateBizDtExpr(String expression) throws EDAGMyBatisException {
    try (SqlSession session = openSession(true)) {
    	String result = session.getMapper(ExportMapper.class).evaluateBizDtExpr(expression); 
      logger.debug(expression + " evaluated to " + result);
      return result;
    } catch (PersistenceException e) {
    	throw new EDAGMyBatisException(ExportDao.CANNOT_EVALUATE_BIZ_DATE_EXPRESSION, expression, e.getMessage());
    }
  }

  /**
   * This method is used to retrieve the previous run status of any given process.
   * @param procInsModel The Process Instance Model
   * @throws UobException when there is an error retrieving the previous run status
   */
  public ProcessInstanceModel getPrevRunStatus(ProcessInstanceModel procInsModel) throws EDAGMyBatisException {
    try (SqlSession session = openSession(true)) {
    	ProcessInstanceModel result = session.getMapper(ExportMapper.class).getPrevRunStatus(procInsModel); 
      logger.debug("Previous run status for " + procInsModel.getProcInstanceId() + " is " + result);
      return result;
    } catch (PersistenceException e) {
    	throw new EDAGMyBatisException(ExportDao.CANNOT_GET_PREVIOUS_RUN_STATUS, procInsModel.getProcInstanceId(), e.getMessage());
    } 
  }

  /**
   * This method is used to retrieve the stage information for any previously 
   * existing process instance.
   * @param procInsModel The Process Instance Model
   * @throws UobException when there is an error retrieving the stage information
   */
  public List<StageModel> getStageInfo(ProcessInstanceModel procInsModel) throws EDAGMyBatisException {
    try (SqlSession session = openSession(true)) {
    	List<StageModel> result = session.getMapper(ExportMapper.class).getStageInfo(procInsModel); 
      logger.debug("Process instance " + procInsModel.getProcInstanceId() + " has " + (result == null ? 0 : result.size()) + " stage(s)");
      return result;
    } catch (PersistenceException e) {
    	throw new EDAGMyBatisException(ExportDao.CANNOT_GET_PROCESS_INSTANCE_STAGES, procInsModel.getProcInstanceId(), e.getMessage());
    } 
  }
}
