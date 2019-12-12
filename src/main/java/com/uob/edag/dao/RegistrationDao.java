package com.uob.edag.dao;

import java.util.List;

import org.apache.ibatis.exceptions.PersistenceException;
import org.apache.ibatis.session.SqlSession;

import com.uob.edag.connection.DBMSConnectionFactory;
import com.uob.edag.exception.EDAGException.PredefinedException;
import com.uob.edag.exception.EDAGMyBatisException;
import com.uob.edag.mappers.RegistrationMapper;
import com.uob.edag.model.RuleModel;

/**
 * @Author : Daya Venkatesan
 * @Date of Creation: 10/24/2016
 * @Description : The file is used for performing database operations related to
 *              the registration of processes.
 * 
 */

public class RegistrationDao extends AbstractDao {
	
  public static final PredefinedException CANNOT_CHECK_FILE_EXISTENCE = new PredefinedException(RegistrationDao.class, "CANNOT_CHECK_FILE_EXISTENCE", "Unable to check if file for {0} exists: {1}");
	public static final PredefinedException CANNOT_GET_PROC_ID = new PredefinedException(RegistrationDao.class, "CANNOT_GET_PROC_ID", "Unable to get process ID for {0}: {1}");
	public static final PredefinedException CANNOT_GET_STD_RULES = new PredefinedException(RegistrationDao.class, "CANNOT_GET_STD_RULES", "Unable to get standardization rules: {0}");
	public static final PredefinedException CANNOT_GET_NEXT_ALERT_ID = new PredefinedException(RegistrationDao.class, "CANNOT_GET_NEXT_ALERT_ID", "Unable to get next alert ID: {0}");
	public static final PredefinedException CANNOT_GET_NEXT_FILE_ID = new PredefinedException(RegistrationDao.class, "CANNOT_GET_NEXT_FILE_ID", "Unable to get next file ID: {0}");
	public static final PredefinedException CANNOT_GET_NEXT_CONTROL_FILE_ID = new PredefinedException(RegistrationDao.class, "CANNOT_GET_NEXT_CONTROL_FILE_ID", "Unable to get next control file ID: {0}");

	public RegistrationDao() {
		super(DBMSConnectionFactory.getFactory());
	}

	/**
   * This method is used to check if a file already exists in the metadata tables.
   * @param procId The process id to be checked
   * @return true if the file already exists, false if the file doesnt exist
	 * @throws UobMyBatisException 
   * @throws Exception when there is an error checking if the file exist
   */
  public boolean checkFileExists(String procId) throws EDAGMyBatisException {
    try (SqlSession session = openSession()) {
      int count = session.getMapper(RegistrationMapper.class).checkFileExists(procId);
      logger.debug("File for " + procId + (count > 0 ? " exists" : " doesn't exist"));
      return count > 0;
    } catch (PersistenceException e) {
    	throw new EDAGMyBatisException(RegistrationDao.CANNOT_CHECK_FILE_EXISTENCE, procId, e.getMessage());
    }
  }

  /** 
   * This method is used to retrieve the process id for the given file.
   * @param fileName The name of the File/Process
   * @return the Process ID
   * @throws Exception when there is an error retrieving the Process ID
   */
  public String getProcId(String fileName) throws EDAGMyBatisException {
    try (SqlSession session = openSession()) { 
    	String processID = session.getMapper(RegistrationMapper.class).getProcId(fileName);
      logger.debug("Process ID for " + fileName + " is " + processID);
      return processID;
    } catch (PersistenceException e) {
    	throw new EDAGMyBatisException(RegistrationDao.CANNOT_GET_PROC_ID, fileName, e.getMessage());
    }
  }

  /**
   * This method is used to retrieve the list of standardization rules available in the metadata
   * @return the List of Rule Model objects
   * @throws Exception when there is an error retrieving the standardization rules.
   */
  public List<RuleModel> retrieveStdRules() throws EDAGMyBatisException {
    try (SqlSession session = openSession()) {
    	List<RuleModel> ruleModels = session.getMapper(RegistrationMapper.class).retrieveStdRules();
      logger.debug((ruleModels == null ? 0 : ruleModels.size()) + " standardization rules retrieved");
      return ruleModels;
    } catch (PersistenceException e) {
    	throw new EDAGMyBatisException(RegistrationDao.CANNOT_GET_STD_RULES, e.getMessage());
    }
  }

  /**
   * This method is used to retrieve the next alert id from the sequence.
   * @return the alert ID
   * @throws Exception when there is an error retrieving the next alert id
   */
  public int selectAlertId() throws EDAGMyBatisException {
    try (SqlSession session = openSession(true)) { 
    	int alertID = session.getMapper(RegistrationMapper.class).selectAlertId();
    	logger.debug("Next alert ID is " + alertID);
      return alertID;
    } catch (PersistenceException e) {
      throw new EDAGMyBatisException(RegistrationDao.CANNOT_GET_NEXT_ALERT_ID, e.getMessage());
    }
  }

  /**
   * This method is used to select the next file id from the sequence.
   * @return the file id
   * @throws Exception when there is an error retrieving the next file id.
   */
  public int selectFileId() throws EDAGMyBatisException {
    try (SqlSession session = openSession(true)) {
    	int fileID = session.getMapper(RegistrationMapper.class).selectFileId();
    	logger.debug("Next file ID is " + fileID);
      return fileID;
    } catch (PersistenceException e) {
    	throw new EDAGMyBatisException(RegistrationDao.CANNOT_GET_NEXT_FILE_ID, e.getMessage());
    }
  }

  /**
   * This method is used to select the next control file id from the sequence.
   * @return the control file id
   * @throws Exception when there is an error retrieving the next control file id.
   */
  public int selectControlFileId() throws EDAGMyBatisException {
    try (SqlSession session = openSession(true)) {
    	int controlFileID = session.getMapper(RegistrationMapper.class).selectControlFileId();
      logger.debug("Next control file ID is " + controlFileID);
      return controlFileID;
    } catch (PersistenceException e) {
      throw new EDAGMyBatisException(RegistrationDao.CANNOT_GET_NEXT_CONTROL_FILE_ID, e.getMessage());
    }
  }
}
