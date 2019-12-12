package com.uob.edag.dao;

import java.util.List;

import org.apache.ibatis.exceptions.PersistenceException;
import org.apache.ibatis.session.SqlSession;

import com.uob.edag.connection.DBMSConnectionFactory;
import com.uob.edag.exception.EDAGException.PredefinedException;
import com.uob.edag.exception.EDAGMyBatisException;
import com.uob.edag.mappers.JobPromotionMapper;
import com.uob.edag.model.FieldModel;
import com.uob.edag.model.FileModel;
import com.uob.edag.model.HadoopModel;
import com.uob.edag.model.ProcessModel;

/**
 * @Author : Daya Venkatesan
 * @Date of Creation: 10/31/2016
 * @Description : The file is used for database operations to retrieve details
 *              for Job Promotion between one environment and the other
 *              environment.
 * 
 */

public class JobPromotionDao extends AbstractDao {
	
  public static final PredefinedException CANNOT_RETRIEVE_PROCESS_MASTER = new PredefinedException(JobPromotionDao.class, "CANNOT_RETRIEVE_PROCESS_MASTER", "Unable to retrieve process model for process ID {0}: {1}");
	public static final PredefinedException CANNOT_RETRIEVE_LOAD_PROCESS = new PredefinedException(JobPromotionDao.class, "CANNOT_RETRIEVE_LOAD_PROCESS", "Unable to retrieve load process for process ID {0}: {1}");
	public static final PredefinedException CANNOT_RETRIEVE_FILE_DETAILS = new PredefinedException(JobPromotionDao.class, "CANNOT_RETRIEVE_FILE_DETAILS", "Unable to retrieve file details for process ID {0}: {1}");
	public static final PredefinedException CANNOT_RETRIEVE_FIELD_DETAILS = new PredefinedException(JobPromotionDao.class, "CANNOT_RETRIEVE_FIELD_DETAILS", "Unable to retrieve fields for file ID {0}: {1}");
	public static final PredefinedException CANNOT_RETRIEVE_CONTROL_FIELD_DETAILS = new PredefinedException(JobPromotionDao.class, "CANNOT_RETRIEVE_CONTROL_FIELD_DETAILS", "Unable to retrieve control fields for file ID {0}: {1}");

	public JobPromotionDao() {
		super(DBMSConnectionFactory.getFactory());
	}

	/**
   * This method is used to retrieve the metadata of the Process Master table.
   * @param procId The Process ID to be retrieved
   * @throws Exception when there is an error retrieving the Process Master
   */
  public ProcessModel retrieveProcessMaster(int procId) throws EDAGMyBatisException {
    try (SqlSession session = openSession()) {
    	ProcessModel result = session.getMapper(JobPromotionMapper.class).retrieveProcessMaster(procId);
      logger.debug("Process model for process ID " + procId + " is " + result);
      return result;
    } catch (PersistenceException e) {
      throw new EDAGMyBatisException(JobPromotionDao.CANNOT_RETRIEVE_PROCESS_MASTER, procId, e.getMessage());
    }
  }

  /**
   * This method is used to retrieve metadata from the Load Process table.
   * @param procId The Process ID to be retrieved
   * @throws Exception when there is an error retrieving the metadata
   */
  public HadoopModel retrieveLoadProcess(int procId) throws EDAGMyBatisException {
    try (SqlSession session = openSession()) {
    	HadoopModel result = session.getMapper(JobPromotionMapper.class).retrieveLoadProcess(procId); 
      logger.debug("Load process for process ID " + procId + " is " + result);
      return result;
    } catch (PersistenceException e) {
      throw new EDAGMyBatisException(JobPromotionDao.CANNOT_RETRIEVE_LOAD_PROCESS, procId, e.getMessage());
    }
  }
  
  /**
   * This method is used to retrieve metadata from the File Details table.
   * @param procId The Process ID to be retrieved
   * @throws Exception when there is an error retrieving the metadata
   */
  public FileModel retrieveFileDetails(int procId) throws EDAGMyBatisException {
    try (SqlSession session = openSession()) {
    	FileModel result = session.getMapper(JobPromotionMapper.class).retrieveFileDetails(procId); 
      logger.debug("File details for process ID " + procId + " is " + result);
      return result;
    } catch (PersistenceException e) {
      throw new EDAGMyBatisException(JobPromotionDao.CANNOT_RETRIEVE_FILE_DETAILS, procId, e.getMessage());
    }
  }

  /**
   * This method is used to retrieve metadata from the Field Details table.
   * @param fileId The File ID to be retrieved
   * @throws Exception when there is an error retrieving the metadata
   */
  public List<FieldModel> retrieveFieldDetails(int fileId) throws EDAGMyBatisException {
    try (SqlSession session = openSession()) {
    	List<FieldModel> result = session.getMapper(JobPromotionMapper.class).retrieveFieldDetails(fileId);
      logger.debug("File " + fileId + " has " + (result == null ? 0 : result.size()) + " field(s)");
      return result;
    } catch (PersistenceException e) {
      throw new EDAGMyBatisException(JobPromotionDao.CANNOT_RETRIEVE_FIELD_DETAILS, fileId, e.getMessage());
    }
  }

  /**
   * This method is used to retrieve metadata from the Control File details table.
   * @param fileId The File ID to be retrieved
   * @throws Exception when there is an error retrieving the metadata
   */
  public List<FieldModel> retrieveControlFieldDetails(int fileId) throws EDAGMyBatisException {
    try (SqlSession session = openSession()) {
    	List<FieldModel> result = session.getMapper(JobPromotionMapper.class).retrieveControlFieldDetails(fileId); 
      logger.debug("File " + fileId + " has " + (result == null ? 0 : result.size()) + " control field(s)");
      return result;
    } catch (PersistenceException e) {
      throw new EDAGMyBatisException(JobPromotionDao.CANNOT_RETRIEVE_CONTROL_FIELD_DETAILS, fileId, e.getMessage());
    }
  }
}
