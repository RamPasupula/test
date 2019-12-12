package com.uob.edag.utils;

import java.io.IOException;
import java.math.BigDecimal;
import java.util.List;
import java.util.stream.Collectors;

import org.apache.commons.lang3.StringUtils;
import org.apache.log4j.Logger;

import com.uob.edag.constants.UobConstants;
import com.uob.edag.dao.IngestionDao;
import com.uob.edag.exception.EDAGMyBatisException;
import com.uob.edag.exception.EDAGValidationException;
import com.uob.edag.model.ControlModel;
import com.uob.edag.model.ProcessInstanceModel;
import com.uob.edag.model.StageModel;
import com.uob.edag.model.StageModel.Status;

/**
 * @Author : Daya Venkatesan.
 * @Date of Creation: 10/24/2016
 * @Description : The file is used for handling the different stages in the 
 *                execution of the process
 * 
 */

public class StageHandler {
  protected Logger logger = Logger.getLogger(getClass());
  
  private IngestionDao ingDao = new IngestionDao();

  /** This method is used to add a log to the stage details table.
   * @param procInsModel The Process Instance Model containing the metadata to be inserted
   * @param stage The stage for which the entry is to be inserted
   * @return the Stage Model with the metadata that was inserted
   * @throws UobException 
   * @throws Exception when there is an error inserting into the Stage Log table.
   */
  public StageModel addStageLog(ProcessInstanceModel procInsModel, String stage) throws EDAGMyBatisException {
    StageModel stgModel = new StageModel();
    stgModel.setProcInstanceId(procInsModel.getProcInstanceId());
    stgModel.setStartTime(new java.sql.Timestamp(System.currentTimeMillis()));
    stgModel.setStageId(stage);
    stgModel.setStatus(Status.I.toString());
    List<StageModel> stageInfoList = ingDao.getStageInfo(procInsModel).stream().filter( f -> f.getStageId().equalsIgnoreCase(stage)).collect(Collectors.toList());
    if(stageInfoList.isEmpty()) {
    	ingDao.insertStageLog(stgModel);
    }

    logger.debug(stgModel + " added into stage log");
    return stgModel;
  }

  /** This method is used to update a stage log entry with the end time and error if any.
   * @param procInsModel The Process Instance Model
   * @param stage The stage for which the entry is to be updated
   * @param stgModel The Stage Model with the metadata of the Stage
   * @param toolProcId The Informatica Process ID
   * @throws Exception when there is an error updating the stage log
   */
  public void updateStageLog(ProcessInstanceModel procInsModel, String stage, StageModel stgModel, 
  		                       String toolProcId) throws EDAGMyBatisException {
  	logger.debug("Stage log of process instance ID " + procInsModel.getProcInstanceId() + " before update: " + stgModel);
    stgModel.setEndTime(new java.sql.Timestamp(System.currentTimeMillis()));
    String errorTxt = procInsModel.getErrorTxt();
    String suppressionMessage = procInsModel.getSuppressionMessage();
    stgModel.setError(StringUtils.isNotBlank(errorTxt) ? errorTxt: suppressionMessage);
    stgModel.setStageId(stage);
    stgModel.setStatus(!procInsModel.isDumped() && StringUtils.isNotBlank(errorTxt) ? Status.F.toString() : Status.S.toString());
    stgModel.setProcInstanceId(procInsModel.getProcInstanceId());
    
    // Update Statistics
    ControlModel ctrlModel = procInsModel.getControlModel();
    if (ctrlModel != null) {
	    if (UobConstants.STAGE_INGEST_PROC_FINAL.equalsIgnoreCase(stage) ||
	    		UobConstants.STAGE_REGISTER_PROC_TECH_RECON.equalsIgnoreCase(stage)) {
	      stgModel.setSrcRowCount(ctrlModel.getTotalRecords());
	      stgModel.setDestRowCount(ctrlModel.getTotalRecordsTarget());
	      stgModel.setETErrorRowCount(ctrlModel.getETErrorRecordCount());
	      stgModel.setUVErrorRowCount(ctrlModel.getUVErrorRecordCount());
	      stgModel.setHashsumFld(ctrlModel.getNormalizedHashSumCol());
	
	      try {
					stgModel.setSrcHashsumAmt(new BigDecimal(ctrlModel.getHashSumVal()));
				} catch (EDAGValidationException | NumberFormatException e) {
					logger.warn("Unable to get source hash sum value: " + e.getMessage());
				} catch (NullPointerException e1) {
					logger.warn("Source hash sum value is null");
				}
	      
	      try {
	      	stgModel.setDestHashsumAmt(new BigDecimal(ctrlModel.getHashSumValTarget()));
	      } catch (NumberFormatException e) {
					logger.warn("Unable to get target hash sum value: " + e.getMessage());
				} catch (NullPointerException e1) {
					logger.warn("Target hash sum value is null");
				}	
	    } else if (UobConstants.STAGE_INGEST_T1_BDM_EXEC.equalsIgnoreCase(stage) ||
	    		       UobConstants.STAGE_INGEST_T11_BDM_EXEC.equalsIgnoreCase(stage)) {
	    	stgModel.setDestDbName(ctrlModel.getTargetDbName());
		    stgModel.setDestTableName(ctrlModel.getTargetTableName());
		    stgModel.setDestErrorTableName(ctrlModel.getTargetErrorTableName());
		    stgModel.setSrcRowCount(ctrlModel.getTotalRecords());
		    stgModel.setDestRowCount(ctrlModel.getTotalRecordsTarget());
	    }
    }
    
    if (StringUtils.isNotEmpty(toolProcId)) {
      stgModel.setToolProcId(toolProcId);
    }
    
		ingDao.updateStageLog(stgModel);
  }

  /** This method is used to update a stage log entry with the end time and error if any.
   * @param procInsModel The Process Instance Model
   * @param stage The stage for which the entry is to be updated
   * @param toolProcId The Informatica Process ID
   * @throws Exception when there is an error updating the stage log
   */
  public void updateStageLog(ProcessInstanceModel procInsModel, String stage, String toolProcId) throws EDAGMyBatisException {
    StageModel stgModel = new StageModel();
    stgModel.setProcInstanceId(procInsModel.getProcInstanceId());
    this.updateStageLog(procInsModel, stage, stgModel, toolProcId);
  }
  
  /** This method is used to update the final stage log entry with the row counts for all the successful Adobe ingestion processes.
   * @param procInsModel The Process Instance Model
   * @param stage The stage for which the entry is to be updated
   * @param stgModel The Stage Model with the metadata of the Stage
   * @param toolProcId The Informatica Process ID
   * @throws Exception when there is an error updating the stage log
   */
  public void updateFinalStageLogForAdobe(ProcessInstanceModel procInsModel, String stage, int srcRowCount, int destRowCount, 
		  				String tgtDbName, String tgtTableName, String tgtErrorTableName) throws EDAGMyBatisException {
	StageModel stgModel = new StageModel();
	stgModel.setProcInstanceId(procInsModel.getProcInstanceId());
  	logger.debug("Stage log of process instance ID " + procInsModel.getProcInstanceId() + " before update: " + stgModel);
    stgModel.setEndTime(new java.sql.Timestamp(System.currentTimeMillis()));
    String errorTxt = procInsModel.getErrorTxt();
    String suppressionMessage = procInsModel.getSuppressionMessage();
    stgModel.setError(StringUtils.isNotBlank(errorTxt) ? errorTxt: suppressionMessage);
    stgModel.setStageId(stage);
    stgModel.setStatus(!procInsModel.isDumped() && StringUtils.isNotBlank(errorTxt) ? Status.F.toString() : Status.S.toString());
    stgModel.setProcInstanceId(procInsModel.getProcInstanceId());
	stgModel.setDestDbName(tgtDbName);
	stgModel.setDestTableName(tgtTableName);
	stgModel.setDestErrorTableName(tgtErrorTableName);
	stgModel.setSrcRowCount(srcRowCount);
	stgModel.setDestRowCount(destRowCount);
    ingDao.updateStageLog(stgModel);
  }

  /** This method is used to retrieve the list of stages and its info from the metadata table 
   * for any given process instance id.
   * @param procInsModel The Process Instance Model
   * @return the list of Stage Information
   * @throws Exception when there is an error retrieving the Stage Information
   */
  private List<StageModel> getStageLogs(ProcessInstanceModel procInsModel) throws EDAGMyBatisException {
    return ingDao.getStageInfo(procInsModel);
  }

  /**
   * This method is used to check if a particular step was completed for a given process
   * instance id.
   * @param procInsModel The Process Instance Model
   * @param stageCd The stage which needs to be checked for
   * @return true if the stage is already completed, false if not completed
   * @throws EDAGMyBatisException 
   * @throws Exception when there is an error during the check
   */
  public boolean checkStepCompleted(ProcessInstanceModel procInsModel, String stageCd) throws EDAGMyBatisException {
		return checkStepCompleted(getStageLogs(procInsModel), procInsModel, stageCd);
  }

  /**
   * This method is used to check if a particular step was completed for a given process
   * instance id.
   * @param stages The list of stages that the process has completed
   * @param procInsModel The Process Instance Model
   * @param stageCd The stage which needs to be checked for
   * @return true if the stage is already completed, false if not completed
   * @throws IOException when there is an error during the check
   */
  public boolean checkStepCompleted(List<StageModel> stages, ProcessInstanceModel procInsModel, 
  		                              String stageCd) {
  	boolean result = false;

  	if (stages != null) {
	    for (StageModel stage : stages) {
	      if (stage.getStageId().equalsIgnoreCase(stageCd) && UobConstants.SUCCESS.equalsIgnoreCase(stage.getStatus())) {
	        result = true;
	        break;
	      }
	    }
  	}
    
    logger.debug("Stage " + stageCd + (result ? " is completed" : " is not completed"));
    return result;
  }

  /** This method is used to retrieve the Stage Information for a given process instance.
   * @param procInsModel The Process Instance Model
   * @param stageCd The Stage ID for which the information is to be generated
   * @return the Stage Model with the metadata of the stage
   * @throws Exception when there is an error retrieving the stage information
   */
  public StageModel getStageInfo(ProcessInstanceModel procInsModel, String stageCd) throws EDAGMyBatisException {
		return getStageInfo(getStageLogs(procInsModel), stageCd);
  }

  /** This method is used to retrieve the Stage Information for a given process instance.
   * @param stages The List of Stages that the Process has completed.
   * @param stageCd The Stage ID for which the information is to be generated
   * @return the Stage Model with the metadata of the stage
   * @throws Exception when there is an error retrieving the stage information
   */
  public StageModel getStageInfo(List<StageModel> stages, String stageCd) {
    for (StageModel stage : stages) {
      if (stage.getStageId().equalsIgnoreCase(stageCd)) {
      	logger.debug("Stage of " + stageCd + " is " + stage);
        return stage;
      }
    }
    
    return null;
  }
  
  /** This method is used to add a log to the stage details table primarily for file attachments.
   * @param procInsModel The Process Instance Model containing the metadata to be inserted
   * @param stageModel The stage model  for which the entry is to be inserted
   * @return the Stage Model with the metadata that was inserted
   * @throws UobException 
   * @throws Exception when there is an error inserting into the Stage Log table.
   */
  public StageModel addStageLogForAttachments(ProcessInstanceModel procInsModel, StageModel stageModel) throws EDAGMyBatisException {
	ingDao.insertStageLogForAttachments(stageModel);
    logger.debug(stageModel + " added into stage log");
    return stageModel;
  }
 
  public void updateStageLog(String procInstanceId, String errorReason, String status, String stageId) throws EDAGMyBatisException {
	StageModel stgModel = new StageModel();
	stgModel.setProcInstanceId(procInstanceId);
	stgModel.setStatus(status);
	stgModel.setStageId(stageId);
	stgModel.setError(errorReason);
	ingDao.updateStageInfo(stgModel);
  }
  
}
