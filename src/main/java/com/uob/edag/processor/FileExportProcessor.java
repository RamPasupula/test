package com.uob.edag.processor;

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.InputStreamReader;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.List;

import org.apache.commons.lang.StringUtils;

import com.uob.edag.constants.UobConstants;
import com.uob.edag.exception.EDAGException;
import com.uob.edag.exception.EDAGIOException;
import com.uob.edag.exception.EDAGProcessorException;
import com.uob.edag.exception.EDAGSSHException;
import com.uob.edag.exception.EDAGValidationException;
import com.uob.edag.model.ControlModel;
import com.uob.edag.model.ExportModel;
import com.uob.edag.model.ProcessInstanceModel;
import com.uob.edag.model.ProcessModel;
import com.uob.edag.model.ProcessParam;
import com.uob.edag.model.StageModel;
import com.uob.edag.utils.FileUtility;
import com.uob.edag.utils.FileUtilityFactory;
import com.uob.edag.utils.FileUtils;
import com.uob.edag.utils.PropertyLoader;
import com.uob.edag.utils.RemoteFileUtils;
import com.uob.edag.utils.StageHandler;
import com.uob.edag.utils.FileUtility.OperationType;

/**
 * @Author : Ganapathy Raman & Daya Venkatesan
 * @Date of Creation: 01/11/2017
 * @Description : The class is used for executing the file extraction from TAS.
 * 
 */
public class FileExportProcessor extends ExportBaseProcessor {
  private String deploymentNode = null;
  private FileUtility localFileUtil;
  private FileUtility fileUtil;
  
  /**
   * This method is used to run the Process Initialization of an Export Process.
   * @param procInsModel The Process Instance Model object
   * @param procModel The Process Object with the metadata of the Process to be exported
   * @param bizDate The Business Date for Export
   * @param cleanup Value of the cleanup flag
   * @param prevStatusModel Process Instance Model object for any previously run instance 
   *     for the same date and country
   * @param forceFileName The name of the file which needs to be force started from the beginning
   * @throws Exception when there is any error in the Process Initialization
   */
  private void runProcessInitialization(ProcessInstanceModel procInsModel, ProcessModel procModel, String bizDate, 
  																			boolean cleanup, ProcessInstanceModel prevStatusModel, 
  																			String forceFileName) throws EDAGException {
  	EDAGException instanceException = null;
    try {
      stgHndle.addStageLog(procInsModel, UobConstants.STAGE_EXPORT_PROC_INIT);

      // Biz Date Validation
      if (bizDate == null) {
        throw new EDAGValidationException(EDAGValidationException.NULL_VALUE, "Business Date", "Business date cannot be null");
      }
      
      // Create TPT Parameter File
      String tmpParamFile = PropertyLoader.getProperty(UobConstants.EXP_TMP_PARAM_FILE_PATH);
      ExportModel exportModel = procModel.getDestInfo();
      tmpParamFile = tmpParamFile.replace(UobConstants.SRC_SYS_PARAM, procModel.getSrcSysCd())
              									 .replace(UobConstants.DBNM_PARAM, exportModel.getSrcDbName())
              									 .replace(UobConstants.TBLNM_PARAM, exportModel.getSrcTblName())
              									 .replace(UobConstants.COUNTRY_PARAM,procInsModel.getCountryCd());
      createExportParamFile(procInsModel, procModel, bizDate, tmpParamFile);
      
      String paramDirectory = PropertyLoader.getProperty(UobConstants.EXP_PARAM_FILE_DIR_PATH);
      
      File sourceParamFile = new File(tmpParamFile);
    	File targetParamFile = new File(paramDirectory, sourceParamFile.getName());
      try {
      	logger.debug("Copying file " + sourceParamFile.getPath() + " to " + targetParamFile.getPath());
      	localFileUtil.copyFile(tmpParamFile, targetParamFile.getPath());
      } catch (EDAGIOException e) {
      	logger.warn("Copying " + sourceParamFile.getPath() + " to " + targetParamFile.getPath() + 
      			         " using SSH since normal file copy failed: " + e.getMessage());
	      RemoteFileUtils rutil = new RemoteFileUtils(deploymentNode);
	      rutil.sshConnectAndWriteFile(paramDirectory, tmpParamFile);
      }
      
      FileUtils fileUtil = new FileUtils();
      fileUtil.deleteFile(tmpParamFile);

      stgHndle.updateStageLog(procInsModel, UobConstants.STAGE_EXPORT_PROC_INIT, null);
    } catch (EDAGException excp) {
    	instanceException = excp;
    } catch (Exception excp) {
    	instanceException = new EDAGProcessorException(EDAGProcessorException.CANNOT_INIT_PROCESS, "Export", excp.getMessage());
    } finally {
    	if (instanceException != null) {
    		procInsModel.setException(instanceException); 
				stgHndle.updateStageLog(procInsModel, UobConstants.STAGE_EXPORT_PROC_INIT, null);
				throw instanceException;
    	}
    }
  }

  /**
   * This method is used to create the parameter file to be used by TPT Script for Export.
   * @param procInsModel The Process Instance Model object
   * @param procModel The Process Model object containing the metadata of the Export Process
   * @param bizDate The Business Date of Export
   * @param paramFilePath The file path where the parameter file is to be created
   * @throws EDAGIOException 
   * @throws Exception when there is an error creating the Parameter file
   */
  private void createExportParamFile(ProcessInstanceModel procInsModel, ProcessModel procModel, String bizDate, 
  		                               String paramFilePath) throws EDAGIOException {
    logger.info("Going to create TPT param file: " + paramFilePath);
    ExportModel destModel = procModel.getDestInfo();
    List<ProcessParam> procParamList = procModel.getProcParam();
    String whereClause = "";
    for (ProcessParam param : procParamList) {
      if (UobConstants.FILTER_PARAM_NAME.equalsIgnoreCase(param.getParamName())) {
        whereClause = param.getParamValue().toLowerCase().replace(UobConstants.BIZ_DATE_PARAM, "''" + bizDate + "''")
                					 															 .replace(UobConstants.COUNTRY_PARAM, "''" + procInsModel.getCountryCd() + "''");
      }
    }
    
    ControlModel controlModel = procInsModel.getControlModel();
    String databaseAddress = PropertyLoader.getProperty(controlModel.getSrcSystemCd() + ".DbAddress");
    String databaseUid = PropertyLoader.getProperty(controlModel.getSrcSystemCd() + ".UserId");
    String databasePwd = PropertyLoader.getProperty(controlModel.getSrcSystemCd() + ".PasswordId");
    
    StringBuilder input = new StringBuilder();
    input.append(destModel.getSrcDbName());
    input.append(UobConstants.PIPE);
    input.append(destModel.getSrcTblName());
    input.append(UobConstants.PIPE);
    input.append(databaseAddress);
    input.append(UobConstants.PIPE);
    input.append(databaseUid);
    input.append(UobConstants.PIPE);
    input.append(databasePwd);
    input.append(UobConstants.PIPE);
    input.append(controlModel.getBizDate());
    input.append(UobConstants.PIPE);
    input.append(controlModel.getCtryCd());
    input.append(UobConstants.PIPE);
    input.append(whereClause);
    input.append(UobConstants.PIPE);
    input.append(controlModel.getSrcSystemCd());
    input.append(UobConstants.NEWLINE);
    
    FileUtils fileUtil = new FileUtils();
    fileUtil.writeToFile(input.toString(), new File(paramFilePath));
    
    logger.info("Parameter file: " + paramFilePath + " created successfully");
  }

  /**
   * This method is used to run the TPT script for exporting the file into the staging area.
   * @param procInsModel The Process Instance Model object
   * @param procModel The Process Model object containing the metadata of the Export Process
   * @param paramFilePath The parameter file path to be used by TPT script
   * @throws EDAGException 
   * @throws Exception when there is any error in the TPT process
   */
  private void runTptProcess(ProcessInstanceModel procInsModel, ProcessModel procModel, 
  		                       String paramFilePath) throws EDAGException {
    String toolId = null;
    EDAGException instanceException = null;
    try {
      stgHndle.addStageLog(procInsModel, UobConstants.STAGE_EXPORT_TPT_EXEC);

      String tptRunCommand = PropertyLoader.getProperty(UobConstants.TPT_EXEC_COMMAND);
      
      ExportModel exportModel = (ExportModel) procModel.getDestInfo();
      tptRunCommand = tptRunCommand.replace(UobConstants.PARAM_FILE_PARAM, paramFilePath)
              										 .replace(UobConstants.SRC_SYS_PARAM, procModel.getSrcSysCd())
              										 .replace(UobConstants.DBNM_PARAM, exportModel.getSrcDbName())
              										 .replace(UobConstants.TBLNM_PARAM, exportModel.getSrcTblName())
              										 .replace(UobConstants.COUNTRY_PARAM, procInsModel.getCountryCd());
      logger.debug("Going to execute TPT command locally: " + tptRunCommand);

      RemoteFileUtils rutil = new RemoteFileUtils(deploymentNode);
      int exitStatus = -1;
      try {
	      Process proc = Runtime.getRuntime().exec(tptRunCommand);
	      exitStatus = proc.waitFor();
	      BufferedReader inputReader = new BufferedReader(new InputStreamReader(proc.getInputStream()));
	      BufferedReader errorReader = new BufferedReader(new InputStreamReader(proc.getErrorStream()));
	      
	      String line = null;
	      logger.debug("Start of process output stream");
	      while ((line = inputReader.readLine()) != null) {
	      	logger.info(line);
	      }
	      logger.debug("End of process output stream");
	      
	      boolean hasError = false;
	      logger.debug("Start of process error stream");
	      while ((line = errorReader.readLine()) != null) {
	      	logger.error(line);
	      	hasError = true;
	      }
	      logger.debug("End of process error stream");
	      
	      if (hasError) {
	      	throw new EDAGProcessorException(EDAGProcessorException.EXT_CMD_ERROR, tptRunCommand, exitStatus);
	      } 
      } catch (EDAGException | IOException e) {
		  throw e;
      	//logger.debug("Executing '" + tptRunCommand + "' as remote command using SSH since local execution failed: " + e.getMessage());
	      //exitStatus = rutil.sshConnectAndExecuteCommand(tptRunCommand);
      }
      
      if (exitStatus == 0) {
        logger.info("Export Process completed successfully");
      } else {
        throw new EDAGProcessorException(EDAGProcessorException.EXT_CMD_ERROR, tptRunCommand, exitStatus);
      }
      
      // Archive TPT Param File
      String paramArchiveFilePath = PropertyLoader.getProperty(UobConstants.EXP_PARAM_FILE_PATH_ARCHIVE);
      paramArchiveFilePath = paramArchiveFilePath.replace(UobConstants.SRC_SYS_PARAM, procModel.getSrcSysCd())
      		                                       .replace(UobConstants.DBNM_PARAM, exportModel.getSrcDbName())
      		                                       .replace(UobConstants.TBLNM_PARAM, exportModel.getSrcTblName())
      		                                       .replace(UobConstants.COUNTRY_PARAM, procInsModel.getCountryCd())
      		                                       .replaceAll(UobConstants.BIZ_DATE_PARAM, procInsModel.getBizDate());

      // Archive BDM Param File
    	// check if target archive directory exists
    	File paramArchiveFile = new File(paramArchiveFilePath);
    	File paramArchiveDir = paramArchiveFile.getParentFile();
    	if (paramArchiveDir.isDirectory()) {
    		// check if target archive file exists
    		if (paramArchiveFile.isFile()) {
    			// rename the existing archived param file
    			String newName = paramArchiveFile.getName() + "." + new SimpleDateFormat("yyyyMMddHHmmss").format(System.currentTimeMillis());
    			File newFile = new File(paramArchiveDir, newName);
    			try {
    				logger.debug("Moving " + paramArchiveFile.getPath() + " to " + newFile.getPath());
						org.apache.commons.io.FileUtils.moveFile(paramArchiveFile, newFile);
					} catch (IOException e) {
						throw new EDAGIOException(EDAGIOException.CANNOT_MOVE_FILE, paramArchiveFile.getPath(), newFile.getPath(), e.getMessage());
					}
    		}
    	}
    	
    	fileUtil.moveFile(paramFilePath, paramArchiveFilePath);
      stgHndle.updateStageLog(procInsModel, UobConstants.STAGE_EXPORT_TPT_EXEC, toolId);
    } catch (EDAGException excp) {
    	instanceException = excp;
    } catch (Exception excp) {
    	instanceException = new EDAGException(excp.getMessage());
    } finally {
    	if (instanceException != null) {
	      procInsModel.setException(instanceException);
			  stgHndle.updateStageLog(procInsModel, UobConstants.STAGE_EXPORT_TPT_EXEC, toolId);
	      
	      throw instanceException;
    	}
    }
  }

  /**
   * This method is used to run the process finalization which includes error threshold validation,
   *     Row Count Validation and Hash Sum Validation. It also moves the file into the appropriate
   *     export directory
   * @param procInsModel The Process Instance Model object
   * @param procModel The Process Model object containing the metadata of the Export Process
   * @throws Exception when there is an error in the finalization process
   */
  private void runProcessFinalization(ProcessInstanceModel procInsModel, ProcessModel procModel) throws EDAGException {
  	EDAGException instanceException = null;
    try {
      stgHndle.addStageLog(procInsModel, UobConstants.STAGE_EXPORT_PROC_FINAL);

      // Copy File to Target Directory
      copyFileToTarget(procInsModel, procModel);

      stgHndle.updateStageLog(procInsModel, UobConstants.STAGE_EXPORT_PROC_FINAL, null);
    } catch (EDAGException excp) {
    	instanceException = excp;
    } catch (Exception excp) {
    	instanceException = new EDAGException(excp.getMessage());
    } finally {	
    	if (instanceException != null) {
	      procInsModel.setException(instanceException); 
	
				stgHndle.updateStageLog(procInsModel, UobConstants.STAGE_EXPORT_PROC_FINAL, null);
	
	      throw instanceException;
    	}
    }
  }

  /**
   * This method is used to copy the source file from the temporary folder to the target folder.
   * @param procInsModel The Process Instance Model object
   * @param procModel The Process Model object containing the metadata of the Export Process
   * @throws EDAGSSHException 
   * @throws Exception when there is an error moving the file to the target folder.
   */
  private void  copyFileToTarget(ProcessInstanceModel procInsModel, ProcessModel procModel) throws EDAGException {
    ExportModel exportModel = procModel.getDestInfo();
    String targetDirectory = exportModel.getTgtDirName();
    String targetFileName = exportModel.getTgtFileName();
    String fullTargetFileName = targetDirectory + "/" + targetFileName;
    fullTargetFileName = fullTargetFileName.replace(UobConstants.COUNTRY_PARAM, procInsModel.getCountryCd().toLowerCase());
    
    String currDirectory  = PropertyLoader.getProperty(UobConstants.EXP_FILE_PATH);
    String currFileName = exportModel.getSrcTblName() + "_" + procInsModel.getCountryCd() + ".dat";
    String newCurrFileName = currDirectory + "/" + currFileName;
    
    // check if target file exists
    File targetFile = new File(fullTargetFileName);
    targetFileName = targetFile.getName();
    File targetDir = targetFile.getParentFile();
    if (targetDir.isDirectory() && targetFile.isFile()) {
    	// target file exists, check 'previous' subfolder
    	File previousDir = new File(targetDir, "previous");
    	if (previousDir.isDirectory()) {
    		// check if target file exists
    		File previousTargetFile = new File(previousDir, targetFileName + "." + procInsModel.getBizDate() + ".gz");
    		if (previousTargetFile.isFile()) {
    			// rename file in previous folder
    			File renamedPreviousTargetFile = new File(previousDir, previousTargetFile.getName() + "." + 
    			                                                       new SimpleDateFormat("yyyyMMddHHmmss").format(System.currentTimeMillis()));
  				logger.debug("Moving file " + previousTargetFile.getPath() + " to " + renamedPreviousTargetFile.getPath());
  				localFileUtil.moveFile(previousTargetFile.getPath(), renamedPreviousTargetFile.getPath());
    		}
    		
    		// now zip target file into previous folder
  			logger.debug("GZipping file " + targetFile.getPath() + " to " + previousTargetFile.getPath());
				FileUtils.gzipFile(targetFile, previousTargetFile);
				
    		// delete target file
    		if (!targetFile.delete()) {
    			throw new EDAGIOException(EDAGIOException.CANNOT_DELETE_FILE, targetFile.getPath(), "Unable to delete file");
    		}
     	}
    }
    
  	logger.debug("Going to move file from: " + newCurrFileName + " to: " + fullTargetFileName);
  	fileUtil.moveFile(newCurrFileName, fullTargetFileName);
  }

  /**
   * This method is used to set the control information into the Control Model
   * @param procInstanceModel The Process Instance Model
   * @param procModel The process model with the metadata
   * @param bizDate The Biz Date for which the export is run
   * @param ctryCd The country code for which the export is done
   * @throws ParseException when there is an error setting the control information.
   */
  private void setControlInfo(ProcessInstanceModel procInstanceModel, ProcessModel procModel, String bizDate, 
  		                        String ctryCd) {
      
    ControlModel ctrlModel = new ControlModel();
    
    // Set Biz Date
    ctrlModel.setBizDate(bizDate);
    logger.debug("Control Information: Biz Date is: " + bizDate);

    // Set Source System
    ctrlModel.setSrcSystemCd(procModel.getSrcSysCd());
    logger.debug("Control Information: Src System is: " + procModel.getSrcSysCd());

    // Set Country
    ctrlModel.setCtryCd(ctryCd);
    logger.debug("Control Information: Country is: " + ctryCd);

    procInstanceModel.setControlModel(ctrlModel);
  }


  /**
   * This method is used to run the Export Process for file based sources.
   * @param procInstanceModel The process Instance Model object
   * @param procModel The Process Model object of the Export Process
   * @param bizDate The business date of the process
   * @param ctryCd The country code of the process
   * @param forceRerun Indicator to show if the Export has to be force rerun from the start
   * @param forceFileName Name of the file which has to force exported
   * @throws EDAGException 
   * @throws Exception when there is an error in the file export process
   */
  public void runFileExport(ProcessInstanceModel procInstanceModel, ProcessModel procModel, String bizDate, 
  		                      String ctryCd, boolean forceRerun, String forceFileName) throws EDAGException {
    logger.info("Going to run File Export for process:" + procModel.getProcId());
    
    fileUtil = FileUtilityFactory.getFileUtility(procModel);
    localFileUtil = FileUtilityFactory.getFileUtility(procModel, OperationType.Local);
    
    boolean cleanup = false;
    boolean procInitCompleted = false;
    String paramFilePath = null; 
    ProcessInstanceModel prevStatusModel = null;
    List<StageModel> stgModelList = null;
    StageHandler stgHndlr = new StageHandler();
    EDAGException instanceException = null;
    try {
      deploymentNode = procModel.getDeployNodeNm();
    
      // Verify Previous Run Status
      prevStatusModel = exportDao.getPrevRunStatus(procInstanceModel);
      if (!forceRerun) {
        if (prevStatusModel == null) {
          logger.info("No Previous Instance of this Process; Running a new instance");
        } else {
          if (UobConstants.SUCCESS.equalsIgnoreCase(prevStatusModel.getStatus())) {
            cleanup = true;
            logger.info("Found existing completed instance of this process; Going to clean up and restart the process");
            prevStatusModel = null;
          } else if (UobConstants.RUNNING.equalsIgnoreCase(prevStatusModel.getStatus())) {
            throw new EDAGProcessorException(EDAGProcessorException.RUNNING_INSTANCE_EXISTS, 
            		                             prevStatusModel.getProcInstanceId(), prevStatusModel.getProcId());
          } else {
            logger.info("Found existing instance of this process; Going to continue the same");
            stgModelList = exportDao.getStageInfo(prevStatusModel);
          }
        }
      } else {
        if (prevStatusModel != null && UobConstants.RUNNING.equalsIgnoreCase(prevStatusModel.getStatus())) {
          throw new EDAGProcessorException(EDAGProcessorException.RUNNING_INSTANCE_EXISTS,
          		                             prevStatusModel.getProcInstanceId(), prevStatusModel.getProcId());
        }
        
        logger.info("Force Rerun is set. Not checking Previous Run Status");
        if (StringUtils.isNotEmpty(forceFileName)) {
          procModel.getSrcInfo().setSourceDirectory(forceFileName);
        }
        
        prevStatusModel = null;
      }
      
      setControlInfo(procInstanceModel, procModel, bizDate, ctryCd);
      
      procInitCompleted = stgHndlr.checkStepCompleted(stgModelList, procInstanceModel, UobConstants.STAGE_EXPORT_PROC_INIT);
      
      ExportModel exportModel = (ExportModel) procModel.getDestInfo();
      paramFilePath = PropertyLoader.getProperty(UobConstants.EXP_PARAM_FILE_DIR_PATH) + 
      		            PropertyLoader.getProperty(UobConstants.EXP_PARAM_FILE_NM_PATH);
      paramFilePath = paramFilePath.replace(UobConstants.SRC_SYS_PARAM, procModel.getSrcSysCd())
      														 .replace(UobConstants.DBNM_PARAM, exportModel.getSrcDbName())
      														 .replace(UobConstants.TBLNM_PARAM, exportModel.getSrcTblName())
      														 .replace(UobConstants.COUNTRY_PARAM, ctryCd);
    } catch (EDAGException excp) {
    	instanceException = excp;
    } catch (Exception excp) {
    	instanceException = new EDAGException(excp.getMessage());
    } finally {	
    	if (instanceException != null) {
	      procInstanceModel.setException(instanceException); 
	      stgHndle.addStageLog(procInstanceModel, UobConstants.STAGE_EXPORT_PROC_INIT);
	      stgHndle.updateStageLog(procInstanceModel, UobConstants.STAGE_EXPORT_PROC_INIT, null);
	      
	      throw instanceException;
    	}
    }
  
    if (forceRerun || ! procInitCompleted || cleanup) {
      runProcessInitialization(procInstanceModel, procModel, bizDate, cleanup, prevStatusModel, forceFileName);
    } else {
      logger.info("Stage: Process Initialization already completed; Skipping stage");
    }

    ControlModel controlModel = procInstanceModel.getControlModel();
    logger.debug("after runProcessInitialization: controlModel: " + controlModel);

    // Run TPT Script
    boolean tptCompleted = stgHndlr.checkStepCompleted(stgModelList, procInstanceModel, UobConstants.STAGE_EXPORT_TPT_EXEC);
    if (forceRerun || !tptCompleted || cleanup) {
      runTptProcess(procInstanceModel, procModel, paramFilePath);
    } else {
      logger.info("Stage: TPT Execution Process already completed; Skipping stage");
    }
    
    // Process Finalization
    boolean procFinalCompleted = stgHndlr.checkStepCompleted(stgModelList, procInstanceModel, UobConstants.STAGE_EXPORT_PROC_FINAL);
    if (forceRerun || !procFinalCompleted || cleanup) {
      runProcessFinalization(procInstanceModel, procModel);
    } else {
      logger.info("Stage: Process Finalization already completed; Skipping stage");
    }

    logger.info("File Export completed: " + procModel.getProcId());
  }
}
