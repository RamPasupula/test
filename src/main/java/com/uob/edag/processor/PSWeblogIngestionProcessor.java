package com.uob.edag.processor;


import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.StandardCopyOption;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Enumeration;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.regex.Pattern;
import java.util.zip.ZipEntry;
import java.util.zip.ZipFile;

import org.apache.commons.collections.MapUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.fs.Path;

import com.uob.edag.constants.UobConstants;
import com.uob.edag.dao.HiveDao;
import com.uob.edag.exception.EDAGException;
import com.uob.edag.exception.EDAGIOException;
import com.uob.edag.exception.EDAGMyBatisException;
import com.uob.edag.exception.EDAGProcessorException;
import com.uob.edag.exception.EDAGSSHException;
import com.uob.edag.exception.EDAGValidationException;
import com.uob.edag.model.ControlModel;
import com.uob.edag.model.CountryAttributes;
import com.uob.edag.model.FileModel;
import com.uob.edag.model.HadoopModel;
import com.uob.edag.model.ProcessInstanceModel;
import com.uob.edag.model.ProcessModel;
import com.uob.edag.model.StageModel;
import com.uob.edag.utils.FileUtility;
import com.uob.edag.utils.FileUtility.OperationType;
import com.uob.edag.utils.FileUtilityFactory;
import com.uob.edag.utils.HadoopUtils;
import com.uob.edag.utils.PropertyLoader;
import com.uob.edag.utils.ReconciliationUtil;
import com.uob.edag.utils.UobUtils;

public class PSWeblogIngestionProcessor extends FileIngestionProcessor implements IngestionProcessor {

	private String environment = null;
	private String environmentNum = null;

	private boolean dropHivePartitionOnReconciliationFailure = true;

	private FileUtility fileUtils;
	private int noOfRecords = 0;
	private List<String> consumedZipfilesList = new ArrayList<String>();
	private List<String> unzippedfilesList = new ArrayList<String>();
	private List<String> tempUnzippedfilesList = new ArrayList<String>();
	Map<String, Integer> individualFileMap = new HashMap<String, Integer>();
	private boolean historicLoadFlag = false;
	

	  public void runFileIngestion(ProcessInstanceModel procInstanceModel, ProcessModel procModel, String bizDate, 
	  		                         String ctryCd, boolean forceRerun, String forceFileName) throws EDAGException {
		  
	    logger.info("PS runFileIngestion: start to run File Ingestion for process: " + procModel.getProcId() 
	    	+ " and bizDate, ctryCd, forceRerun, forceFileName are " + forceRerun + " : " + bizDate + " : " + ctryCd + " : " + forceRerun + " : "+ forceFileName +" ; ");
	    
	    boolean cleanup = false;
	    boolean procInitCompleted = false;
	    String paramFilePath = null; 
	    ProcessInstanceModel prevStatusModel = null;
	    List<StageModel> stgModelList = null;
	    EDAGException instanceException = null;
	    this.fileUtils = FileUtilityFactory.getFileUtility(procModel);
	    
	    try {
	      environment = PropertyLoader.getProperty(UobConstants.ENVIRONMENT);
	      environmentNum = PropertyLoader.getProperty(UobConstants.ENVIRONMENT_NUM);
	      String drop = PropertyLoader.getProperty(UobConstants.DDS_HIVE_PARTITION_DROP_ON_RECONCILIATION_FAILURE);
	      this.dropHivePartitionOnReconciliationFailure = drop == null ? true : UobUtils.parseBoolean(drop);
	      prevStatusModel = ingestDao.getPrevRunStatus(procInstanceModel);
	      
	      if (!forceRerun) {
	    	  /* 
	    	   * forceRerun is always true.
	           */
	      } else {
	    	  
	        if (prevStatusModel != null && UobConstants.RUNNING.equalsIgnoreCase(prevStatusModel.getStatus())) {
	        	throw new EDAGProcessorException(EDAGProcessorException.RUNNING_INSTANCE_EXISTS, prevStatusModel.getProcInstanceId(), prevStatusModel.getProcId());
	        }
	        
	        logger.info("Force Rerun is set. Not checking Previous Run Status");
	        
	        // There should be no forceFileName as files are from source directory - landing folder.
	        if (StringUtils.isNotEmpty(forceFileName)) {
	          procModel.getSrcInfo().setSourceDirectory(forceFileName);
	        }
	        
	        prevStatusModel = null;
	      }
	      
	    
	      parseControlInformation(procModel, procInstanceModel, ctryCd);
	      procInitCompleted = stgHndle.checkStepCompleted(stgModelList, procInstanceModel, UobConstants.STAGE_INGEST_PROC_INIT);
	      paramFilePath = PropertyLoader.getProperty(UobConstants.BDM_PARAM_FILE_PATH);
	      paramFilePath = paramFilePath.replace(UobConstants.SRC_SYS_PARAM, procModel.getSrcSysCd())
	      														 .replace(UobConstants.ENV_PARAM, environment)
	      														 .replace(UobConstants.ENV_NUM_PARAM, environmentNum)
	      														 .replace(UobConstants.FREQ_PARAM, procModel.getProcFreqCd())
	      														 .replace(UobConstants.FILENM_PARAM, procModel.getProcNm())
	      														 .replace(UobConstants.BIZ_DATE_PARAM, procInstanceModel.getBizDate())
	      														 .replace(UobConstants.COUNTRY_PARAM, ctryCd);
	      
	      logger.debug("PS runFileIngestion: After replace params : paramFilePath is  " + paramFilePath);
	      
	    } catch (EDAGException excp) {
	    	instanceException = excp;
	    } catch (Throwable excp) {
	    	instanceException = new EDAGException(excp.getMessage(), excp);
	    } finally {
	      if (instanceException != null) {
	        procInstanceModel.setException(instanceException); 
	        stgHndle.addStageLog(procInstanceModel, UobConstants.STAGE_INGEST_PROC_INIT);
	        stgHndle.updateStageLog(procInstanceModel, UobConstants.STAGE_INGEST_PROC_INIT, null);
	        throw instanceException;
	      }
	    }
	    
	    if (forceRerun || ! procInitCompleted || cleanup) {
	      runProcessInitialization(procInstanceModel, procModel, bizDate, paramFilePath, cleanup, prevStatusModel, 
	      		                     forceFileName, ctryCd);
	    } else {
	      logger.info("Stage: Process Initialization already completed; Skipping stage");
	    }

	    ControlModel controlModel = procInstanceModel.getControlModel();

	    boolean procFinalCompleted;
	    boolean skipBDMProcess = false;
	    Properties props = System.getProperties();
	    
	    if (props.containsKey("localDevEnv") && System.getProperty("localDevEnv").equals("true")) {
	    	
	      procFinalCompleted = false;
	      logger.info("localDevEnv is true and no T1 and T11 jobs");
	      
	    } else {
	    	
	    	logger.info("localDevEnv is false and going to invoke T1 and T11 jobs");
	    	
	    	boolean processEmptyFile = Boolean.parseBoolean(PropertyLoader.getProperty(UobConstants.BDM_PROCESS_EMPTY_FILE));
	  		File sourceFile = getSourceFile(new File(paramFilePath), true);
	  		boolean emptySourceFile = !containsDataRow(controlModel, sourceFile);
	  		
	  		logger.debug("Source file " + sourceFile.getPath() + (emptySourceFile ? " is empty" : " is not empty"));
	  		
	    	skipBDMProcess = !processEmptyFile && emptySourceFile;
	    	
	      boolean t1BdmCompleted = stgHndle.checkStepCompleted(stgModelList, procInstanceModel, UobConstants.STAGE_INGEST_T1_BDM_EXEC);
	      
	      if (forceRerun || !t1BdmCompleted || cleanup) {
	      	if (skipBDMProcess) {
	      		logger.info("Stage: Tier 1 BDM Process is skipped due to empty source file");
	      	} else {
	      		
	      		logger.debug("starting: invoke the runBdmTier1Process method." + paramFilePath);
	      		
	      		HadoopUtils.deleteHDFSFiles(new Path(procModel.getDestInfo().getStagingDir()),true);
	      		
	      		logger.debug("T1: " + procModel.getDestInfo().getStagingDir() + " is truncated.");
	      		
	      		for (int i=0;i<unzippedfilesList.size();i++) {//
	      			String unzippedfile = unzippedfilesList.get(i);
	      			logger.debug("unzippedfile : " + unzippedfile);
	      			procInstanceModel.setFileNm(unzippedfile);	    	
	      			copySourceFileToHDFS(procInstanceModel, procModel, i);
	      		}
	      		
	      		runBdmTier1Process(procInstanceModel, procModel, paramFilePath);
	            logger.debug("ending: invoked the runBdmTier1Process method.");
	          
	      	}
	      } else {
	        logger.info("Stage: Tier 1 BDM Process already completed; Skipping stage");
	      }

	      boolean t11BdmCompleted = stgHndle.checkStepCompleted(stgModelList, procInstanceModel,UobConstants.STAGE_INGEST_T11_BDM_EXEC);
	      
	      if (forceRerun || !t11BdmCompleted || cleanup) {

	      	if (!UobConstants.HISTORY_LOAD_CD.equals(procModel.getLoadProcess().getTgtAplyTypeCd()) && historicLoadFlag) {
	      		logger.info("Drop hive partition before run T1.1 process");
	      		dropT11HivePartition(procModel);
	      	} else {
	      		logger.info("Process model " + procModel.getProcId() + " has historical load type, T1.1 partition is not dropped");
	      	}
	      	
	      	dropT1ErrorPartition(procModel);
	      	
	      	if (skipBDMProcess) {
	      		logger.info("Stage: Tier 1.1 BDM Process is skipped due to empty source file");
	      	} else {
	      		
	      		logger.debug("starting: invoke the runBdmTier11Process method." + paramFilePath);
	      		
	      		runBdmTier11Process(procInstanceModel, procModel, paramFilePath);
	      		
	      		logger.debug("ending: invoked the runBdmTier11Process method.");
	      	}	
	      } else {
	        logger.info("Stage: Tier 1.1 BDM Process already completed; Skipping stage");
	      }

	      procFinalCompleted = stgHndle.checkStepCompleted(stgModelList, procInstanceModel, UobConstants.STAGE_INGEST_PROC_FINAL);
	    }

	    if (forceRerun || !procFinalCompleted || cleanup) {
	    	
	      runProcessFinalization(procInstanceModel, procModel, skipBDMProcess,ctryCd);
	      
	    } else {
	      logger.info("Stage: Process Finalization already completed; Skipping stage");
	    }

	    logger.info("File Ingestion completed: " + procModel.getProcId());
	  }

	  /**
	   * This method is used to parse the control information for the source file being ingested. The
	   *     control information can be in the header/footer of the source file or in a separate
	   *     control file.
	   * @param procInsModel The Process Instance Model object
	   * @param procModel The Process Model object containing the metadata of the Ingestion Process
	   * @throws EDAGValidationException 
	   * @throws EDAGSSHException 
	   * @throws EDAGIOException 
	   * @throws EDAGValidationException
	   * @throws Exception when there is an error parsing the control file/header-trailer.
	   */
	  private void parseControlInformation(ProcessModel procModel, ProcessInstanceModel procInsModel, String ctryCd) throws EDAGException {

		ControlModel controlModel = new ControlModel();
		controlModel.setHeaderLine(UobConstants.EMPTY);
		controlModel.setFooterLine(UobConstants.EMPTY);
		controlModel.setTotalRecords(noOfRecords);
		controlModel.setCtryCd(ctryCd);
		controlModel.setBizDate(procInsModel.getBizDate());
		procInsModel.setControlModel(controlModel);

	  }
	  
	  /**
	   * This method is used to run the Process Initialization of an Ingestion Process.
	   * @param procInsModel The Process Instance Model object
	   * @param procModel The Process Object with the metadata of the Process to be ingested
	   * @param bizDate The Business Date for Ingestion
	   * @param paramFilePath The BDM Parameter File Path
	   * @param cleanup Value of the cleanup flag
	   * @param prevStatusModel Process Instance Model object for any previously run instance 
	   *     for the same date and country
	   * @param forceFileName The name of the file which needs to be force started from the beginning
	   * @throws EDAGException 
	   * @throws Exception when there is any error in the Process Initialization
	   */
	  private void runProcessInitialization(ProcessInstanceModel procInsModel, ProcessModel procModel, String bizDate, 
	  		                                  String paramFilePath, boolean cleanup, ProcessInstanceModel prevStatusModel, 
	  		                                  String forceFileName, String country) throws EDAGException {
		  
		  int linesToValidate = -1;
		  EDAGException instanceException = null;
          DateFormat sdf = new SimpleDateFormat("yyyyMMddHHmmss");
          String currentTime = sdf.format(System.currentTimeMillis());

          String fileNameRegx = PropertyLoader.getProperty(UobConstants.PERSONETICS_ZIPFILE_REGEX).replace("bizdate_param", bizDate);
          logger.debug("beigning of runProcessInitialization: " + fileNameRegx);
          
          String dataDir = PropertyLoader.getProperty(UobConstants.DATADIR);
          String landingFolder = procModel.getSrcInfo().getSourceDirectory();
          
          File tempFile1 = new File (landingFolder);
          landingFolder = tempFile1.getParent()+"/";
          
          String processingfolder =  PropertyLoader.getProperty(UobConstants.LANDING_AREA_PSWEBLOG_PROCESSING_DIR_PATH);
          String sourceLogFileNameInProceesing = null;
          
          // fixes for SIT ticket 345
	      // archive file from landing location to previous folder
          //String achiveFullFile =  PropertyLoader.getProperty(UobConstants.LANDING_AREA_ARCHIVE_DIR_PATH);
	      String achiveFullFile = procModel.getSrcInfo().getSourceArchivalDir().replaceAll(UobConstants.COUNTRY_PARAM, country);
          File tempFile2 = new File (achiveFullFile);
          String achiveFolder = tempFile2.getParent()+"/";
          boolean noSourceFileFlag = true;
          
          String sourceZipFileName = null;
          String sourceZipFileNameWithPath = null;
          List<String> sourceZipFileList = new ArrayList<String>();
          int totalRownumberOfAllsourcefiles = 0;
          int totalRowCountOfCurrentfile = 0;
          
	    try {
	    	
	      stgHndle.addStageLog(procInsModel, UobConstants.STAGE_INGEST_PROC_INIT);
	      
	      FileModel fileModel = procModel.getSrcInfo();
	      
	      if (!cleanup) {
	    	  
	      	CountryAttributes attrs = procModel.getCountryAttributesMap().get(country);
	        String charset = attrs != null ? attrs.getCharset(true) : PropertyLoader.getProperty(UobConstants.DEFAULT_CHARSET);
	        
	    	Map<String, String> controlCharacterMap = ingestDao.getControlCharacterReplacement(procInsModel.getProcId(), procInsModel.getCountryCd()); 
	    	
	        if (prevStatusModel != null) {
	        	logger.info("Exception: prevStatusModel is not null. it should be always NULL.");
	        	/* 
	        	 * prevStatusModel is always NULL.
	        	 */
	        } else {
	        
	          File file = new File(landingFolder);
	          File[] tempListFiles = file.listFiles();
	          
	          // if no files in landing folder or no matching files in landing folder, noSourceFileFlag will be true.
				if (tempListFiles == null || tempListFiles.length == 0 ) {
					logger.info("There is no source files available in the landing folder originally.");
				}else {
			          for (File tempFile:tempListFiles) {
			        	  
							if (Pattern.compile(fileNameRegx, Pattern.CASE_INSENSITIVE).matcher(tempFile.getName()).matches()) {
								noSourceFileFlag= false;
			        		  	break;
			        		  }
				        }
				}
				
				// if no valid source file exist in the landing folder then check archive folder
				if (noSourceFileFlag) {
					
					File achiveFile = new File(achiveFolder);
					File[] tempAchiveListFiles = achiveFile.listFiles();

					for (File tempAchiveFile : tempAchiveListFiles) {

						if (Pattern.compile(fileNameRegx, Pattern.CASE_INSENSITIVE).matcher(tempAchiveFile.getName()).matches()) {
							
							File landingFile = new File(landingFolder+tempAchiveFile.getName());
							
							Files.move(tempAchiveFile.toPath(), landingFile.toPath(),StandardCopyOption.REPLACE_EXISTING);
							logger.info(tempAchiveFile.getName() + " is moved from archive folder to landing folder.");
							noSourceFileFlag = false;
							historicLoadFlag = true;
						}
					}
				}
					
				if (noSourceFileFlag) {
					logger.info("There is no source file to be consumed.");
		        	  throw new EDAGException("There is no source files available in the Personetics Weblog landing folder");
				} else {
					
		          tempListFiles = new File(landingFolder).listFiles();
			          
					for (File tempFile:tempListFiles) {
						
						  totalRowCountOfCurrentfile = 0;
			        	  
			        	  if (Pattern.compile(fileNameRegx, Pattern.CASE_INSENSITIVE).matcher(tempFile.getName()).matches()) {
			        		  
			        		  sourceZipFileName = tempFile.getName();
			        		  sourceZipFileList.add(sourceZipFileName);
			        		  sourceZipFileNameWithPath = landingFolder + sourceZipFileName;
			                  String fileName = procModel.getSrcInfo().getSourceFileName();
			        		  
			        		  processingfolder = processingfolder.replace(UobConstants.DATADIR_STR_PARAM, dataDir).replace(UobConstants.SRC_SYS_PARAM, procModel.getSrcSysCd().toLowerCase())
			    	          		                         .replace(UobConstants.ENV_PARAM, environment)
			    	          		                         .replace(UobConstants.ENV_NUM_PARAM, environmentNum)
			    	          		                         .replace(UobConstants.FREQ_PARAM, procModel.getProcFreqCd())
			    	          		                         .replace(UobConstants.FILENM_PARAM, fileName)
			    	          		                         .replace(UobConstants.SYS_TS_PARAM, currentTime)
			    	          		                         .replace(UobConstants.BIZ_DATE_PARAM, bizDate)
			    	          		                         .replace(UobConstants.COUNTRY_PARAM, procInsModel.getCountryCd().toLowerCase());
			    	          
			        		  // 1. source file name, 2. source file with full path, 3. target directory. 4. new source file name. cover copying zip file unzip?
			        		  try {
			        			  // newSourceFileName is the (path + file name) which is new source file file with processing folder path.
			        			  sourceLogFileNameInProceesing = unzipSourceFileToProcessFolder(sourceZipFileName, sourceZipFileNameWithPath, processingfolder);
			        			  
			        			  logger.info("File " + sourceZipFileNameWithPath + " unzipped successfully to Processing folder " + sourceLogFileNameInProceesing);
			        			  
			        		  }catch (EDAGIOException e) {
			        			  logger.info(sourceZipFileName + " unzip is failed and system continue for other files unzip.");
			        			  continue;
			        		  }
			        		  
			    	          // newSrcFileLoc is processing folder.
			    	          procInsModel.setOrigFileNm(sourceLogFileNameInProceesing);
			    	          procInsModel.setFileNm(sourceLogFileNameInProceesing + "_1"); 
			    	          
			    	          FileUtility.OperationType opType = Boolean.valueOf(PropertyLoader.getProperty(UobConstants.ENABLE_REPLACER_STEP)) ? OperationType.Local : OperationType.Remote;
			    	          FileUtility removeControlCharUtil = FileUtilityFactory.getFileUtility(procModel, opType);
			    	          
			    	          // replace the control-characters
			    	          try {
			    		          if (MapUtils.isEmpty(controlCharacterMap)) {
			    		        	  
			    		          	  removeControlCharUtil.removeControlCharacter(sourceLogFileNameInProceesing, sourceLogFileNameInProceesing + "_1", procModel.getSrcInfo(), 
			    		          			                                         procInsModel.getControlModel(), procModel.getDestInfo(), charset, isSkipErrRecordsDisabled, linesToValidate);
			    		          } else {
			    		        	  
			    		          	  removeControlCharUtil.removeControlCharacter(sourceLogFileNameInProceesing, sourceLogFileNameInProceesing + "_1", procModel.getSrcInfo(), 
			    		          			                                         procInsModel.getControlModel(), procModel.getDestInfo(), charset, isSkipErrRecordsDisabled, controlCharacterMap);
			    		          }	
			    		          
			    		          totalRownumberOfAllsourcefiles = totalRownumberOfAllsourcefiles + procInsModel.getControlModel().getTotalRecords();
			    		          totalRowCountOfCurrentfile = procInsModel.getControlModel().getTotalRecords();
			    		          
			    	          } catch (EDAGValidationException e) {
			    	          	// EDF-209
			    	          	if (UobConstants.HISTORY_CD.equalsIgnoreCase(procModel.getProcFreqCd())) {
			    	          		logger.info(procInsModel.getProcId() + " is a history process, dumping " + sourceLogFileNameInProceesing + " into dump table");
			    	          		this.fileUtils.copyFile(sourceLogFileNameInProceesing, sourceLogFileNameInProceesing + "_1");
			    	          		          		
			    	          		procInsModel.setDumped(true);
			    	          		procInsModel.setException(e);
			    	          	} else {
			    	          		
			    	          		// rethrow the exception
			    	          		throw e;
			    	          	}
			    	          }
			    	          
			    	          unzippedfilesList.add(sourceLogFileNameInProceesing + "_1");
			    	          tempUnzippedfilesList.add(sourceLogFileNameInProceesing);
			    	          consumedZipfilesList.add(sourceZipFileNameWithPath);
			    	          individualFileMap.put(sourceZipFileName, totalRowCountOfCurrentfile);
			    	          
			    	          logger.debug("source file name : row counts == " + sourceZipFileName + " : " + totalRowCountOfCurrentfile);
			    	          
			    	          ingestDao.updateProcessLogFileName(procInsModel);
			    	          fileModel.setSourceDirectory(sourceLogFileNameInProceesing + "_1");
			        	  }else {
			        		  logger.info("this file is not matched hence ignore this file");
			        	  }
			          }
				}
	          
	          procInsModel.getControlModel().setTotalRecords(totalRownumberOfAllsourcefiles);
	       
	        }
	      } else {
	    	/* 
	    	 * cleanup is always false.
	        */
	      }

	      // Biz Date Validation
	      if (bizDate == null) {
	        throw new EDAGValidationException(EDAGValidationException.NULL_VALUE, "Business Date", "Business date cannot be null");
	      }

	      // Create BDM Parameter Mapping File
	      createBdmParamFile(procInsModel, procModel, bizDate, paramFilePath);
	      
	      stgHndle.updateStageLog(procInsModel, UobConstants.STAGE_INGEST_PROC_INIT, null);
	      
	    } catch (EDAGException excp) {
	    	instanceException = excp;
	    } catch (Throwable excp) {
	    	instanceException = new EDAGException(excp.getClass().getName() + ": " + excp.getMessage(), excp);
	    } finally {
	    	if (instanceException != null) {
		      procInsModel.setException(instanceException);
			  stgHndle.updateStageLog(procInsModel, UobConstants.STAGE_INGEST_PROC_INIT, null);
		      throw instanceException;
	    	}
	    }
	  }
	  
	  /**
	   * This method is used to run the process finalization which includes error threshold validation,
	   *     Row Count Validation and Hash Sum Validation. It also archives the file into the archive
	   *     area.
	   * @param procInsModel The Process Instance Model object
	   * @param procModel The Process Model object containing the metadata of the Ingestion Process
	   * @param skipBDMProcess 
	   * @throws Exception when there is an error in the finalization process
	   */
	  private void runProcessFinalization(ProcessInstanceModel procInsModel, ProcessModel procModel, boolean skipBDMProcess, String countrycode) throws EDAGException {
	      EDAGException instanceException = null;
	      
	      
	      try {
	    	  
	          stgHndle.addStageLog(procInsModel, UobConstants.STAGE_INGEST_PROC_FINAL);
	          logger.debug("Going to start Reconciliation for process:" + procInsModel.getProcInstanceId());
	          
	          // If local env without Informatica and HDFS / Hive then skip that step
	          Properties props = System.getProperties();
	          if (!(props.containsKey("localDevEnv") && System.getProperty("localDevEnv").equals("true"))) {
	              if (skipBDMProcess || procInsModel.isDumped()) {
	                  logger.info("Reconciliation is skipped as it is not required");
	              } else {
	                  try {
	                	  
	                	  for (Map.Entry<String, Integer> entry: individualFileMap.entrySet() ) {
	                		  
	                		  procInsModel.setTempFileNm(entry.getKey());
	                		  procInsModel.setTempFileRowCount(entry.getValue());
	                		  
	                		  ingestDao.insertLoadedFileLog(procInsModel);
	                	  }
	                	  
	                      // Validate Threshold
	                      ReconciliationUtil util = new ReconciliationUtil();
	                      
	                      // Reconciliation
	                      ControlModel controlModel = procInsModel.getControlModel();
	                      
	                      logger.info("procModel before runErrorThresholdValidation " + controlModel);
	                      
	                      int errRowCnt = util.runErrorThresholdValidation(procInsModel, procModel);
	                      
	                      util.runRowCountValidation(procInsModel, procModel, errRowCnt);
	                      
	                  } catch (Exception excp) {
	                	  	if (dropHivePartitionOnReconciliationFailure) {
		                      logger.error("Going to drop Hive Partition on T1.1 Table as Reconciliation failed: " + excp.getMessage());
		                      dropHivePartition(procModel);
	                	  	} else {
	                	  		logger.info("Not dropping T1.1 Hive partition since DDS_HIVE_PARTITION_DROP_ON_RECONCILIATION_FAILURE property is set to false");
	                	  	}
	                      throw excp;
	                  }
	              }
	          }

	          // Delete processing folder
	          if (PropertyLoader.getProperty(UobConstants.PERSONETICS_DEL_PROCESS_FILE).equals("true")) {
		      	  for (String processedFile: tempUnzippedfilesList) {
			      		this.fileUtils.deleteFile(processedFile);
			      		this.fileUtils.deleteFile(processedFile + "_1");
			      		logger.info("processedFile " + processedFile +" is deleted from processing folder.");
			      		logger.info("processedFile " + processedFile + "_1" +" is deleted from processing folder.");
			      	  }
	          }

	          
		    	// Archive Source File
		      // archive file from landing location to previous folder
		      String archiveFilePath = procModel.getSrcInfo().getSourceArchivalDir().replaceAll(UobConstants.COUNTRY_PARAM, countrycode);
		      logger.debug("archiveFilePath: " + archiveFilePath);
		      File tempArchiveFile = new File(archiveFilePath);
		      archiveFilePath = tempArchiveFile.getParent();
		      logger.debug("archiveFilePath parent: " + archiveFilePath);
		      
		      //this.fileUtils.archiveFile(procModel.getSrcInfo().getSourceDirectory(), archiveFilePath, false);
		      File sourcefile = null;
		      File targetfile = null;
	      	  for (String zipFile: consumedZipfilesList) {
	      		  sourcefile = new File(zipFile);
	      		  targetfile = new File(archiveFilePath+"/"+sourcefile.getName());
	      		  logger.info("consumed zip file name: " + zipFile);
	      		  logger.info("Archived files path and name: " + archiveFilePath+"/"+sourcefile.getName());
	      		  //this.fileUtils.moveFile(zipFile, archiveFilePath + "/" + file.getName());
				  Files.move(sourcefile.toPath(), targetfile.toPath(),StandardCopyOption.REPLACE_EXISTING);
	      	  }
		      
	          stgHndle.updateStageLog(procInsModel, UobConstants.STAGE_INGEST_PROC_FINAL, null);
	          
	      } catch (EDAGException excp) {
	          instanceException = excp;
	      } catch (Throwable excp) {
	          instanceException = new EDAGException(excp.getMessage(), excp);
	      } finally {
	          if (instanceException != null) {
	              procInsModel.setException(instanceException); 
	              stgHndle.updateStageLog(procInsModel, UobConstants.STAGE_INGEST_PROC_FINAL, null);
	              throw instanceException;
	          }
	      }
	  }

	  private String unzipSourceFileToProcessFolder(String sourceZipFileName, String sourceZipFileNameWithPath, String processingfolder) 
			throws EDAGIOException {

		// 1. source file name, 2. source file with full path, 3. target directory. 4.
		// new source file name. cover copying zip file unzip?

		long start = System.currentTimeMillis();
		ZipFile zipFile = null;
		String newSourceFileName = processingfolder + sourceZipFileName.substring(0, sourceZipFileName.length() - 4);

		try {

			zipFile = new ZipFile(sourceZipFileNameWithPath);
			Enumeration<?> entries = zipFile.entries();

			while (entries.hasMoreElements()) {

				ZipEntry entry = (ZipEntry) entries.nextElement();

				File targetFile = new File(newSourceFileName);
				targetFile.createNewFile();

				InputStream is = zipFile.getInputStream(entry);
				FileOutputStream fos = new FileOutputStream(targetFile);

				int len;
				byte[] buf = new byte[4096];
				while ((len = is.read(buf)) != -1) {
					fos.write(buf, 0, len);
				}

				fos.close();
				is.close();
			}

			long end = System.currentTimeMillis();

			logger.debug("unzip time consumed: " + sourceZipFileName + " : " + (end - start) + " ms");

		} catch (IOException e) {

			// output the failed files to log file and then continue for next file.
			logger.info("unzipSourceFile failure: Unable to unzip " + sourceZipFileNameWithPath + " to " + processingfolder + "; " + e.getMessage());
			throw new EDAGIOException("Unable to unzip " + sourceZipFileNameWithPath + " to " + processingfolder + "; " + e.getMessage(), e);

		} finally {
			if (zipFile != null) {
				try {
					zipFile.close();
				} catch (IOException e) {

					// output the error message to log files.
					logger.info("unzipSourceFileToProcessFolder: zip file is null. " + e.getMessage());
					throw new EDAGIOException("Unable to unzip " + sourceZipFileNameWithPath + " to " + processingfolder + ": " + e.getMessage(), e);
				}
			}
		}

		return newSourceFileName;

	}
	  
	  private void copySourceFileToHDFS(ProcessInstanceModel procInsModel, ProcessModel procModel, int sequenceNum) throws EDAGIOException {
		  
		  	File src = new File(procInsModel.getFileNm());
		  	
		    FileModel fileModel = procModel.getSrcInfo();
		    HadoopModel destModel = procModel.getDestInfo();
		    
		    String targetDir = destModel.getStagingDir(); // EDF-209 // procInsModel.isDumped() ? destModel.getDumpStagingDir(procModel, procInsModel) : 

		    logger.info("copySourceFileToHDFS : Copying file " + src.getPath() + " from landing area to " + HadoopUtils.getConfigValue("fs.defaultFS") + targetDir + fileModel.getSourceFileName());
		    
		    logger.debug("copySourceFileToHDFS : procInsModel.getFileNm(): " + procInsModel.getFileNm());
		    logger.debug("copySourceFileToHDFS : T1 target location: " + targetDir + fileModel.getSourceFileName());
		    
		    HadoopUtils.copyToHDFS(false, false, src, new Path(targetDir + fileModel.getSourceFileName() + sequenceNum));//  
		  }
	  
	  /**
	   * This method is used to run the Tier 1 BDM Mapping for ingesting the file into the staging area.
	   * @param procInsModel The Process Instance Model object
	   * @param procModel The Process Model object containing the metadata of the Ingestion Process
	   * @param paramFilePath The parameter file path to be used by BDM
	   * @throws EDAGException 
	   * @throws Exception when there is any error in the Tier 1 BDM process
	   */
	  private void runBdmTier1Process(ProcessInstanceModel procInsModel, ProcessModel procModel, 
	  		                            String paramFilePath) throws EDAGException {
	    String toolId = null;
	    EDAGException instanceException = null;
	    logger.info("Going to run BDM Tier 1 Process");
	    stgHndle.addStageLog(procInsModel, UobConstants.STAGE_INGEST_T1_BDM_EXEC);
	    try {
	    	
	      // copySourceFileToHDFS(procInstanceModel, procModel);

	      // Attach Hive Partition
	      HadoopModel destModel = procModel.getDestInfo();
	      String schemaName = destModel.getStagingDbName();
	      String tableName = procInsModel.isDumped() ? destModel.getDumpStagingTableName() : destModel.getStagingTableName(); // EDF-209
	      String partitionValue = destModel.getStagingHivePartition();
	      
	      HiveDao dao = new HiveDao();
	      logger.debug("PS : Going to attach Hive Partition: " + partitionValue + " To Staging Table: " + schemaName + "." + tableName);
	      dao.attachHivePartition(schemaName, tableName, partitionValue);
	      
	      // EDF-209
	      int stagingRowCount = dao.getRowCountByPartition(schemaName, tableName, partitionValue, destModel.getHadoopQueueName());
	      
	      logger.info("PS : stagingRowCount" + stagingRowCount );
	      
	      ControlModel controlModel = procInsModel.getControlModel();
	      if (controlModel != null) {
	      	controlModel.setTargetDbName(schemaName);
	      	controlModel.setTargetTableName(tableName);
	      	controlModel.setTotalRecordsTarget(stagingRowCount);
	      }

	      stgHndle.updateStageLog(procInsModel, UobConstants.STAGE_INGEST_T1_BDM_EXEC, toolId);
	    } catch (EDAGException excp) {
	    	instanceException = excp;
	    } catch (Throwable excp) {	
	    	instanceException = new EDAGException(excp.getMessage(), excp);
	    } finally {
	    	if (instanceException != null) {
		      procInsModel.setException(instanceException);
					stgHndle.updateStageLog(procInsModel, UobConstants.STAGE_INGEST_T1_BDM_EXEC, toolId);
		      throw instanceException;
	    	}
	    }
	  }
	  
	  private void dropT11HivePartition(ProcessModel procModel) throws EDAGMyBatisException {
		  	// Truncate Partition on T1.1 table
		    HiveDao dao = new HiveDao();
		    HadoopModel destModel = procModel.getDestInfo();
		    String schemaName = destModel.getHiveDbName();
		    String tableName = destModel.getHiveTableName();
		    String partition = destModel.getHivePartition();
		    String parentPartition = null;
		    
		    char[] value = partition.toCharArray();
		    int lastIndexOfComma = 0;
		    for(int i=value.length-1;i>=0;i--) {
				if(value[i]==',') {
					lastIndexOfComma = i;
					break;
		        }
		    }
		    
		    if (lastIndexOfComma==0) {
		    	parentPartition = partition;
		    }else {
		    	parentPartition = partition.substring(0,lastIndexOfComma);
		    }
		    
		    logger.info("schemaName: " + schemaName + ", tableName: " + tableName + ", parent partition: " + parentPartition + ", partition: " + partition);
		    
		    dao.dropHivePartitionRecursive(schemaName, tableName, parentPartition);
			}
	  
}
