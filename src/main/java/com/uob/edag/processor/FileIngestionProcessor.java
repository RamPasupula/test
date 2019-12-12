package com.uob.edag.processor;


import java.io.File;
import java.nio.charset.Charset;
import java.sql.SQLException;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.stream.Collectors;

import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.collections.MapUtils;
import org.apache.commons.io.FileUtils;
import org.apache.commons.io.FilenameUtils;
import org.apache.commons.io.IOUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.fs.Path;
import org.w3c.dom.Document;

import com.teradata.hook.tdinterfaces.HookFrameworkInterface;
import com.uob.edag.constants.UobConstants;
import com.uob.edag.dao.HiveDao;
import com.uob.edag.dao.HiveGenerationDao;
import com.uob.edag.dao.ImpalaDao;
import com.uob.edag.exception.EDAGException;
import com.uob.edag.exception.EDAGIOException;
import com.uob.edag.exception.EDAGMyBatisException;
import com.uob.edag.exception.EDAGProcessorException;
import com.uob.edag.exception.EDAGSSHException;
import com.uob.edag.exception.EDAGValidationException;
import com.uob.edag.exception.EDAGXMLException;
import com.uob.edag.model.ControlModel;
import com.uob.edag.model.CountryAttributes;
import com.uob.edag.model.FieldModel;
import com.uob.edag.model.FileModel;
import com.uob.edag.model.HadoopModel;
import com.uob.edag.model.ProcessInstanceModel;
import com.uob.edag.model.ProcessModel;
import com.uob.edag.model.ProcessParam;
import com.uob.edag.model.SparkExecParamsModel;
import com.uob.edag.model.StageModel;
import com.uob.edag.security.EncryptionUtil;
import com.uob.edag.utils.BdmUtils;
import com.uob.edag.utils.CompactionUtil;
import com.uob.edag.utils.FileUtility;
import com.uob.edag.utils.FileUtility.OperationType;
import com.uob.edag.utils.FileUtilityFactory;
import com.uob.edag.utils.HadoopUtils;
import com.uob.edag.utils.LinuxUtils;
import com.uob.edag.utils.OSMMergerUtil;
import com.uob.edag.utils.PropertyLoader;
import com.uob.edag.utils.SparkIngestionLauncher;
import com.uob.edag.utils.ReconciliationUtil;
import com.uob.edag.utils.UobUtils;
import com.uob.edag.utils.XMLUtils;

/**
 * @Author : Daya Venkatesan.
 * @Date of Creation: 10/24/2016
 * @Description : The class is used for executing the file ingestion.
 */
public class FileIngestionProcessor extends BaseProcessor implements IngestionProcessor {
	
	private static final String SOURCE_FILE_DIR_XPATH = "/xsi:root/xsi:project/xsi:mapping/xsi:parameter[@name='source_file_dir']";
	private static final String SOURCE_FILE_XPATH = "/xsi:root/xsi:project/xsi:mapping/xsi:parameter[@name='source_file']";
	private static final String LINES_TO_VALIDATE = FileIngestionProcessor.class.getName() + ".LinesToValidate";
	private static final SimpleDateFormat DEFAULT_BUSINESS_DATE_FORMAT = new SimpleDateFormat("yyyy-mm-dd");
	
	private String environment = null;
  	private String environmentNum = null;
  
  	private boolean dropHivePartitionOnReconciliationFailure = true;
  
  	private FileUtility fileUtils;
  
  	private int noOfRecords = 0;
  
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
  	EDAGException instanceException = null;
    try {
      stgHndle.addStageLog(procInsModel, UobConstants.STAGE_INGEST_PROC_INIT);
      logger.debug("Going to Run Process Initialization");
      FileModel fileModel = procModel.getSrcInfo();
      ControlModel controlModel = procInsModel.getControlModel();
      logger.debug("Control Information is: " + controlModel);
      boolean hasBadLines = Boolean.FALSE;
      boolean preProcessFlag = Boolean.FALSE;
      boolean preProcessResult = Boolean.FALSE;
  	 
      String newSrcFileLoc = null;
      if (!cleanup) {
      	CountryAttributes attrs = procModel.getCountryAttributesMap().get(country);
        String charset = attrs != null ? attrs.getCharset(true) : PropertyLoader.getProperty(UobConstants.DEFAULT_CHARSET);
    		Map<String, String> controlCharacterMap = ingestDao.getControlCharacterReplacement(procInsModel.getProcId(), procInsModel.getCountryCd()); 
    		int linesToValidate = -1;
    		try {
    			linesToValidate = Integer.parseInt(PropertyLoader.getProperty(LINES_TO_VALIDATE));
    		} catch (NumberFormatException e) {
    			logger.debug("Defaulting lines to validate to 11 since " + LINES_TO_VALIDATE + " property cannot be retrieved from framework-conf.properties: " + e.getMessage());
    			linesToValidate = 11;
    		}
        
        if (prevStatusModel != null) {
          String procInstanceId = prevStatusModel.getProcInstanceId();
          newSrcFileLoc = ingestDao.retrieveProcessFileName(procInstanceId);
          if (StringUtils.isEmpty(newSrcFileLoc)) {
            newSrcFileLoc = PropertyLoader.getProperty(UobConstants.LANDING_AREA_PROCESSING_DIR_PATH);
            String fileName = procModel.getSrcInfo().getSourceFileName();
            DateFormat sdf = new SimpleDateFormat("yyyyMMddHHmmss");
            String currentTime = sdf.format(System.currentTimeMillis());
            String dataDir = PropertyLoader.getProperty(UobConstants.DATADIR);
            newSrcFileLoc = newSrcFileLoc.replace(UobConstants.DATADIR_STR_PARAM, dataDir);
            newSrcFileLoc = newSrcFileLoc.replace(UobConstants.SRC_SYS_PARAM, procModel.getSrcSysCd().toLowerCase())
                  											 .replace(UobConstants.ENV_PARAM, environment)
                  											 .replace(UobConstants.ENV_NUM_PARAM, environmentNum)
                  											 .replace(UobConstants.FREQ_PARAM, procModel.getProcFreqCd())
                  											 .replace(UobConstants.FILENM_PARAM, fileName)
                  											 .replace(UobConstants.SYS_TS_PARAM, currentTime)
                  											 .replace(UobConstants.COUNTRY_PARAM, procInsModel.getCountryCd().toLowerCase())
                  											 .replace(UobConstants.BIZ_DATE_PARAM, bizDate);
            
            logger.debug("Going to move the file " + fileModel.getSourceDirectory() + 
                         " to Processing folder " + newSrcFileLoc);
            this.fileUtils.moveFile(fileModel.getSourceDirectory(), newSrcFileLoc);
            
            FileUtility.OperationType opType = Boolean.valueOf(PropertyLoader.getProperty(UobConstants.ENABLE_REPLACER_STEP)) ? OperationType.Local : OperationType.Remote;
            FileUtility removeControlCharUtil = FileUtilityFactory.getFileUtility(procModel, opType);
            logger.debug("runProcessInitialization 2: newSrcFileLoc 2: " + newSrcFileLoc);
            try {

                if(doDirectCopy(fileModel.getSourceFileLayoutCd())){
                    logger.debug("Skipping Control Character removal. Doing a direct copy of the file:" + newSrcFileLoc);
                    FileUtils.copyFile(new File(newSrcFileLoc), new File(newSrcFileLoc + "_1"));
                }
                else{
                	if(ingestDao.retrievePreProcessFlag(procModel.getProcId(), country).equalsIgnoreCase("Y"))
              		  preProcessFlag = Boolean.TRUE;
                	if(preProcessFlag) {
                			try {
              		  			logger.debug("Invoking the Pre Processing Handler Class 1");
              		  			String preProcClassName = ingestDao.retrievePreProcessClassName(procModel.getProcId());
              		  			logger.debug("Pre Process Class Name is ....: " + preProcClassName);
								String preProcPropertyFile = "";
              		  			if(System.getProperty(UobConstants.PRE_PROC_CONF_FILE) !=null) {
              		  				logger.debug("System Property preprocessing-client.properties  : " + System.getProperty(UobConstants.PRE_PROC_CONF_FILE));
              		  				preProcPropertyFile=System.getProperty(UobConstants.PRE_PROC_CONF_FILE);
              		  			}
              		  			String driverMemoryPropValue = "", executorMemoryPropValue = "", executorInstancePropValue = "", executorCoresPropValue = "";
              		  			try {
              		  				SparkExecParamsModel sparkPropModel = new SparkExecParamsModel();
              		  				sparkPropModel = ingestDao.getSparkExecutionProperties(procModel.getProcId(), procInsModel.getCountryCd(),UobConstants.PARALLEL_PRE_PROCESSING_PARAM_NM);
              		  				if(sparkPropModel !=null) {
              		  					driverMemoryPropValue = sparkPropModel.getDriverMemory();
              		  					executorMemoryPropValue = sparkPropModel.getExecutorMemory();
              		  					executorInstancePropValue = sparkPropModel.getExecutorInstances();
              		  					executorCoresPropValue = sparkPropModel.getExecutorCores();
              		  				}
              		  				logger.debug("Checked metadata table for Spark Properties.");
              		  			}
              		  			catch(Exception e) {
              		  				e.printStackTrace();
              		  			}
              		  			logger.debug("setting dynamic spark properties from Property File Info : "+ driverMemoryPropValue + " - " + executorMemoryPropValue + " - " + executorInstancePropValue+" - "+executorCoresPropValue);
              				
              		  			HashMap<String, String> paramsMap = new HashMap<String, String>();
              		  			logger.debug("Parameters ..... procId: " + procModel.getProcId() + " ctryCd: " +country+" bizDt: "+bizDate+" procInstanceId: "+procInsModel.getProcInstanceId() + " skipErrorRecords: "+Boolean.toString(procInsModel.isSkipErrRecordsEnabled()));
              		  			paramsMap.put("i", procModel.getProcId());
              		  			paramsMap.put("c", country);
              		  			paramsMap.put("d", bizDate);
              		  			paramsMap.put("n", procInsModel.getProcInstanceId());
              		  			paramsMap.put("p", Boolean.toString(procInsModel.isSkipErrRecordsEnabled()));
              		  			paramsMap.put("t", currentTime);
								paramsMap.put("a", preProcPropertyFile);
                                paramsMap.put("dm", driverMemoryPropValue);
                                paramsMap.put("em", executorMemoryPropValue);
                                paramsMap.put("ei", executorInstancePropValue);
                                paramsMap.put("ec", executorCoresPropValue);
              		  			logger.debug("PRE_PROC_CONF_FILE : " + preProcPropertyFile);
              		  			//paramsMap.put("procId", "");
              		  			Class cla= Class.forName(preProcClassName);
              		  			HookFrameworkInterface hfiObhj = (HookFrameworkInterface)cla.newInstance();
              		  			Map<String,String> preProcResultsMap = hfiObhj.execute(paramsMap);
              		  			logger.debug("The pre processing result is : " + preProcResultsMap.get("status"));
              		  			logger.debug("The pre processing result description is : " + preProcResultsMap.get("description"));
              		  			if(preProcResultsMap.get("status").equalsIgnoreCase("0"))
              		  				preProcessResult = Boolean.TRUE;
              		  			else {
              		  				throw new EDAGValidationException(EDAGValidationException.PRE_PROCESSING_EXCEPTION, preProcResultsMap.get("description"));
              		  			}
              				} 
              				/*catch (ClassNotFoundException ce) {
              					ce.printStackTrace();
              				} */
              				catch (Exception e) {
              					e.printStackTrace();
              				}
              				finally{ // added sm186140
								HadoopUtils.resetUGI(); // reset the UGI, so that the delegation tokens are updated.
							}
                	}
                	logger.debug("Checking Pre Process Result Flag.... : " + preProcessResult);
                	if(!preProcessResult) {
                		 logger.debug("Control Character removal...");
                		if (MapUtils.isEmpty(controlCharacterMap)) {
                			hasBadLines = removeControlCharUtil.removeControlCharacter(newSrcFileLoc, newSrcFileLoc + "_1", procModel.getSrcInfo(),
                                procInsModel.getControlModel(), procModel.getDestInfo(), charset, procInsModel.isSkipErrRecordsEnabled(), linesToValidate);
                		} else {
                			hasBadLines = removeControlCharUtil.removeControlCharacter(newSrcFileLoc, newSrcFileLoc + "_1", procModel.getSrcInfo(),
                                procInsModel.getControlModel(), procModel.getDestInfo(), charset, procInsModel.isSkipErrRecordsEnabled(), controlCharacterMap);
                		}


                		if(hasBadLines) {
                			handleErrorRecords(newSrcFileLoc, procInsModel, procModel);
                		}
                	}	

                }

            } catch (EDAGValidationException e) {
            	// EDF-209
            	if (UobConstants.HISTORY_CD.equalsIgnoreCase(procModel.getProcFreqCd())) {
            		logger.info(procInsModel.getProcId() + " is a history process, dumping " + newSrcFileLoc + " into dump table");
            		this.fileUtils.copyFile(newSrcFileLoc, newSrcFileLoc + "_1");
            		
            		// remove header and footer if necessary
            		if (StringUtils.isNotBlank(procInsModel.getControlModel().getFooterLine())) {
            			this.fileUtils.removeHeaderAndFooter(newSrcFileLoc + "_1", procInsModel.getBizDate(), charset);
            		}
            		
            		procInsModel.setDumped(true);
            		procInsModel.setException(e); 
            	} else {
            		// rethrow the exception
            		throw e;
            	}
            }
          }
          procInsModel.setOrigFileNm(newSrcFileLoc);
          procInsModel.setFileNm(newSrcFileLoc + "_1");
          
          ingestDao.updateProcessLogFileName(procInsModel);
          fileModel.setSourceDirectory(newSrcFileLoc + "_1");
        } else {
          newSrcFileLoc = PropertyLoader.getProperty(UobConstants.LANDING_AREA_PROCESSING_DIR_PATH);
          String fileName = procModel.getSrcInfo().getSourceFileName();
          DateFormat sdf = new SimpleDateFormat("yyyyMMddHHmmss");
          String currentTime = sdf.format(System.currentTimeMillis());
          String dataDir = PropertyLoader.getProperty(UobConstants.DATADIR);
          newSrcFileLoc = newSrcFileLoc.replace(UobConstants.DATADIR_STR_PARAM, dataDir)
          														 .replace(UobConstants.SRC_SYS_PARAM, procModel.getSrcSysCd().toLowerCase())
          		                         .replace(UobConstants.ENV_PARAM, environment)
          		                         .replace(UobConstants.ENV_NUM_PARAM, environmentNum)
          		                         .replace(UobConstants.FREQ_PARAM, procModel.getProcFreqCd())
          		                         .replace(UobConstants.FILENM_PARAM, fileName)
          		                         .replace(UobConstants.SYS_TS_PARAM, currentTime)
          		                         .replace(UobConstants.COUNTRY_PARAM, procInsModel.getCountryCd().toLowerCase())
          		                         .replace(UobConstants.BIZ_DATE_PARAM, bizDate);
          procInsModel.setOrigFileNm(newSrcFileLoc);
          procInsModel.setFileNm(newSrcFileLoc + "_1");

          logger.debug("Going to move the file " + fileModel.getSourceDirectory() + 
                       " to Processing folder " + newSrcFileLoc);
          this.fileUtils.moveFile(fileModel.getSourceDirectory(), newSrcFileLoc);
          
          FileUtility.OperationType opType = Boolean.valueOf(PropertyLoader.getProperty(UobConstants.ENABLE_REPLACER_STEP)) ? OperationType.Local : OperationType.Remote;
          FileUtility removeControlCharUtil = FileUtilityFactory.getFileUtility(procModel, opType);
          
          try {


              if(doDirectCopy(fileModel.getSourceFileLayoutCd())){
                  logger.debug("Doing a direct copy of the file:" + newSrcFileLoc);
                  FileUtils.copyFile(new File(newSrcFileLoc), new File(newSrcFileLoc + "_1"));
              }
              else{
            	  if(ingestDao.retrievePreProcessFlag(procModel.getProcId(), country).equalsIgnoreCase("Y"))
              		  preProcessFlag = Boolean.TRUE;
                	if(preProcessFlag) {
                			try {
              		  			logger.debug("Invoking the Pre Processing Handler Class 2");
              		  			String preProcClassName = ingestDao.retrievePreProcessClassName(procModel.getProcId());
              		  			logger.debug("Pre Process Class Name is ....: " + preProcClassName);
								String preProcPropertyFile = "";
              		  			if(System.getProperty(UobConstants.PRE_PROC_CONF_FILE) !=null) {
              		  				logger.debug("System Property preprocessing-client.properties  : " + System.getProperty(UobConstants.PRE_PROC_CONF_FILE));
              		  				preProcPropertyFile=System.getProperty(UobConstants.PRE_PROC_CONF_FILE);
              		  			}

								String driverMemoryPropValue = "", executorMemoryPropValue = "", executorInstancePropValue = "", executorCoresPropValue = "";
								try {
									SparkExecParamsModel sparkPropModel = new SparkExecParamsModel();
									sparkPropModel = ingestDao.getSparkExecutionProperties(procModel.getProcId(), procInsModel.getCountryCd(),UobConstants.PARALLEL_PRE_PROCESSING_PARAM_NM);
									if(sparkPropModel !=null) {
										driverMemoryPropValue = sparkPropModel.getDriverMemory();
										executorMemoryPropValue = sparkPropModel.getExecutorMemory();
										executorInstancePropValue = sparkPropModel.getExecutorInstances();
										executorCoresPropValue = sparkPropModel.getExecutorCores();
									}
									logger.debug("Checked metadata table for Spark Properties.");
								}
								catch(Exception e) {
									e.printStackTrace();
								}
								logger.debug("setting dynamic spark properties from Property File Info : "+ driverMemoryPropValue + " - " + executorMemoryPropValue + " - " + executorInstancePropValue+" - "+executorCoresPropValue);

								HashMap<String, String> paramsMap = new HashMap<String, String>();
              		  			logger.debug("Parameters ..... procId: " + procModel.getProcId() + " ctryCd: " +country+" bizDt: "+bizDate+" procInstanceId: "+procInsModel.getProcInstanceId() + " skipErrorRecords: "+Boolean.toString(procInsModel.isSkipErrRecordsEnabled()));
              		  			paramsMap.put("i", procModel.getProcId());
              		  			paramsMap.put("c", country);
              		  			paramsMap.put("d", bizDate);
              		  			paramsMap.put("n", procInsModel.getProcInstanceId());
              		  			paramsMap.put("p", Boolean.toString(procInsModel.isSkipErrRecordsEnabled()));
              		  			paramsMap.put("t", currentTime);
								paramsMap.put("a", preProcPropertyFile);
                                paramsMap.put("dm", driverMemoryPropValue);
                                paramsMap.put("em", executorMemoryPropValue);
                                paramsMap.put("ei", executorInstancePropValue);
                                paramsMap.put("ec", executorCoresPropValue);
              		  			logger.debug("PRE_PROC_CONF_FILE : " + preProcPropertyFile);
              		  			//	paramsMap.put("procId", "");
              		  			Class cla= Class.forName(preProcClassName);
              		  			HookFrameworkInterface hfiObhj = (HookFrameworkInterface)cla.newInstance();
              		  			Map<String,String> preProcResultsMap = hfiObhj.execute(paramsMap);
              		  			logger.info("The pre processing result is : " + preProcResultsMap.get("status"));
              		  			logger.info("The pre processing result description is : " + preProcResultsMap.get("description"));
              		  			if(preProcResultsMap.get("status").equalsIgnoreCase("0"))
              		  				preProcessResult = Boolean.TRUE;
              		  			else
              		  			{
              		  				throw new EDAGValidationException(EDAGValidationException.PRE_PROCESSING_EXCEPTION, preProcResultsMap.get("description"));
              		  			}
              				} 
              				/*catch (ClassNotFoundException ce) {
              					ce.printStackTrace();
              				} */
              				catch (Exception e) {
              					throw e;
              				}
							finally{ // added sm186140
								HadoopUtils.resetUGI(); // reset the UGI, so that the delegation tokens are updated.
							}
                  }
                  logger.debug("Checking Pre Process Result Flag.... : " + preProcessResult);
            	  if(!preProcessResult) {
            		  logger.debug("Control Character removal...");
                    if (MapUtils.isEmpty(controlCharacterMap)) {
                      hasBadLines = removeControlCharUtil.removeControlCharacter(newSrcFileLoc, newSrcFileLoc + "_1", procModel.getSrcInfo(),
                              procInsModel.getControlModel(), procModel.getDestInfo(), charset, procInsModel.isSkipErrRecordsEnabled(), linesToValidate);
                    } else {
                      hasBadLines = removeControlCharUtil.removeControlCharacter(newSrcFileLoc, newSrcFileLoc + "_1", procModel.getSrcInfo(),
                              procInsModel.getControlModel(), procModel.getDestInfo(), charset, procInsModel.isSkipErrRecordsEnabled(), controlCharacterMap);
                    }
                  
                    if(hasBadLines) {
                      handleErrorRecords(newSrcFileLoc, procInsModel, procModel);
                    }
            	  }
              }
          } catch (EDAGValidationException e) {
          	// EDF-209
          	if (UobConstants.HISTORY_CD.equalsIgnoreCase(procModel.getProcFreqCd())) {
          		logger.info(procInsModel.getProcId() + " is a history process, dumping " + newSrcFileLoc + " into dump table");
          		this.fileUtils.copyFile(newSrcFileLoc, newSrcFileLoc + "_1");
          		
          	  // remove header and footer if necessary
          		if (StringUtils.isNotBlank(procInsModel.getControlModel().getFooterLine())) {
          			this.fileUtils.removeHeaderAndFooter(newSrcFileLoc + "_1", procInsModel.getBizDate(), charset);
          		}
          		
          		procInsModel.setDumped(true);
          		procInsModel.setException(e);
          	} else {
          		// rethrow the exception
          		throw e;
          	}
          }
          
          ingestDao.updateProcessLogFileName(procInsModel);
          fileModel.setSourceDirectory(newSrcFileLoc + "_1");

          logger.debug("File " + fileModel.getSourceDirectory() + 
          		        " moved successfully to Processing folder " + newSrcFileLoc);
        }
      } else {
        if (StringUtils.isNotEmpty(forceFileName)) {
          logger.debug("Going to use the file: " + forceFileName + " for processing");
          fileModel.setSourceDirectory(forceFileName);
          procInsModel.setFileNm(forceFileName);
          ingestDao.updateProcessLogFileName(procInsModel);
        } else {
          newSrcFileLoc = PropertyLoader.getProperty(UobConstants.LANDING_AREA_PROCESSING_DIR_PATH);
          DateFormat sdf = new SimpleDateFormat("yyyyMMddHHmmss");
          String currentTime = sdf.format(System.currentTimeMillis());
          String dataDir = PropertyLoader.getProperty(UobConstants.DATADIR);
          newSrcFileLoc = newSrcFileLoc.replace(UobConstants.DATADIR_STR_PARAM, dataDir)
          		                         .replace(UobConstants.SRC_SYS_PARAM, procModel.getSrcSysCd().toLowerCase())
          		                         .replace(UobConstants.ENV_PARAM, environment)
          		                         .replace(UobConstants.ENV_NUM_PARAM, environmentNum)
          		                         .replace(UobConstants.FREQ_PARAM, procModel.getProcFreqCd())
          		                         .replace(UobConstants.FILENM_PARAM, procModel.getProcNm())
          		                         .replace(UobConstants.SYS_TS_PARAM, currentTime)
          		                         .replace(UobConstants.COUNTRY_PARAM, procInsModel.getCountryCd().toLowerCase())
          		                         .replace(UobConstants.BIZ_DATE_PARAM, procInsModel.getBizDate());
          procInsModel.setFileNm(newSrcFileLoc);
          
          logger.debug("Going to move the file " + fileModel.getSourceDirectory() + 
                       " to Processing folder:" + newSrcFileLoc);
          this.fileUtils.moveFile(fileModel.getSourceDirectory(), newSrcFileLoc);

          ingestDao.updateProcessLogFileName(procInsModel);
          fileModel.setSourceDirectory(newSrcFileLoc);

          logger.debug("File " + fileModel.getSourceDirectory() + 
          						 " moved successfully to Processing folder " + forceFileName);
        }
      }

    
      if(!procModel.getSrcSysCd().equalsIgnoreCase(UobConstants.ADOBE_SITE_CATALYST_SRC_SYS_CD)) {
        // Biz Date Validation
        if (bizDate == null) {
          throw new EDAGValidationException(EDAGValidationException.NULL_VALUE, "Business Date", "Business date cannot be null");
        } else {
      	  // EDF-209
      	  String format = StringUtils.trimToNull(procModel.getProcParam(UobConstants.PROC_PARAM_CTRL_BIZ_DATE_VALIDATION_FORMAT));
      	  SimpleDateFormat bizDateValidationFormat = format == null ? null : new SimpleDateFormat(format);
      	
      	  if (bizDateValidationFormat != null) {	
      		  // EDF-209	
      		  Date controlBizDate = DEFAULT_BUSINESS_DATE_FORMAT.parse(controlModel.getBizDate());	
      		  Date inputBizDate = DEFAULT_BUSINESS_DATE_FORMAT.parse(bizDate);	
      		  String formattedControlBizDate = bizDateValidationFormat.format(controlBizDate);	
      		  String formattedInputBizDate = bizDateValidationFormat.format(inputBizDate);	
      		  if (!formattedInputBizDate.equals(formattedControlBizDate)) {
      			  if(procInsModel.isBizDateValidationDisabled()) {
      				  logger.info("Business Date Validation is disabled");
      				  procInsModel.setBizDateValidationMessage(UobConstants.BIZ_DATE_VALIATION_MESSAGE);
      			  } else {
      			      throw new EDAGValidationException(EDAGValidationException.INVALID_VALUE, formattedInputBizDate, 	
                            "Invalid Biz Date: Control File has a different biz date: " + 	
			                                formattedControlBizDate + " from Biz Date for which job is run");	
      		     }
      		  }	
      	  } else if (!bizDate.equalsIgnoreCase(controlModel.getBizDate())) { 	
      	      if(procInsModel.isBizDateValidationDisabled()) {
      	    	logger.info("Business Date Validation is disabled");
  				procInsModel.setBizDateValidationMessage(UobConstants.BIZ_DATE_VALIATION_MESSAGE);
  			  } else {
	              throw new EDAGValidationException(EDAGValidationException.INVALID_VALUE, bizDate, 	
	        		                              "Invalid Biz Date: Control File has a different biz date: " + 	
	        																  controlModel.getBizDate() + " from Biz Date for which job is run");
  			  }
      	  }
        }
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


  private void handleErrorRecords(String srcFileLoc, ProcessInstanceModel procInsModel, ProcessModel procModel) throws EDAGIOException, EDAGMyBatisException {
      procInsModel.setSkipErrRecordsMessage(UobConstants.SKIP_ERR_RECORDS_MESSAGE);
      copyErrorFileToHDFS(srcFileLoc + "_1_0", procInsModel, procModel);
      createErrorDumpTable(procInsModel, procModel);
  }


  private void copyErrorFileToHDFS(String errSrcFileLocation, ProcessInstanceModel procInsModel, ProcessModel procModel) throws EDAGIOException {
      File src = new File(errSrcFileLocation);
      HadoopModel destModel = procModel.getDestInfo();
      String targetDir = destModel.getDumpStagingDir(procModel, procInsModel);
      logger.info("Copying Error Dump file " + src.getPath() + " from landing area to "
              + HadoopUtils.getConfigValue("fs.defaultFS") + targetDir + src.getName());
      HadoopUtils.copyToHDFS(false, true, src, new Path(targetDir + procModel.getSrcInfo().getSourceFileName()));
  }


  private void createErrorDumpTable(ProcessInstanceModel procInsModel, ProcessModel procModel) throws EDAGMyBatisException {
      HiveDao dao = new HiveDao();
      HiveGenerationDao genDao = new HiveGenerationDao();
      String sql = genDao.createDumpStagingHiveTable(procModel, procInsModel);
      dao.executeUpdate(sql, new Object[] {});
      dao.attachHivePartition(procModel.getDestInfo().getStagingDbName(), procModel.getDestInfo().getDumpStagingTableName(), procModel.getDestInfo().getStagingHivePartition());
  }


  /**
   * This method is used to create the parameter file to be used by BDM for Ingestion.
   * @param procInsModel The Process Instance Model object
   * @param procModel The Process Model object containing the metadata of the Ingestion Process
   * @param bizDate The Business Date of Ingestion
   * @param paramFilePath The file path where the parameter file is to be created
   * @throws EDAGIOException 
   * @throws EDAGXMLException 
   * @throws EDAGIOException
   * @throws EDAGXMLException
   * @throws Exception when there is an error creating the Parameter file
   */
  protected void createBdmParamFile(ProcessInstanceModel procInsModel, ProcessModel procModel, String bizDate, 
  		                           String paramFilePath) throws EDAGXMLException, EDAGIOException {
    FileModel fileModel = procModel.getSrcInfo();
    HadoopModel destModel = procModel.getDestInfo();
    ControlModel controlModel = procInsModel.getControlModel();

    Map<String, String> bdmParamMap = new HashMap<String, String>();
    File source = new File(procInsModel.getFileNm());
    bdmParamMap.put(UobConstants.BDM_T11_MAPPING_NAME, PropertyLoader.getProperty(UobConstants.BDM_T11_MAPPING_NAME));
    bdmParamMap.put(UobConstants.BDM_PROJ_NAME, PropertyLoader.getProperty(UobConstants.BDM_PROJ_NAME));
    bdmParamMap.put(UobConstants.T1_SRC_FILE, source.getName());
    bdmParamMap.put(UobConstants.T1_SRC_FILE_DIR, FilenameUtils.getFullPathNoEndSeparator(source.getAbsolutePath()));
    bdmParamMap.put(UobConstants.T1_TGT_FILE, fileModel.getSourceFileName());
    bdmParamMap.put(UobConstants.T1_TGT_FILE_DIR, procInsModel.isDumped() ? destModel.getDumpStagingDir(procModel, procInsModel) : destModel.getStagingDir()); // EDF-209
    bdmParamMap.put(UobConstants.STG_DB, destModel.getStagingDbName());
    bdmParamMap.put(UobConstants.STG_TBL, procInsModel.isDumped() ? destModel.getDumpStagingTableName() : destModel.getStagingTableName()); // EDF-209
    bdmParamMap.put(UobConstants.TGT_DB, destModel.getHiveDbName());
    bdmParamMap.put(UobConstants.TGT_ERR_TBL, destModel.getStagingTableName() + UobConstants.UNDERSCORE + "err");
    bdmParamMap.put(UobConstants.TGT_TBL, procInsModel.isDumped() ? destModel.getHiveDumpTableName() : destModel.getHiveTableName()); // EDF-209
    bdmParamMap.put(UobConstants.TGT_PROC_ID, procInsModel.getProcInstanceId());
    bdmParamMap.put(UobConstants.TGT_BIZ_DT, bizDate);
    if(!procModel.getSrcSysCd().equalsIgnoreCase(UobConstants.ADOBE_SITE_CATALYST_SRC_SYS_CD))
    	bdmParamMap.put(UobConstants.TGT_CTRY_CD, controlModel.getCtryCd());
    else
    	bdmParamMap.put(UobConstants.TGT_CTRY_CD, procInsModel.getCountryCd());
    bdmParamMap.put(UobConstants.TGT_CNX_NM, destModel.getBdmConnName());
    bdmParamMap.put(UobConstants.HDFS_CNX_NM, PropertyLoader.getProperty(UobConstants.HDFS_CNX_NM));
    bdmParamMap.put(UobConstants.HIVE_CNX_NM, PropertyLoader.getProperty(UobConstants.HIVE_CNX_NM));
    
    // Standardization Params
    Map<Integer, String> bdmRuleList = new HashMap<Integer, String>();
    
    if (!procInsModel.isDumped()) {
	    for (Integer fldNum : destModel.getDestFieldInfo().keySet()) {
	      FieldModel fldModel = destModel.getDestFieldInfo().get(fldNum);
	      List<Integer> rulesList = fldModel.getRulesList();
	      for (Integer i : rulesList) {
	        if (bdmRuleList.containsKey(i)) {
	          String existingRules = bdmRuleList.get(i);
	          String newRule = existingRules + UobConstants.COMMA + fldModel.getFieldName();
	          bdmRuleList.put(i, newRule);
	        } else {
	          bdmRuleList.put(i, fldModel.getFieldName());
	        }
	      }
	    }
    }
    
    // Validation Param
    StringBuilder validationParam = new StringBuilder();
    validationParam.append("IIF((0");
    boolean set = false;
    String ruleNums = PropertyLoader.getProperty(UobConstants.VALIDATION_RULE_NUMS);
    String[] ruleNumArr = ruleNums.split(UobConstants.COMMA);
    for (String ruleNum : ruleNumArr) {
      int rule = Integer.parseInt(ruleNum.trim());
      if (bdmRuleList.containsKey(rule)) {
        set = true;
        String columns = bdmRuleList.get(rule);
        String[] columnArr = columns.split(UobConstants.COMMA);
        String condition = PropertyLoader.getProperty("RULE_" + rule + "_CONDITION");
        for (String column : columnArr) {
          validationParam.append(condition.replace(UobConstants.FIELDNM_PARAM, column));
        }
      }
    }
    
    validationParam.append(")=0, true, false)");
    if (!set) {
      validationParam = new StringBuilder();
      validationParam.append("true");
    }
	    
    bdmParamMap.put(UobConstants.VALIDATION, validationParam.toString());
    for (int i = 1; i <= 61; i++) {
    	bdmParamMap.put(UobConstants.TGT_RULES[i], bdmRuleList.containsKey(i) ? bdmRuleList.get(i) : UobConstants.DUMMY);
    }
    
    //EDF-203
    List<FieldModel> fileReferenceFields = fileModel.getSrcFieldInfo().stream()
			.filter(f -> f.getDataType().equalsIgnoreCase(UobConstants.SRC_FILE_REFERENCE))
			.sorted((f1, f2) -> Integer.compare(f1.getFieldNum(), f2.getFieldNum())).collect(Collectors.toList());
    bdmParamMap.put(UobConstants.HDFS_REFERENCE_FOLDER, fileReferenceFields.isEmpty() ? UobConstants.DUMMY: 
    	fileModel.getHdfsTargetFolder().endsWith(UobConstants.SEPARATOR) ? fileModel.getHdfsTargetFolder() : fileModel.getHdfsTargetFolder() + UobConstants.SEPARATOR);
    for (int i = 1; i < 11; i++) {
    	bdmParamMap.put(UobConstants.SOURCE_FIELD_RULES[i], (i - 1 < fileReferenceFields.size()) ? fileReferenceFields.get(i - 1).getNormalizedFieldName() : UobConstants.DUMMY);
    }
    
    logger.debug("BDM Parameter Map is " + bdmParamMap);
    new BdmUtils().createBdmParamFileFromTemplate(PropertyLoader.getProperty(UobConstants.BDM_PARAM_TEMPLATE), paramFilePath, 
    		                                          bdmParamMap);
  }
  
  protected File getSourceFile(File paramFile, boolean fileMustExist) throws EDAGIOException, EDAGXMLException {
  	Document doc = XMLUtils.parseDocument(paramFile, true);
  	doc.getDocumentElement().setPrefix("xsi");
  	logger.debug("BDM parameter document namespace prefix set to xsi");
  	String dir = XMLUtils.evaluateXPath(SOURCE_FILE_DIR_XPATH, doc);
  	String filename = XMLUtils.evaluateXPath(SOURCE_FILE_XPATH, doc);
  	
  	File result = new File(dir, filename); 
  	if (fileMustExist && !result.isFile()) {
  		throw new EDAGIOException(EDAGIOException.FILE_NOT_FOUND, result.getPath(), "File doesn't exist or is a directory");
  	}
  	logger.debug("Param file " + paramFile.getPath() + " contains source file " + result.getPath());
  	return result;
  }
  
  private void copySourceFileToHDFS(ProcessInstanceModel procInsModel, ProcessModel procModel) throws EDAGIOException {
  	File src = new File(procInsModel.getFileNm());
    FileModel fileModel = procModel.getSrcInfo();
    HadoopModel destModel = procModel.getDestInfo();
    String targetDir = procInsModel.isDumped() ? destModel.getDumpStagingDir(procModel, procInsModel) : destModel.getStagingDir(); // EDF-209

    logger.info("Copying file " + src.getPath() + " from landing area to "
                + HadoopUtils.getConfigValue("fs.defaultFS") + targetDir + fileModel.getSourceFileName());
	HadoopUtils hUtils = new HadoopUtils();
    logger.debug("HadoopUtils hUtils = new HadoopUtils()...................");
    HadoopUtils.copyToHDFS(false, true, src, new Path(targetDir + fileModel.getSourceFileName()));
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
    	// EDF-209
    	if (procInsModel.isDumped()) {
    	  HiveDao dao = new HiveDao();
  	  	HiveGenerationDao genDao = new HiveGenerationDao();
  	  	String sql = genDao.createDumpStagingHiveTable(procModel, procInsModel);
  	  	dao.executeUpdate(sql, new Object[] {});
    	}
    	
    	if (UobUtils.parseBoolean(PropertyLoader.getProperty(UobConstants.BDM_RUN_T1_CMD))) {
	      String bdmRunCommand = PropertyLoader.getProperty(UobConstants.BDM_CMD);
	      String maskedCommand = bdmRunCommand;
	      String encryptedInfaPwd = PropertyLoader.getProperty(UobConstants.INFA_PWD);
	      String infaPwd = new EncryptionUtil().decrypt(encryptedInfaPwd);
	      bdmRunCommand = bdmRunCommand.replace(UobConstants.PARAM_FILE_PARAM, paramFilePath)
	      														 .replace(UobConstants.INFA_PWD_PARAM, infaPwd);
	      maskedCommand = maskedCommand.replace(UobConstants.PARAM_FILE_PARAM, paramFilePath)
	      		                         .replace(UobConstants.INFA_PWD_PARAM, "xxx");
	      logger.debug("Executing " + maskedCommand);
	      Process bdmProc = Runtime.getRuntime().exec(bdmRunCommand);
	      bdmProc.waitFor();
	      int bdmExitVal = bdmProc.exitValue();
	      String output = IOUtils.toString(bdmProc.getInputStream(), Charset.defaultCharset());
	      if (output != null && output.indexOf("Job with ID [") != -1 && 
	      		output.indexOf("] submitted to the Integration Service") != -1) {
	        toolId = output.substring(output.indexOf("Job with ID [") + 13, 
	        		                      output.indexOf("] submitted to the Integration Service"));
	      }
	
	      if (bdmExitVal != 0) {
	        String errorOutput = IOUtils.toString(bdmProc.getErrorStream(), Charset.defaultCharset());
	
	        String allOutput = "";
	        if (StringUtils.isNotEmpty(output)) {
	          allOutput = allOutput + "STDOUT:" + output + ";";
	        }
	        
	        if (StringUtils.isNotEmpty(errorOutput)) {
	          allOutput = allOutput + "STDERR:" + errorOutput;
	        }
	        
	        logger.error(allOutput);
	        throw new EDAGProcessorException(EDAGProcessorException.EXT_CMD_ERROR, maskedCommand, bdmExitVal);
	      }
    	} else {
    		copySourceFileToHDFS(procInsModel, procModel);
    	}

      // Attach Hive Partition
      HadoopModel destModel = procModel.getDestInfo();
      String schemaName = destModel.getStagingDbName();
      String tableName = procInsModel.isDumped() ? destModel.getDumpStagingTableName() : destModel.getStagingTableName(); // EDF-209
      String partitionValue = destModel.getStagingHivePartition();
      
      HiveDao dao = new HiveDao();
      logger.debug("Going to attach Hive Partition: " + partitionValue + 
      		         " To Staging Table: " + schemaName + "." + tableName);
      dao.attachHivePartition(schemaName, tableName, partitionValue);
      
      // EDF-209
      int stagingRowCount = dao.getRowCountByPartition(schemaName, tableName, partitionValue, destModel.getHadoopQueueName());
      ControlModel controlModel = procInsModel.getControlModel();
      if (controlModel != null) {
      	controlModel.setTargetDbName(schemaName);
      	controlModel.setTargetTableName(tableName);
      	controlModel.setTotalRecordsStaging(stagingRowCount);


		  // PEDDAGML - 1285
		  if(checkIfAllRecordsAreInErrorTable(procInsModel, procModel)){

			  String errMessage = "Aborting ingestion. All records are in Staging error table, \n info={ " +
					  "bizDate= " + procInsModel.getBizDate()
					  + ", stagingTableNm= " + controlModel.getTargetTableName()
					  + ", stagingCount= " + controlModel.getTotalRecordsStaging()
					  + ", stagingErrTableNm= " + destModel.getStagingErrorTableName()
					  + ", stagingErrCount= " + controlModel.getTotalErrRecordsTarget()
					  +" }";

			  logger.error(errMessage);
			  throw new EDAGException(errMessage);
		  }



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



  protected void runTierT11Process(ProcessInstanceModel procInsModel, ProcessModel procModel, String paramFilePath) throws EDAGException {
	  boolean isUseSparkIngestion = procModel.getSrcInfo().getIsUseSparkBasedIngestion();
	  String toolId = UobConstants.EMPTY;
	  EDAGException instanceException = null;
	  try {
		  stgHndle.addStageLog(procInsModel, UobConstants.STAGE_INGEST_T11_BDM_EXEC);
		  logger.info("Going to run T1.1 ingestion process");
		  
	      // EDF-209
	      if (procInsModel.isDumped()) {
	      	HiveDao dao = new HiveDao();
	  	  	HiveGenerationDao genDao = new HiveGenerationDao();
	  	  	String sql = genDao.createDumpHiveTable(procModel);
	  	  	dao.executeUpdate(sql, new Object[] {});
	      }
		  
		  if(isUseSparkIngestion) {
			  SparkIngestionLauncher sparkLauncher = new SparkIngestionLauncher();
			  toolId = sparkLauncher.launchSparkApplication(procInsModel, procModel);
		  } else {
			  toolId = runBdmTier11Process(procInsModel, procModel, paramFilePath);
		  }
	  
		  HadoopModel destModel = procModel.getDestInfo();
	      String schemaName = destModel.getHiveDbName();
	      String tableName = procInsModel.isDumped() ? destModel.getHiveDumpTableName() : destModel.getHiveTableName(); // EDF-209
	      String partitionValue = destModel.getHivePartition();
	      String queueName = destModel.getHadoopQueueName();
	      HiveDao dao = new HiveDao();
	      
	      // Run Statistics
	      boolean runStats = UobUtils.parseBoolean(PropertyLoader.getProperty(UobConstants.HIVE_RUN_STATS));
	      if (runStats) {
	        logger.info("Going to run Hive Statistics on T1.1 Table");
	        	dao.runStatistics(schemaName, tableName, partitionValue, queueName);
	      }
	      
	      // EDF-209
	      int hiveRowCount = dao.getRowCountByPartition(schemaName, tableName, partitionValue, destModel.getHadoopQueueName());

	      // PEDDAGML - 1285, commented out
	      // int errorRowCount = dao.getErrorRowCount(destModel.getStagingDbName(), destModel.getStagingErrorTableName(), procInsModel.getBizDate(), queueName);
	      
	      ControlModel controlModel = procInsModel.getControlModel();
	      if (controlModel != null) {
	      	controlModel.setTargetDbName(schemaName);
	      	controlModel.setTargetTableName(tableName);
	      	controlModel.setTotalRecordsTarget(hiveRowCount);

	      	//PEDDAGML - 1285, commented out
	      	//controlModel.setTargetErrorTableName(destModel.getStagingDbName() + "." + destModel.getStagingErrorTableName());
	      	//controlModel.setTotalErrRecordsTarget(errorRowCount);
	      }
	      
	      // Run Impala Refresh except for FIXED_FILES
	      boolean runRefresh = UobUtils.parseBoolean(UobConstants.N);
	      FileModel fileModel = procModel.getSrcInfo();
          String fileLayoutId = StringUtils.trimToEmpty(fileModel.getSourceFileLayoutCd());
          if(!UobConstants.FIXED_FILE.equalsIgnoreCase(fileLayoutId)) {
        	  logger.debug("Checking IMPALA_RUN_REFRESH Flag for files except FIXED_FILES...");
        	  runRefresh = UobUtils.parseBoolean(PropertyLoader.getProperty(UobConstants.IMPALA_RUN_REFRESH));
        	  if(UobUtils.bypassImpalaForCustomSerDe(procModel))
        		  runRefresh = Boolean.FALSE;
        	  logger.debug("IMPALA_RUN_REFRESH flag final value :- "+ runRefresh);
          }
          
	      if (runRefresh) {
	        ImpalaDao impalaDao = new ImpalaDao();
	        impalaDao.runRefresh(schemaName, tableName, partitionValue);
	        impalaDao.runRefresh(destModel.getStagingDbName(), destModel.getStagingErrorTableName(), destModel.getStagingHivePartition());
	        impalaDao.runRefresh(destModel.getStagingDbName(), procInsModel.isDumped() ? destModel.getDumpStagingTableName() : destModel.getStagingTableName(), 
	        		destModel.getStagingHivePartition()); // EDF-209
	      }
	
	      environment = PropertyLoader.getProperty(UobConstants.ENVIRONMENT);
	      environmentNum = PropertyLoader.getProperty(UobConstants.ENVIRONMENT_NUM);
	      // Archive BDM Param File
	      String paramArchiveFilePath = PropertyLoader.getProperty(UobConstants.BDM_PARAM_FILE_PATH_ARCHIVE);
	      
	      paramArchiveFilePath = paramArchiveFilePath.replace(UobConstants.SRC_SYS_PARAM, procModel.getSrcSysCd())
	      																					 .replace(UobConstants.ENV_PARAM, environment)
	      																					 .replace(UobConstants.ENV_NUM_PARAM, environmentNum)
	      																					 .replace(UobConstants.FREQ_PARAM, procModel.getProcFreqCd())
	      																					 .replace(UobConstants.FILENM_PARAM, procModel.getProcNm())
	      																					 .replace(UobConstants.COUNTRY_PARAM, procInsModel.getCountryCd())
	      																					 .replaceAll(UobConstants.BIZ_DATE_PARAM, procInsModel.getBizDate());
	      // Archive BDM Param File
	      LinuxUtils.moveFile(paramFilePath, paramArchiveFilePath);
	      stgHndle.updateStageLog(procInsModel, UobConstants.STAGE_INGEST_T11_BDM_EXEC, toolId);
	  } catch (EDAGException excp) {
	    	instanceException = excp;
	  } catch (Throwable excp) {	
	    	instanceException = new EDAGException(excp.getMessage(), excp);
	  } finally {
		  if (instanceException != null) {
				try {
					logger.info("Dropping hive partition due to exception");
					dropHivePartition(procModel);
				} catch (Exception e) {
					logger.error("Error dropping hive partion - runProcessFinalization", e);
				}
			  procInsModel.setException(instanceException); 
			  stgHndle.updateStageLog(procInsModel, UobConstants.STAGE_INGEST_T11_BDM_EXEC, toolId);
			  throw instanceException;
		  }
	  }
  }

  /**
   * This method is used to run the Tier 1.1 BDM Mapping for ingesting the file into the DDS area.
   * @param procInsModel The Process Instance Model object
   * @param procModel The Process Model object containing the metadata of the Ingestion Process
   * @param paramFilePath The parameter file path to be used by BDM
   * @throws Exception when there is any error in the Tier 1.1 BDM process
   */
   protected String runBdmTier11Process(ProcessInstanceModel procInsModel, ProcessModel procModel, 
  		                             String paramFilePath) throws EDAGException {
    String toolId = null;
    try {
      logger.info("Going to run BDM Tier 1.1 Process");
      String bdmRunCommand = PropertyLoader.getProperty(UobConstants.BDM_T11_CMD, true);
      String maskedCommand = bdmRunCommand;
      String encryptedInfaPwd = PropertyLoader.getProperty(UobConstants.INFA_PWD);
      String infaPwd = new EncryptionUtil().decrypt(encryptedInfaPwd);
      bdmRunCommand = bdmRunCommand.replace(UobConstants.PARAM_FILE_PARAM, paramFilePath)
      														 .replace(UobConstants.INFA_PWD_PARAM, infaPwd);
      maskedCommand = maskedCommand.replace(UobConstants.PARAM_FILE_PARAM, paramFilePath)
					 												 .replace(UobConstants.INFA_PWD_PARAM, "xxx");
      logger.debug("Executing BDM run command: " + maskedCommand);
      Process bdmProc = Runtime.getRuntime().exec(bdmRunCommand);
      bdmProc.waitFor();
      int bdmExitVal = bdmProc.exitValue();
      String output = IOUtils.toString(bdmProc.getInputStream(), Charset.defaultCharset());

      if (output != null && output.indexOf("Job with ID [") != -1 && 
      	  output.indexOf("] submitted to the Integration Service") != -1) {
        toolId = output.substring(output.indexOf("Job with ID [") + 13,
            											output.indexOf("] submitted to the Integration Service"));
      }

      if (bdmExitVal != 0) {
        String errorOutput = IOUtils.toString(bdmProc.getErrorStream(), Charset.defaultCharset());

        String allOutput = "";
        if (StringUtils.isNotEmpty(output)) {
          allOutput = allOutput + "STDOUT: " + output + ";";
        }
        
        if (StringUtils.isNotEmpty(errorOutput)) {
          allOutput = allOutput + "STDERR: " + errorOutput;
        }
        
        logger.error(allOutput);
        throw new EDAGProcessorException(EDAGProcessorException.EXT_CMD_ERROR, maskedCommand, bdmExitVal);
      }

     return toolId;
    } catch (Throwable excp) {
    	throw new EDAGException(excp.getMessage(), excp);
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
  private void runProcessFinalization(ProcessInstanceModel procInsModel, ProcessModel procModel, boolean skipBDMProcess) throws EDAGException {
      EDAGException instanceException = null;
      try {
          stgHndle.addStageLog(procInsModel, UobConstants.STAGE_INGEST_PROC_FINAL);
          logger.debug("Going to start Reconciliation for process:" + procInsModel.getProcInstanceId());

          // If local env without Informatica and HDFS / Hive then skip that step
          Properties props = System.getProperties();
          if (!(props.containsKey("localDevEnv") && System.getProperty("localDevEnv").equals("true"))) {
              if (skipBDMProcess || !checkIfReconciliationRequired (procInsModel, procModel)) {
                  logger.info("Reconciliation is skipped as it is not required");
              } else {
                  try {
                      // Validate Threshold
                      ReconciliationUtil util = new ReconciliationUtil();
                      FileModel fileModel = procModel.getSrcInfo();
                      String fileLayoutId = StringUtils.trimToEmpty(fileModel.getSourceFileLayoutCd());
                      logger.debug("Use Impala Flag currently is set to :" + util.getUseImpalaFlag());
                      if(UobConstants.FIXED_FILE.equalsIgnoreCase(fileLayoutId)) {
                    	  logger.info("Changing the reconciliation engine to Hive by default for FIXED FILE...");
                    	  util.setUseImpalaFlag(false);
                    	  logger.debug("Use Impala Flag currently is set to :" + util.getUseImpalaFlag());
                      } else if(UobUtils.bypassImpalaForCustomSerDe(procModel)){ // Added a new condition to avoid regression testing
                    	  logger.info("Changing the reconciliation engine to Hive by default for "+fileLayoutId);
                    	  util.setUseImpalaFlag(false);
                    	  logger.debug("Use Impala Flag currently is set to :" + util.getUseImpalaFlag());
                      }
                      int errRowCnt = util.runErrorThresholdValidation(procInsModel, procModel);

                      // Reconciliation
                      ControlModel controlModel = procInsModel.getControlModel();
                      if (StringUtils.isNotEmpty(controlModel.getHashSumCol()) && errRowCnt == 0) {
                    	  logger.debug("Running row count and hash sum validation on column " + controlModel.getHashSumCol());
                    	  util.runRowCountAndSumValidation(procInsModel, procModel, errRowCnt);
                      } else { // Row count validation only
                    	  util.runRowCountValidation(procInsModel, procModel, errRowCnt);
                      }
                     
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
          // Archive Source File
          removeTempFile(procInsModel);
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
  
  protected void dropHivePartition(ProcessModel procModel) throws EDAGMyBatisException {
  	// Truncate Partition on T1.1 table
    HiveDao dao = new HiveDao();
    HadoopModel destModel = procModel.getDestInfo();
    String schemaName = destModel.getHiveDbName();
    String tableName = destModel.getHiveTableName();
    String partition = destModel.getHivePartition();
    dao.dropHivePartition(schemaName, tableName, partition);
    
    String dumpTableName = destModel.getHiveDumpTableName();
    dao.dropHivePartition(schemaName, dumpTableName, partition);
	}
  
  protected void dropT1ErrorPartition(ProcessModel procModel) throws EDAGMyBatisException {
  	HiveDao dao = new HiveDao();
  	HadoopModel destModel = procModel.getDestInfo();
  	// truncate partition on T1 error table
    String schemaName = destModel.getStagingDbName();
    String tableName = destModel.getStagingErrorTableName();
    String partition = destModel.getStagingHivePartition();
    dao.dropHivePartition(schemaName, tableName, partition);
  }

	/**
   * This method is used to remove the temporary source file from the processing folder.
   * @param procInsModel The Process Instance Model object
	 * @throws EDAGValidationException
	 * @throws EDAGSSHException 
   * @throws Exception when there is an error removing the temp source file
   */
  private void removeTempFile(ProcessInstanceModel procInsModel) throws EDAGException {
  	this.fileUtils.deleteFile(procInsModel.getFileNm());
  	this.fileUtils.deleteFile(procInsModel.getOrigFileNm());
  	File file = new File(procInsModel.getFileNm()+"_0");
  	if(file.exists()) {
  		this.fileUtils.deleteFile(procInsModel.getFileNm()+"_0");
  	}
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
    // Retrieve Execution Parameters from File
    FileModel fileModel = procModel.getSrcInfo();

    // Setting Source Dir Path
    String filePath = fileModel.getSourceDirectory();

    String controlInfo = StringUtils.trimToNull(fileModel.getControlInfo());
    ControlModel controlModel = null;

    // If Header / Trailer
    if (UobConstants.CTRL_INFO_HT.equalsIgnoreCase(controlInfo)) {
      logger.info("Control Information is Header and Trailer");
      String charsetName = PropertyLoader.getCharsetName(procInsModel.getCountryCd());
      
      String header = fileUtils.readLineByIndicator(filePath, UobConstants.FIRST, charsetName);
      String footer = fileUtils.readLineByIndicator(filePath, UobConstants.LAST, charsetName);

      // parse and validate control information from header and footer
      controlModel = parseControlInfo(procModel, header, footer);

      String hashSumColName = null;
      for (FieldModel fldModel : procModel.getSrcInfo().getSrcFieldInfo()) {
        if (fldModel.isHashSumField()) {
          hashSumColName = fldModel.getFieldName();
        }
      }
      
      if (StringUtils.isNotEmpty(hashSumColName)) {
        logger.info("Control Information: HashSum Column: " + hashSumColName);
        controlModel.setHashSumCol(hashSumColName);
      }
    } else if (UobConstants.CTRL_INFO_C.equalsIgnoreCase(controlInfo)) { // If Control File
      logger.info("Control Information is in a separate controlFile");
      // Read Lines for the File
      String controlFilePath = fileModel.getControlFileDir();
      logger.info("Going to read Control File: " + controlFilePath);

      List<String> lineList = this.fileUtils.readFile(controlFilePath, PropertyLoader.getCharsetName(procInsModel.getCountryCd()));
      if (CollectionUtils.isEmpty(lineList)) {
        throw new EDAGValidationException(EDAGValidationException.EMPTY_VALUE, "Control line", "Control file is empty");
      }

      for (String controlLine : lineList) {
        ControlModel tempControlModel = parseControlInfo(procModel, controlLine);
        String fileName = procModel.getSrcInfo().getSourceFileName();
        if (tempControlModel != null && StringUtils.isNotEmpty(tempControlModel.getFileName()) &&
        		tempControlModel.getFileName().trim().equalsIgnoreCase(fileName.trim())) {
          controlModel = tempControlModel.clone();
          logger.info("Setting Control Model to: " + controlModel);
          break;
        }
      }
      
      if (controlModel == null) {
        throw new EDAGValidationException(EDAGValidationException.NULL_VALUE, "Control Model", "No Control Information found for the file");
      }
    } else {
      logger.error("Invalid Control Code: " + controlInfo);
      // Assuming it is for Files without control information.
      controlModel = new ControlModel();
      controlModel.setHeaderLine(UobConstants.EMPTY);
      controlModel.setFooterLine(UobConstants.EMPTY);
      controlModel.setTotalRecords(noOfRecords);
      controlModel.setBizDate(procInsModel.getBizDate());
    } 

    verifyCountryCode(controlModel, ctryCd);
    procInsModel.setControlModel(controlModel);
  }
  
  private void verifyCountryCode(ControlModel controlModel, String ctryCd) throws EDAGValidationException {
  	if (ctryCd == null) {
  		throw new EDAGValidationException(EDAGValidationException.NULL_VALUE, "country code", "Country code cannot be null");
  	}
  	
  	if (controlModel == null) {
  		throw new EDAGValidationException(EDAGValidationException.NULL_VALUE, "control model", "Control model cannot be null");
  	}
  	
  	if (StringUtils.isEmpty(controlModel.getCtryCd())) {
      controlModel.setCtryCd(ctryCd);
    } else if (!ctryCd.equalsIgnoreCase(controlModel.getCtryCd())) {
      throw new EDAGValidationException(EDAGValidationException.CONTROL_VALUE_MISMATCH, "Country Code", ctryCd, 
      		                              controlModel.getCtryCd());
    }
  	
  	logger.info("Country code " + ctryCd + " verified");
  }

  /**
   * This method is used to parse the control information for the source file being ingested. The
   *     control information will be in the header/footer of the source file.
   * @param procModel The Process Model object containing the metadata of the Ingestion Process
   * @param header The header line in the source file from which to capture the control info
   * @param footer The footer line in the source file from which to capture the control info
   * @throws EDAGValidationException 
   * @throws Exception when there is an error parsing the header line or trailer line.
   */
  private ControlModel parseControlInfo(ProcessModel procModel, String header, String footer) throws EDAGValidationException {
    FileModel fileModel = procModel.getSrcInfo();
    String fileLayoutId = StringUtils.trimToEmpty(fileModel.getSourceFileLayoutCd());
    ControlModel ctrlModel = new ControlModel();
    ctrlModel.setHeaderLine(header);
    ctrlModel.setFooterLine(footer);
    ctrlModel.setExplicitDecimalPoint(!UobConstants.N.equalsIgnoreCase(fileModel.getExplicitDecimalPoint()));
    List<FieldModel> fldModelList = fileModel.getCtrlInfo();

    List<EDAGValidationException> controlInfoExceptions = new ArrayList<EDAGValidationException>();
    if (UobConstants.FIXED_FILE.equalsIgnoreCase(fileLayoutId) || UobConstants.FIXED_TO_DELIMITED_FILE.equalsIgnoreCase(fileLayoutId)) {
      for (FieldModel model : fldModelList) {
        if (model.getRecordType().isHeader()) {
          model.parseFixedWidthLine(header);
        } else if (model.getRecordType().isFooter()) {
          model.parseFixedWidthLine(footer);
        }
        
        model.validateRecordTypeIndicator();
        
        try {
        	ctrlModel.setControlInfoFromFieldModel(model);
        } catch (EDAGValidationException e) {
        	controlInfoExceptions.add(e);
        } catch (Exception e) {
        	controlInfoExceptions.add(new EDAGValidationException(EDAGValidationException.INVALID_VALUE, model.getFieldValue(), e.getMessage(), e));
        }
      }
    } else if (UobConstants.DELIMITED_FILE.equalsIgnoreCase(fileLayoutId.trim())) {
      String colDel = StringUtils.trimToEmpty(fileModel.getColumnDelimiter());
      colDel = colDel.replaceAll("\\|", "\\\\\\|");
      
      String txtDel = fileModel.getTextDelimiter();
      for (FieldModel model : fldModelList) {
        if (model.getRecordType().isHeader()) {
          model.parseDelimitedLine(header, colDel, txtDel);
        } else if (model.getRecordType().isFooter()) {
          model.parseDelimitedLine(footer, colDel, txtDel);
        }
        
        model.validateRecordTypeIndicator();
        
        try {
        	ctrlModel.setControlInfoFromFieldModel(model);
        } catch (EDAGValidationException e) {
        	controlInfoExceptions.add(e);
        } catch (Exception e) {
        	controlInfoExceptions.add(new EDAGValidationException(EDAGValidationException.INVALID_VALUE, model.getFieldValue(), e.getMessage(), e));
        }
      }
    }
    
    if (!controlInfoExceptions.isEmpty()) {
    	throw controlInfoExceptions.get(0); // just throw the 1st item in the list. This is to ensure missing header / footer exception gets thrown first
    }
    
    return ctrlModel;
  }

	/**
   * This method is used to parse the control information for the source file being ingested. The
   *     control information will be in control file
   * @param procModel The Process Model object containing the metadata of the Ingestion Process
   * @param controlLine The control line in the control file from which to capture the control info
   * @throws Exception when there is an error parsing the control line.
   */
  private ControlModel parseControlInfo(ProcessModel procModel, String controlLine) throws EDAGValidationException, EDAGIOException {
    FileModel fileModel = procModel.getSrcInfo();
    boolean fallback = false;
    String fileLayoutId = StringUtils.trimToNull(fileModel.getControlFileLayoutCd());
    if (fileLayoutId == null) {
    	fallback = true;
    	fileLayoutId = StringUtils.trimToNull(fileModel.getSourceFileLayoutCd()); // fallback to source file layout code, in case the control file hasn't been re-registered
    }
    ControlModel ctrlModel = new ControlModel();
    ctrlModel.setExplicitDecimalPoint(!UobConstants.N.equalsIgnoreCase(fileModel.getCtrlFileExplicitDecimalPoint()));
    List<FieldModel> fldModelList = fileModel.getCtrlInfo();

    if (UobConstants.FIXED_FILE.equalsIgnoreCase(fileLayoutId) || 
    		UobConstants.FIXED_TO_DELIMITED_FILE.equalsIgnoreCase(fileLayoutId) || 
     		(fallback && fileModel.getColumnDelimiter().equals(UobConstants.BELL_CHAR))) { 
      for (FieldModel model : fldModelList) {
        model.parseFixedWidthLine(controlLine);
        ctrlModel.setControlInfoFromFieldModel(model);
      }
    } else if (UobConstants.DELIMITED_FILE.equalsIgnoreCase(fileLayoutId)) {
      String colDel = fallback ? fileModel.getColumnDelimiter() : fileModel.getControlFileColumnDelimiter();
      colDel = colDel.replaceAll("\\|", "\\\\\\|");
      
      String txtDel = fallback ? fileModel.getTextDelimiter() : fileModel.getControlFileTextDelimiter();
      for (FieldModel model : fldModelList) {
        model.parseDelimitedLine(controlLine, colDel, txtDel);
        ctrlModel.setControlInfoFromFieldModel(model);
      }
    }
    
    logger.debug("Parsed Control Model is: " + ctrlModel);
    return ctrlModel;
  }

  /**
   * This method is used to run the Ingestion Process for file based sources.
   * @param procInstanceModel The process Instance Model object
   * @param procModel The Process Model object of the Ingestion Process
   * @param bizDate The business date of the process
   * @param ctryCd The country code of the process
   * @param forceRerun Indicator to show if the Ingestion has to be force rerun from the start
   * @param forceFileName Name of the file which has to force ingested
   * @throws Exception when there is an error in the file Ingestion process
   */
  public void runFileIngestion(ProcessInstanceModel procInstanceModel, ProcessModel procModel, String bizDate, 
  		                         String ctryCd, boolean forceRerun, String forceFileName) throws EDAGException {
    logger.info("Going to run File Ingestion for process: " + procModel.getProcId());
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
      // if property doesn't exist in framework-conf.properties, drop partition on reconciliation failure, since it's the original behaviour
      this.dropHivePartitionOnReconciliationFailure = drop == null ? true : UobUtils.parseBoolean(drop);  
    
      // Verify Previous Run Status
      prevStatusModel = ingestDao.getPrevRunStatus(procInstanceModel);
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
            logger.info("Found existing instance of this process; continuing process");
            stgModelList = ingestDao.getStageInfo(prevStatusModel);

            String procInstanceId = prevStatusModel.getProcInstanceId();
            String newSrcFileLoc = ingestDao.retrieveProcessFileName(procInstanceId);
            if (StringUtils.isNotEmpty(newSrcFileLoc)) {
              procInstanceModel.setFileNm(newSrcFileLoc);
              ingestDao.updateProcessLogFileName(procInstanceModel);
              procModel.getSrcInfo().setSourceDirectory(newSrcFileLoc);
            }
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
      
      // start: added by Tyler for OSM
      
		String OSMFlag = null;
		for (int i = 0; i < procModel.getProcParam().size(); i++) {
			ProcessParam paramObj = procModel.getProcParam().get(i);
			if (paramObj.getParamName().equalsIgnoreCase(UobConstants.OSM_MERGE_FILE_FLAG)) {
				OSMFlag = paramObj.getParamValue().trim();
				break;
			}
		}
		
		logger.debug("source file path: " + procModel.getSrcInfo().getSourceDirectory());

			if ((procModel.getSrcSysCd().equalsIgnoreCase("OSM")) || (OSMFlag != null && "TRUE".equalsIgnoreCase(OSMFlag))) {
				
				logger.debug("coming to OSM branch: " + procModel.getSrcInfo().getSourceDirectory());

				File source = new File(procModel.getSrcInfo().getSourceDirectory());
				File file01 = new File(source.getParent() + "/" + procModel.getSrcInfo().getSourceFileName() + "_01");
				File file02 = new File(source.getParent() + "/" + procModel.getSrcInfo().getSourceFileName() + "_02");
				
				logger.debug("coming to OSM branch: file01: " + file01.getParent() + " | " +file01.getName());
				logger.debug("coming to OSM branch: file02: " + file02.getParent() + " | " +file02.getName());
				logger.debug("coming to OSM branch: file01.exists() " + file01.exists());
				logger.debug("coming to OSM branch: file02.exists() " + file02.exists());

				if (file01.exists() && file02.exists()) {
					
					logger.debug("coming to OSM branch f1 and f 2 exist and starting merge files...");
					
					OSMMergerUtil mergerUtil = new OSMMergerUtil();
					String output = mergerUtil.mergeFileBuffer(procModel.getSrcInfo().getSourceFileName(), source.getParent());
					logger.info("OSM files are merged. " + output);
				} else {
					logger.info("OSM files are not existed hence no need merge");
				}

			}
		// end: added by Tyler for OSM
	 
      if(filesNeedsToBeArchived(procModel)) {
    	  // archive file from landing location to previous folder
    	  String archiveFilePath = procModel.getSrcInfo().getSourceArchivalDir();
    	  archiveFilePath = archiveFilePath.replaceAll(UobConstants.BIZ_DATE_PARAM, procInstanceModel.getBizDate());
    	  
    	  // start: added by Tyler for OSM
			if (procModel.getSrcSysCd().equalsIgnoreCase("OSM") || (OSMFlag != null && "TRUE".equalsIgnoreCase(OSMFlag))) {
				logger.info("** archiveFileOSM is starting...");
				 this.fileUtils.archiveFileOSM(procModel, archiveFilePath, true);					
			}else {
				this.fileUtils.archiveFile(procModel.getSrcInfo().getSourceDirectory(), archiveFilePath, false);
			}
	       // end: added by Tyler for OSM
    	 // this.fileUtils.archiveFile(procModel.getSrcInfo().getSourceDirectory(), archiveFilePath, false);
      }
    
      if(!procModel.getSrcSysCd().equalsIgnoreCase(UobConstants.ADOBE_SITE_CATALYST_SRC_SYS_CD)) {
    	  // Read Header/Footer or Control File Information
    	  parseControlInformation(procModel, procInstanceModel, ctryCd);
      }
      
      procInitCompleted = stgHndle.checkStepCompleted(stgModelList, procInstanceModel, UobConstants.STAGE_INGEST_PROC_INIT);
      paramFilePath = PropertyLoader.getProperty(UobConstants.BDM_PARAM_FILE_PATH);
      paramFilePath = paramFilePath.replace(UobConstants.SRC_SYS_PARAM, procModel.getSrcSysCd())
      														 .replace(UobConstants.ENV_PARAM, environment)
      														 .replace(UobConstants.ENV_NUM_PARAM, environmentNum)
      														 .replace(UobConstants.FREQ_PARAM, procModel.getProcFreqCd())
      														 .replace(UobConstants.FILENM_PARAM, procModel.getProcNm())
      														 .replace(UobConstants.BIZ_DATE_PARAM, procInstanceModel.getBizDate())
      														 .replace(UobConstants.COUNTRY_PARAM, ctryCd);
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
    logger.debug("after runProcessInitialization: controlModel: " + controlModel);

    // Run BDM Ingestion Job - Tier 1
    // If local env without Informatica and HDFS / Hive then skip that step
    boolean procFinalCompleted;
    boolean skipBDMProcess = false;
    Properties props = System.getProperties();
    if (props.containsKey("localDevEnv") && System.getProperty("localDevEnv").equals("true")) {
      procFinalCompleted = false;
    } else {
    	boolean processEmptyFile = Boolean.parseBoolean(PropertyLoader.getProperty(UobConstants.BDM_PROCESS_EMPTY_FILE));
    	logger.info("Process empty file flag is " + processEmptyFile);
    	
  		File sourceFile = getSourceFile(new File(paramFilePath), true);
  		boolean emptySourceFile = !containsDataRow(controlModel, sourceFile);
  		logger.debug("Source file " + sourceFile.getPath() + (emptySourceFile ? " is empty" : " is not empty"));
    	skipBDMProcess = !processEmptyFile && emptySourceFile;
    	
      boolean t1BdmCompleted = stgHndle.checkStepCompleted(stgModelList, procInstanceModel, 
      		                                                 UobConstants.STAGE_INGEST_T1_BDM_EXEC);
      if (forceRerun || !t1BdmCompleted || cleanup) {
      	if (skipBDMProcess) {
      		logger.info("Stage: Tier 1 BDM Process is skipped due to empty source file");
      	} else {
          runBdmTier1Process(procInstanceModel, procModel, paramFilePath);
      	}
      } else {
        logger.info("Stage: Tier 1 BDM Process already completed; Skipping stage");
      }

      // Run BDM Ingestion Job - Tier 1.1
      boolean t11BdmCompleted = stgHndle.checkStepCompleted(stgModelList, procInstanceModel,
      																										  UobConstants.STAGE_INGEST_T11_BDM_EXEC);
      if (forceRerun || !t11BdmCompleted || cleanup) {
      	// drop partition first
      	if (!UobConstants.HISTORY_LOAD_CD.equals(procModel.getLoadProcess().getTgtAplyTypeCd())) {
      		dropHivePartition(procModel);
      	} else {
      		logger.info("Process model " + procModel.getProcId() + " has historical load type, T1.1 partition is not dropped");
      	}
      	
      	dropT1ErrorPartition(procModel);
      	
      	if (skipBDMProcess) {
      		logger.info("Stage: Tier 1.1 BDM Process is skipped due to empty source file");
      	} else {
      		runTierT11Process(procInstanceModel, procModel, paramFilePath);
      	}	
      } else {
        logger.info("Stage: Tier 1.1 BDM Process already completed; Skipping stage");
      }

      procFinalCompleted = stgHndle.checkStepCompleted(stgModelList, procInstanceModel, 
      		                                             UobConstants.STAGE_INGEST_PROC_FINAL);
    }

    if (forceRerun || !procFinalCompleted || cleanup) {
      // Process Finalization
      runProcessFinalization(procInstanceModel, procModel, skipBDMProcess);
      runTableCompaction(procInstanceModel, procModel);
    } else {
      logger.info("Stage: Process Finalization already completed; Skipping stage");
    }

    logger.info("File Ingestion completed: " + procModel.getProcId());
  }


	// PEDDAGML - 1285
	private boolean checkIfAllRecordsAreInErrorTable(ProcessInstanceModel procInsModel, ProcessModel procModel) throws EDAGException {


	boolean allRecordsInErrTbl = Boolean.FALSE;
	  int errorRowCount = -1; // set to -1 to distinguish from actual 0 and failures

	ControlModel controlModel = procInsModel.getControlModel();
	long stagingCount = controlModel.getTotalRecordsStaging();

	HadoopModel destModel = procModel.getDestInfo();
	HiveDao dao = new HiveDao();

	  try {
		  errorRowCount = dao.getErrorRowCount(destModel.getStagingDbName(),
								destModel.getStagingErrorTableName(), procInsModel.getBizDate(),
								  destModel.getHadoopQueueName());
		  // Also set the control model appropriately.
		  controlModel.setTargetErrorTableName(destModel.getStagingDbName() + "." + destModel.getStagingErrorTableName());
		  controlModel.setTotalErrRecordsTarget(errorRowCount);
	  } catch (EDAGMyBatisException e) {
		  throw new EDAGException(String.format("Unable to fetch Staging Error Table Record Count for table %s for biz date %s",
				  destModel.getStagingErrorTableName(), procInsModel.getBizDate()), e);
	  }


	if(stagingCount == 0 && errorRowCount > 0) allRecordsInErrTbl = Boolean.TRUE;

	logger.info("checkIfAllRecordsAreInErrorTable, \n info={ "
			+ "allRecordsInErrTbl = " + allRecordsInErrTbl
			+ ", bizDate = " + procInsModel.getBizDate()
			+ ", staginDbNm = " + destModel.getStagingDbName()
			+ ", stagingTableNm = " + controlModel.getTargetTableName()
			+ ", stagingCount = " + controlModel.getTotalRecordsStaging()
			+ ", stagingErrTableNm = " + destModel.getStagingErrorTableName()
			+ ", stagingErrCount = " + controlModel.getTotalErrRecordsTarget()
			+" }");

	return allRecordsInErrTbl;

  }

  /**
   * This method should only be called after header and footer lines are removed.
   * @param sourceFile
   * @return
   * @throws EDAGIOException
   */
	protected boolean containsDataRow(ControlModel controlModel, File sourceFile) throws EDAGIOException {
		if (!sourceFile.isFile()) {
			throw new EDAGIOException(EDAGIOException.FILE_NOT_FOUND, sourceFile.getPath(), "File does not exist or is a directory");
		}
		
		if (controlModel != null) {
			logger.info("Total source record count is " + controlModel.getTotalRecords());
		}
		
		long fileSize = org.apache.commons.io.FileUtils.sizeOf(sourceFile);
		logger.debug("Size of file " + sourceFile.getPath() + " is " + fileSize);
		
		boolean empty = (controlModel != null && controlModel.getTotalRecords() == 0 && fileSize == 0) ||
				            (controlModel == null && fileSize == 0);
		
		return !empty;
	}

	
	protected void setNoOfRecords(int noOfRecords) {
		this.noOfRecords = noOfRecords;
	}
	
	private boolean checkIfReconciliationRequired(ProcessInstanceModel procInsModel, ProcessModel procModel) {
		return !(procInsModel.isDumped() || procModel.getSrcSysCd().equalsIgnoreCase(UobConstants.ADOBE_SITE_CATALYST_SRC_SYS_CD));
	}
	
    private boolean filesNeedsToBeArchived(ProcessModel procModel) {
	  long count = procModel.getProcParam().stream().filter(f -> f.getParamName().equalsIgnoreCase(UobConstants.WEBLOGS_HDFS_FOLDER_PATH) 
				  || f.getParamName().equalsIgnoreCase(UobConstants.WEBLOGS_TOPIC_NAME)
				  || f.getParamName().equalsIgnoreCase(UobConstants.WEBLOGS_EDAG_LOAD_TABLE_FREQUENCY)).count();
	  return !(UobConstants.ADOBE_SITE_CATALYST_SRC_SYS_CD.equalsIgnoreCase(procModel.getSrcSysCd()) || count > 0);
	}



    /**
     * Do a clean copy.
     * @param sourceFileLayoutCd File Layout code.
     * @return
     */
	private boolean doDirectCopy(String sourceFileLayoutCd ){

	    return UobConstants.FileLayoutIdentifier
                .getFileLayoutType(sourceFileLayoutCd)
                .equals(UobConstants.FileLayoutIdentifier.CSV);
    }
	
	private void runTableCompaction(ProcessInstanceModel procInstanceModel, ProcessModel processModel) throws EDAGException{
		boolean compactionEnabled = Boolean.FALSE;
		for (ProcessParam procParam : processModel.getProcParam()) {
			if(UobConstants.COMPACTION_ENABLED.equalsIgnoreCase(procParam.getParamName())) {
				compactionEnabled = Boolean.parseBoolean(procParam.getParamValue());
				break;
			}
		}
		
		if(compactionEnabled) {
			logger.info(String.format("Performing Compaction for process id %s", processModel.getProcId()));
			CompactionUtil compactionUtil = new CompactionUtil();
			compactionUtil.performCompaction(procInstanceModel, processModel);
		}
	}
	
	
}
