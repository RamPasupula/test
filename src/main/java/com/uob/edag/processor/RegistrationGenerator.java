package com.uob.edag.processor;

import java.io.File;
import java.io.IOException;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.io.FilenameUtils;
import org.apache.commons.lang3.ArrayUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.log4j.Logger;

import com.uob.edag.constants.UobConstants;
import com.uob.edag.dao.CountryMetaDao;
import com.uob.edag.dao.HiveGenerationDao;
import com.uob.edag.dao.RegistrationDao;
import com.uob.edag.exception.EDAGException;
import com.uob.edag.exception.EDAGIOException;
import com.uob.edag.exception.EDAGMyBatisException;
import com.uob.edag.exception.EDAGProcessorException;
import com.uob.edag.exception.EDAGValidationException;
import com.uob.edag.model.AdobeProcessMapModel;
import com.uob.edag.model.FieldModel;
import com.uob.edag.model.FieldModel.RecordType;
import com.uob.edag.model.FileModel;
import com.uob.edag.model.HadoopModel;
import com.uob.edag.model.InterfaceSpec;
import com.uob.edag.model.ProcessModel;
import com.uob.edag.utils.CountryMetaUtil;
import com.uob.edag.utils.FileUtils;
import com.uob.edag.utils.InterfaceSpecMap;
import com.uob.edag.utils.PropertyLoader;
import com.uob.edag.utils.UobInterfaceFileParser;
import com.uob.edag.utils.UobUtils;
import com.uob.edag.validation.InterfaceSpecHandler;

/**
 * @Author : Daya Venkatesan
 * @Date of Creation: 10/24/2016
 * @Description : The class is used for processing the uploaded interface spec
 *              and generating the metadata for insertion later into the
 *              Metadata Database.
 * 
 */

public class RegistrationGenerator {

  private static Logger logger = null;
  private static Options options = new Options();
  private Map<String, List<String>> sqlList = new HashMap<String, List<String>>();
  private static String sqlFileName = null;

  private static boolean isPIIRegEnabled = false;
  private static String PIIColumnNumber; // set default value

  /**
   * This method will upload the data into the metadata database.
   * @param arguments Acceptable arguments are -h,-f and -r. Use Help to see more usage info.
   * @throws EDAGProcessorException 
   * @throws EDAGValidationException 
   * @throws EDAGIOException 
   * @throws Exception when there is any error in the registration process.
   */
  public static void main(String[] arguments) throws EDAGProcessorException, EDAGValidationException, EDAGIOException {
    boolean force = false;
    String filesToCleanup = null;
    String filesToProcess = null;

    CommandLineParser parser = new DefaultParser();
    options.addOption("h", "help", false, "Show Help");

    Option forceOpt = new Option("f", "force", false, 
    		                         "Rerun Registration Process for specific files within Interface Spec with or without force cleanup. process_id arguments are optional");
    forceOpt.setArgs(2);
    forceOpt.setOptionalArg(true);
    forceOpt.setArgName("cleanup_process_id> <run_only_process_id");
    options.addOption(forceOpt);

    Option regOpt = new Option("r", "register", false, "Run Registration Process for given Interface Spec");
    regOpt.setArgs(2);
    regOpt.setArgName("Interface Spec Path> <Process Spec Path");
    options.addOption(regOpt);

    // PII enabled registration, EDF 224
    Option PIIOpt = new Option("p", "pii", false, "Run Registration Process for given Interface Spec with PII");
    PIIOpt.setArgs(1);
    PIIOpt.setArgName("pii_column_number");
    options.addOption(PIIOpt);

    CommandLine command;
		try {
			command = parser.parse(options, arguments);
		} catch (ParseException e) {
			throw new EDAGProcessorException(EDAGProcessorException.CANNOT_PARSE_CLI_OPTIONS, e.getMessage());
		}

    if (command.hasOption("h")) {
      showHelp();
    }
    
    if (!command.hasOption("r")) {
      throw new EDAGProcessorException(EDAGProcessorException.MISSING_MANDATORY_OPTION, "r", "Register Option is mandatory");
    }

    String[] registerArgs = command.getOptionValues("r");
    if (registerArgs == null || registerArgs.length != 2) {
      throw new EDAGProcessorException(EDAGProcessorException.INCORRECT_ARGS_FOR_OPTION, "r", "Not enough arguments passed to run registration");
    }
    
    String interfaceSpecPath = registerArgs[0];
    String processSpecPath = registerArgs[1];
    if (StringUtils.isBlank(interfaceSpecPath)) {
      throw new EDAGValidationException(EDAGValidationException.EMPTY_VALUE, "Interface Spec Path", "Interface Spec Path cannot be empty");
    }
    
    if (StringUtils.isBlank(processSpecPath)) {
      throw new EDAGValidationException(EDAGValidationException.EMPTY_VALUE, "Process Spec Path", "Process Spec Path cannot be empty");
    }

    // PII specific registration, EDF 224
    if(command.hasOption("p")){
      String[] PIIArgs = command.getOptionValues("p");
      if (PIIArgs == null || PIIArgs.length != 1) {
        throw new EDAGProcessorException(EDAGProcessorException.INCORRECT_ARGS_FOR_OPTION, "p",
                "Not enough arguments passed to run registration with PII enabled");
      }
      else {
          isPIIRegEnabled = true;
          PIIColumnNumber = PIIArgs[0];
      }

    }

    File intSpec = new File(interfaceSpecPath);
    String fileName = intSpec.getName();
    String execDate = new SimpleDateFormat("yyyyMMddHHmmss").format(System.currentTimeMillis());
    String logFileName = "EDA_DL_" + fileName + "_" + execDate + ".log";
    System.setProperty("logFileName", logFileName);
    logger = Logger.getLogger(RegistrationGenerator.class);
    UobUtils.logJavaProperties();
    UobUtils.logPackageProperties();
    
    for (Option option : command.getOptions()) {
    	logger.info("Option " + option.getOpt() + " = " + option.getValue());
    }
    
    sqlFileName = PropertyLoader.getProperty(UobConstants.SQL_FILE_NM);

    if (command.hasOption("f")) {
      force = true;
      logger.info("Force Rerun flag has been set.");

      String[] forceArgs = command.getOptionValues("f");
      logger.debug("Arguments for Force: " + Arrays.toString(forceArgs));
      if (ArrayUtils.isNotEmpty(forceArgs)) {
        if (forceArgs.length == 2) {
          filesToCleanup = forceArgs[0];
          filesToProcess = forceArgs[1];
        } else if (forceArgs.length == 1) {
          filesToCleanup = forceArgs[0];
        }

        if ("*".equalsIgnoreCase(filesToCleanup)) {
          if (StringUtils.isNotBlank(filesToProcess) && !"*".equalsIgnoreCase(filesToProcess.trim())) {
            throw new EDAGProcessorException("Request to cleanup all the files, but load back only for certain files. This can result in data loss.");
          } else {
            filesToProcess = "*";
            logger.info("Going to cleanup all files and process all files again");
          }
        } else if (StringUtils.isBlank(filesToCleanup)) {
          if (StringUtils.isBlank(filesToProcess)) {
            logger.info("Going to Cleanup all files and process all files again");
          } else if ("*".equalsIgnoreCase(filesToProcess.trim())) {
            logger.info("No cleanup required; Going to process all the files");
          } else {
            logger.info("No cleanup required; Going to process only selected files");
          }
        } else if (!"*".equalsIgnoreCase(filesToCleanup)) {
          if (StringUtils.isNotBlank(filesToProcess)) {
            logger.info("Going to cleanup the selected files and process the selected files again");
            List<String> filesToCleanupList = Arrays.asList(filesToCleanup.split(UobConstants.COMMA));
            List<String> filesToProcessList = Arrays.asList(filesToProcess.split(UobConstants.COMMA));
            boolean allCleanupProcessed = filesToProcessList.containsAll(filesToCleanupList);
            if (!filesToCleanupList.isEmpty() && ! allCleanupProcessed) {
              throw new EDAGProcessorException("Request to cleanup some files, but not load the same files again. This can result in data loss.");
            }
          } else {
            filesToProcess = "*";
            logger.info("Going to do cleanup of the given files; Going to process all files");
          }
        }
      } else {
        logger.info("Going to Cleanup all files and process all files again");
        filesToCleanup = "*";
        filesToProcess = "*";
      }
    }
    
    RegistrationGenerator regis = new RegistrationGenerator();
    regis.processInterfaceSpec(interfaceSpecPath, processSpecPath, force, filesToCleanup, filesToProcess);
  }

  /**
   * This method displays the usage help for this Java class.
   */
  private static void showHelp() {
    HelpFormatter formatter = new HelpFormatter();
    formatter.printHelp("RegistrationProcessor", options);
    System.exit(0);

  }

  /**
   * This method is used to process an interface and process specification.
   * @param filePath The Path of the Interface Specification in the file system.
   * @param procSpecPath The Path of the Process Specification in the file system.
   * @param force Whether or not to force overwrite existing registration.
   * @param filesToForce List of files which needs to be force overwritten and registered.
   * @param filesToProcess List of files which needs to be processed from the Interface Spec.
   * @throws EDAGProcessorException 
   * @throws EDAGValidationException 
   * @throws EDAGIOException 
   * @throws Exception when there is any error in the Registration Process.
   */
  private void processInterfaceSpec(String filePath, String procSpecPath, boolean force, String filesToForce,
  		                              String filesToProcess) throws EDAGProcessorException, EDAGValidationException, EDAGIOException {
    logger.info("Going to start registering interface spec: " + filePath);
    Map<String, EDAGException> errorProcesses = new HashMap<String, EDAGException>();
    int procSize = 0;
    try {
      // Parse the Interface Spec file
      UobInterfaceFileParser parser = new UobInterfaceFileParser();
      InterfaceSpec spec = parser.parseInterfaceSpecFile(filePath, procSpecPath);
      if (spec == null) {
        throw new EDAGValidationException(EDAGValidationException.NULL_VALUE, "Interface Spec", "There is no data to register in the interface spec");
      }
      
      Map<String, InterfaceSpecMap> srcFileSpecList = spec.getSrcFileSpec();

      AdobeProcessMapModel adm = null;
      if(UobConstants.ADOBE_SITE_CATALYST_SRC_SYS_CD.equalsIgnoreCase(spec.getSrcSystem())) {
      	adm = new AdobeProcessMapModel();
      }
       /*
		 * Added By -Kerwin-
		 * This will support COUNTRY APPLICABILITY
		 * */
	 	CountryMetaUtil cMetaUtil = new CountryMetaUtil();
		CountryMetaDao  metaDao   = new CountryMetaDao();
		
      // Validate the Spec file and Convert into ProcessModel
      InterfaceSpecHandler handler = new InterfaceSpecHandler();
      procSize = srcFileSpecList.entrySet().size();
      for (Entry<String, InterfaceSpecMap> fileMap : srcFileSpecList.entrySet()) {
        String fileName = null;
        try {
          fileName = fileMap.getKey();
          InterfaceSpecMap fileSpec = fileMap.getValue();
          ProcessModel procModel = handler.handleInterfaceSpecDetails(spec, force, filesToForce, filesToProcess, 
          		                                                        fileName, fileSpec);
          if (procModel == null) {
            continue;
          }
          
          /* * Added By -Kerwin-
			 * This will support COUNTRY APPLICABILITY
			 * */
			// KG [START] ****************************************** **
			boolean deltaProcess = false;
			List<HashMap<String,String>> metaCountries = new ArrayList<>();
			String newCountryCodes = null;
			try {
				HashMap<String,String> paramMap = new HashMap<>();
				paramMap.put("procId", procModel.getProcId());
				metaCountries = metaDao.getCountryMetaList(paramMap);
	 		} catch (Exception e) {
	 			logger.error("ERROR Getting Country Meta Data: ", e);
			}
	 		newCountryCodes = cMetaUtil.getNewCountryCodes(metaCountries, fileSpec);
	 		if (StringUtils.isNotBlank(newCountryCodes)) {
				deltaProcess = true;
			} 
			// KG [END] **********************************************
          // Set Process Sub Process Information for Adobe Site Catalyst
          
          if(UobConstants.ADOBE_SITE_CATALYST_SRC_SYS_CD.equalsIgnoreCase(spec.getSrcSystem())) {
          	adm.setProcId(procModel.getProcId());
          }
          
          // Force Cleanup
          if (force) {
            logger.debug("Force is true, Cleanup files: " + filesToForce);
            if (deltaProcess) {
				cleanupDelta(procModel, filesToForce, true, true, metaCountries, newCountryCodes);
	 		} else {
				cleanup(procModel, filesToForce, true, true);
	 		}
          }
  
          String[] filesToProcessArr = null;
          if (StringUtils.isNotEmpty(filesToProcess) && !"*".equalsIgnoreCase(filesToProcess)) {
            logger.info("Going to process only for files: " + filesToProcess);
            filesToProcessArr = filesToProcess.split(UobConstants.COMMA);
          }
          
          if (ArrayUtils.isNotEmpty(filesToProcessArr)) {
            boolean process = false;
            for (String fileProc : filesToProcessArr) {
              if (StringUtils.trimToEmpty(fileProc).equalsIgnoreCase(procModel.getProcNm().trim())) {
            	    if (deltaProcess) {
            		  processDelta(procModel, metaCountries, newCountryCodes);							 		
					} else {
				 	  process(procModel);
					}
                process = true;
              }
            }
            
            if (!process) {
              logger.info("Did not process the file: " + procModel.getProcNm());
            }
          } else {
        	  if (deltaProcess) {
        	    processDelta(procModel, metaCountries, newCountryCodes);							 		
			  } else {
		 		process(procModel);
			  }
          }
        } catch (EDAGException excp) {
          errorProcesses.put(fileName , excp);
        }
      }
      
      try {
    	  
    	  if(UobConstants.ADOBE_SITE_CATALYST_SRC_SYS_CD.equalsIgnoreCase(spec.getSrcSystem())) {
    	      // Force Cleanup
              if (force) {
                logger.debug("Force is true, Cleanup files: " + UobConstants.ADOBE_SITE_CATALYST_MASTER_PROCESS);
                cleanupAdobeModel(adm);
              }
        	  //Add SQLs for Adobe Site Catalyst to MI scripts
        	  processAdobeModel(adm);	  
    	  }

        }
        catch (EDAGException excp) {
            errorProcesses.put(UobConstants.ADOBE_SITE_CATALYST_MASTER_PROCESS, excp);
        }



      // Do PII registration here.


      if(isPIIRegEnabled){
        try {
          PIIRegistrationProcessor piireg = new PIIRegistrationProcessor(spec, filePath, procSpecPath, logger);
          try {
            piireg.processPIIFromFileSpec(PIIColumnNumber);
            logger.info("PII Registration Views generated.");
          }
          catch(EDAGException e){
            piireg.rollback(); // try to roll back
            errorProcesses.put("Error registering with PII enabled", e);
          }

        } catch (EDAGException e) {
          errorProcesses.put("Failed to initialize PIIRegistrationProcessor", e);
        }
      }
      // Create SQL File
      createSqlFile();
      logger.info("Bulk Registration completed for interface spec:" + filePath);

    } finally {
      if (!errorProcesses.isEmpty()) {
        for (Entry<String, EDAGException> errorProc : errorProcesses.entrySet()) {
          EDAGException excp = errorProc.getValue();
          logger.error("Process: " + errorProc.getKey() + " failed with error: " + excp.getMessage()); 
        }

        String message = procSize == errorProcesses.size() ? "All Processes failed to register" 
        		                                               : "Some Processes failed to Register";
        throw new EDAGProcessorException(message);
      }
    }
  }

  /**
   * This method is used to create the SQL file with the inserts for the metadata tables and DDLs 
   * for the Hive table creation.
   * @throws IOException when there is an error in writing the SQL file.
   * @throws EDAGIOException when there is custom thrown exception.
   */
  private void createSqlFile() throws EDAGIOException {
    String sqlFileLoc = PropertyLoader.getProperty(UobConstants.SQL_FILE_LOC);
    String sqlFileArchiveLoc = PropertyLoader.getProperty(UobConstants.SQL_FILE_ARCHIVE_LOCATION);
    // Create SQL file - Loop for Process
    for (Entry<String, List<String>> entrySet : sqlList.entrySet()) {
      String sqlFileNm = entrySet.getKey();
      String fullFileName = sqlFileLoc + File.separator + sqlFileNm;
      List<String> sqlList = entrySet.getValue();
      File sqlFile = new File(fullFileName);

      if (sqlFile.exists()) {
        // Archive the sql file
        String sqlArchiveFileName = null;
        if (StringUtils.isNotEmpty(sqlFileNm)) {
          String basename = FilenameUtils.getBaseName(sqlFileNm);
          String extension = FilenameUtils.getExtension(sqlFileNm);
          DateFormat sdf = new SimpleDateFormat("yyyyMMddHHmmss");
          sqlArchiveFileName = sqlFileArchiveLoc + File.separator + basename + UobConstants.UNDERSCORE + 
          		                 sdf.format(System.currentTimeMillis()) + "." + extension;
          boolean renamed = sqlFile.renameTo(new File(sqlArchiveFileName));
          if (!renamed) {
            throw new EDAGIOException(EDAGIOException.CANNOT_MOVE_FILE, sqlFile.getPath(), sqlArchiveFileName, "SQL file cannot be archived");
          }
          
          logger.info("Existing SQL file " + sqlFileNm + " archived successfully to " + sqlArchiveFileName);
        }
      }
      FileUtils fileUtil = new FileUtils();
      fileUtil.writeToFile(sqlList, sqlFile);
      logger.info("New SQL file created successfully:" + fullFileName);
    }
  }
  
  /*
   * ADDED By -Kerwin-
   * This will support COUNTRY APPLICABILITY
   * */
  private void processDelta(ProcessModel procModel, List<HashMap<String,String>> countryList
			, String countryCodes) throws EDAGMyBatisException {
		logger.info("Going to start registering process: " + procModel.getProcNm());

		HiveGenerationDao hiveDao = new HiveGenerationDao();

		List<String> metadataInsertList = new ArrayList<String>();
		List<String> hiveCreateList = new ArrayList<String>();
		String procName = procModel.getProcNm();
		logger.debug("ProcModel is: " + procModel);
		
		CountryMetaUtil metaUtil = new CountryMetaUtil();
		 
		metadataInsertList.add("set define off;");
		metadataInsertList.add("set sqlblanklines on;");
		metadataInsertList.add("whenever SQLERROR EXIT ROLLBACK;");

		// Insert into Process Master
		RegistrationDao regDao = new RegistrationDao();
		metadataInsertList.add(procModel.getInsertProcessMasterSql());

		// Insert into Process Country
		List<String> ctrySqlList = procModel.getInsertProcessCountrySql();
		if (CollectionUtils.isNotEmpty(ctrySqlList)) {
			metadataInsertList.addAll(ctrySqlList);
		}

		// Insert country control character
		List<String> ctryControlCharList = procModel.getInsertCountryControlCharacter();
		if (CollectionUtils.isNotEmpty(ctryControlCharList)) {
			metadataInsertList.addAll(ctryControlCharList);
		}

		// Insert into Load Process
		HadoopModel destModel = procModel.getDestInfo();
		destModel.setProcessId(procModel.getProcId());
		metadataInsertList.add(destModel.getInsertLoadProcessSql());

		// Insert into Control File Detail
		FileModel srcFileModel = procModel.getSrcInfo();

		if (UobConstants.CTRL_INFO_C.equalsIgnoreCase(srcFileModel.getControlInfo())) {
			int ctrlFileId = regDao.selectControlFileId();
			srcFileModel.setControlFileId(Integer.toString(ctrlFileId));
			metadataInsertList.add(srcFileModel.getInsertControlFileDetailSql());
		}

		// Insert into File Detail
		int fileId = regDao.selectFileId();
		srcFileModel.setFileId(fileId);
		srcFileModel.setProcessId(procModel.getProcId());
		metadataInsertList.add(srcFileModel.getInsertFileDetailSql());

		// Insert into Field Details - Data Records
		List<FieldModel> fieldModelList = srcFileModel.getSrcFieldInfo();
		Collections.sort(fieldModelList);
		for (FieldModel model : fieldModelList) {
			model.setFileId(srcFileModel.getFileId());
		}

		for (FieldModel model : fieldModelList) {
			metadataInsertList.add(model.getInsertFieldDetailSql());
		}

		// Insert into Field Standardization Rules
		for (FieldModel model : fieldModelList) {
			metadataInsertList.addAll(model.getInsertFieldStdRulesSql());
		}

		// Insert into Field Name Patterns
		for (FieldModel model : fieldModelList) {
			String insertedSql = model.getInsertFieldNamePatternsSql(procModel.getSrcSystemId(), procModel.getProcId());
			if (!StringUtils.isEmpty(insertedSql)) {
				metadataInsertList.add(insertedSql);
			}
		}

		// Insert into Load Params
		metadataInsertList.addAll(procModel.getInsertProcParamSql());

		// Insert into Downstream Application
		if (StringUtils.isNotBlank(procModel.getDownstreamAppl())) {
			metadataInsertList.add(procModel.getInsertProcDownstreamApplSql());
		}

		// Insert into Alert Detail
		if (procModel.getAlertInfo() != null) {
			metadataInsertList.add(procModel.getAlertInfo().getInsertAlertSql());
		}

		// Insert into Field Details - Control Records
		List<FieldModel> controlModel = srcFileModel.getCtrlInfo();
		if (CollectionUtils.isNotEmpty(controlModel)) {
			for (FieldModel model : controlModel) {
				model.setFileId(srcFileModel.getFileId());
				model.setRecordType(RecordType.CONTROL_INFO);

				metadataInsertList.add(model.getInsertFieldDetailSql());
			}
		}

		String insertSqlKey = null;
		String miDeltaParam = new StringBuilder(UobConstants.METADATA_INSERT).append("_")
						.append(countryCodes).append(CountryMetaUtil.DELTA_SUFFIX).toString();
		
		if (StringUtils.isNotEmpty(sqlFileName)) {
			insertSqlKey = sqlFileName.replace(UobConstants.SRC_SYS_PARAM, procModel.getSrcSysCd())
					.replace(UobConstants.FILENM_PARAM, procName)	
					.replace(UobConstants.QUERY_TYPE_PARAM, miDeltaParam);
		}
		 
		String[] includeTables = new String[] { "EDAG_PROC_CTRY_CTRL_CHAR", "EDAG_PROC_CTRY_DTL" };
		List<String> newMetaList = metaUtil.buildNewMIScript(metadataInsertList, countryList, includeTables);
		if (!newMetaList.isEmpty()) {
			metadataInsertList = new ArrayList<>(newMetaList);
		}
		
		if (sqlList.containsKey(insertSqlKey)) {
			List<String> existingSqlList = sqlList.get(procName);
			existingSqlList.addAll(metadataInsertList);

			sqlList.put(insertSqlKey, existingSqlList);
		} else {
			sqlList.put(insertSqlKey, metadataInsertList);
		}

		// Create Hive Schema and Tables
		for (String country : procModel.getCountryCodes()) {
			String stagingDbName = destModel.getStagingDbName();
			String newStagingDbName = stagingDbName.replace(UobConstants.COUNTRY_PARAM, country);
			hiveCreateList.add(hiveDao.createDatabase(newStagingDbName));
			hiveCreateList.add(hiveDao.createStagingHiveTable(procModel, country));
			hiveCreateList.add(hiveDao.createErrorHiveTable(procModel, country));
		}

		hiveCreateList.add(hiveDao.createDatabase(destModel.getHiveDbName()));
		hiveCreateList.add(hiveDao.createHiveTable(procModel));

		String action = "Registration of Process " + procModel.getProcNm() + " with ID " + procModel.getProcId();
		 
		metadataInsertList.add(procModel.getInsertAuditDetailSql(action, UobConstants.SUCCESS));
		metadataInsertList.add("COMMIT;");
		 
	 	List<String> newHCList = metaUtil.buildNewHiveScript(hiveCreateList, countryList);
		if (!newHCList.isEmpty()) {
			hiveCreateList = new ArrayList<>(newHCList);
		}
		
		String hiveCreateSqlKey = null;
		String hiveDeltaParam = new StringBuilder(UobConstants.HIVE_CREATE).append("_")
				.append(countryCodes).append(CountryMetaUtil.DELTA_SUFFIX).toString();
		if (StringUtils.isNotEmpty(sqlFileName)) {
			hiveCreateSqlKey = sqlFileName.replace(UobConstants.SRC_SYS_PARAM, procModel.getSrcSysCd())
					.replace(UobConstants.FILENM_PARAM, procName)
					.replace(UobConstants.QUERY_TYPE_PARAM, hiveDeltaParam);
		}

		if (sqlList.containsKey(hiveCreateSqlKey)) {
			List<String> existingSqlList = sqlList.get(procName);
			existingSqlList.addAll(hiveCreateList);

			sqlList.put(hiveCreateSqlKey, existingSqlList);
		} else {
			sqlList.put(hiveCreateSqlKey, hiveCreateList);
		}
		 
		logger.info("Process Registration completed: " + procModel.getProcNm());
	}
  
  /*
   * ADDED By -Kerwin-
   * This will support COUNTRY APPLICABILITY
   * */
  private void cleanupDelta(ProcessModel procModel, String filesToCleanup, boolean cleanupMetadata, boolean cleanupHive
			, List<HashMap<String,String>> countryList, String countryCodes)
		throws EDAGMyBatisException {
	boolean cleanup = false;
	if ("*".equalsIgnoreCase(filesToCleanup)) {
		cleanup = true;
	} else if (StringUtils.isNotBlank(filesToCleanup)) {
		String[] forceFilesArr = filesToCleanup.split(UobConstants.COMMA);
		List<String> forceFilesList = Arrays.asList(forceFilesArr);
		if (forceFilesList.contains(procModel.getProcNm())) {
			cleanup = true;
		}
	}

	logger.debug("Going to cleanup for file " + procModel.getProcNm());

	String procName = procModel.getProcNm();

	if (cleanup) {
		if (cleanupMetadata) {
			String procId = procName;

			if (StringUtils.isNotEmpty(procId)) {
				// Drop Metadata Entries
				List<String> delSqlList = new ArrayList<String>();

				delSqlList.add("set define off;");
				delSqlList.add("whenever SQLERROR EXIT ROLLBACK;");

				delSqlList.add(procModel.getRemoveAlertsSql());

				delSqlList.add(procModel.getRemoveProcParamSql());

				delSqlList.add(procModel.getRemoveProcDownstreamApplSql());

				delSqlList.add(procModel.getRemoveFieldStdRulesSql());

				delSqlList.add(procModel.getDeleteFieldNamePatternsSql());

				delSqlList.add(procModel.getRemoveFieldDetailSql());

				delSqlList.add(procModel.getRemoveControlFileDetailSql());

				delSqlList.add(procModel.getRemoveFileDetailSql());

				delSqlList.add(procModel.getRemoveLoadProcessSql());

				delSqlList.add(procModel.getRemoveCountryControlCharSql());

				delSqlList.add(procModel.getRemoveProcessCountrySql());

				delSqlList.add(procModel.getRemoveProcessMasterSql());

				String delSqlKey = null;
				if (StringUtils.isNotEmpty(sqlFileName)) {
					delSqlKey = sqlFileName.replace(UobConstants.SRC_SYS_PARAM, procModel.getSrcSysCd())
							.replace(UobConstants.FILENM_PARAM, procName)
							.replace(UobConstants.QUERY_TYPE_PARAM, UobConstants.METADATA_DELETE);
				}

				delSqlList.add("COMMIT; ");
					
				if (sqlList.containsKey(delSqlKey)) {
					List<String> existingSqlList = sqlList.get(procName);
					existingSqlList.addAll(delSqlList);

					sqlList.put(delSqlKey, existingSqlList);
				} else {
					sqlList.put(delSqlKey, delSqlList);
				}
			}
		}

		if (cleanupHive) {
			// Rename Hive Tables
			HadoopModel destModel = procModel.getDestInfo();
			List<String> hiveRenameList = new ArrayList<String>();
			HiveGenerationDao hiveGenDao = new HiveGenerationDao();
			String hiveDbName = destModel.getHiveDbName();
			String hiveTableName = destModel.getHiveTableName();
			String stagingHiveDbName = destModel.getStagingDbName();
			String stagingHiveTableName = destModel.getStagingTableName();
			String stagingErrorHiveTableName = destModel.getStagingTableName() + UobConstants.UNDERSCORE + "err";

			for (String country : procModel.getCountryCodes()) {
				String newStagingDbName = stagingHiveDbName.replace(UobConstants.COUNTRY_PARAM, country);
				String newStagingTableName = stagingHiveTableName
						+ new SimpleDateFormat("yyyyMMddHHmmss").format(System.currentTimeMillis());
				String sql = hiveGenDao.renameHiveTable(newStagingDbName, stagingHiveTableName,
						newStagingTableName);
				hiveRenameList.add(sql);

				String newStagingErrorTableName = stagingErrorHiveTableName
						+ new SimpleDateFormat("yyyyMMddHHmmss").format(System.currentTimeMillis());
				String errorSql = hiveGenDao.renameHiveTable(newStagingDbName, stagingErrorHiveTableName,
						newStagingErrorTableName);
				hiveRenameList.add(errorSql);
			}

			// EDF-209. If dump table name is not empty, it means it's a history file. Don't
			// rename T1.1 table since the table is shared with normal daily file.
			if (!UobConstants.HISTORY_CD.equalsIgnoreCase(procModel.getProcFreqCd())) {
				String newHiveTableName = hiveTableName
						+ new SimpleDateFormat("yyyyMMddHHmmss").format(System.currentTimeMillis());
				String sql = hiveGenDao.renameHiveTable(hiveDbName, hiveTableName, newHiveTableName);
				hiveRenameList.add(sql);
			}

			String renameSqlKey = null;
			String hiveDeltaParam = new StringBuilder(UobConstants.HIVE_RENAME).append("_")
					.append(countryCodes).append(CountryMetaUtil.DELTA_SUFFIX).toString();
			if (StringUtils.isNotEmpty(sqlFileName)) {
				renameSqlKey = sqlFileName.replace(UobConstants.SRC_SYS_PARAM, procModel.getSrcSysCd())
						.replace(UobConstants.FILENM_PARAM, procName)
						.replace(UobConstants.QUERY_TYPE_PARAM, hiveDeltaParam);
			}
			CountryMetaUtil metaUtil = new CountryMetaUtil();	
			List<String> newHRList = metaUtil.buildNewHiveScript(hiveRenameList, countryList);
			if (!newHRList.isEmpty()) {
				hiveRenameList = new ArrayList<>(newHRList);
			}
			
			if (sqlList.containsKey(renameSqlKey)) {
				List<String> existingSqlList = sqlList.get(procName);
				existingSqlList.addAll(hiveRenameList);

				sqlList.put(renameSqlKey, existingSqlList);
			} else {
				sqlList.put(renameSqlKey, hiveRenameList);
			}
		}
	}
}


  /**
   * This method is used to remove the existing metadata entries if any for the process being
   * registered. This method will also rename the existing Hive tables with the current timestamp.
   * @param procModel Process to be cleaned up
   * @param filesToCleanup List of Files for which cleanup needs to be run
   * @param cleanupMetadata Whether or not to clean up the metadata
   * @param cleanupHive Whether or not to cleanup the Hive tables
   * @throws EDAGMyBatisException 
   * @throws Exception when there is any error in the cleanup process
   */
  private void cleanup(ProcessModel procModel, String filesToCleanup, boolean cleanupMetadata, 
  		                 boolean cleanupHive) throws EDAGMyBatisException {
    boolean cleanup = false;
    if ("*".equalsIgnoreCase(filesToCleanup)) {
      cleanup = true;
    } else if (StringUtils.isNotBlank(filesToCleanup)) {
      String[] forceFilesArr = filesToCleanup.split(UobConstants.COMMA);
      List<String> forceFilesList = Arrays.asList(forceFilesArr);
      if (forceFilesList.contains(procModel.getProcNm())) {
        cleanup = true;
      }
    }

    logger.debug("Going to cleanup for file " + procModel.getProcNm());

    String procName = procModel.getProcNm();

    if (cleanup) {
      if (cleanupMetadata) {
        String procId = procName;

        if (StringUtils.isNotEmpty(procId)) {
          // Drop Metadata Entries
          List<String> delSqlList = new ArrayList<String>();

          delSqlList.add("set define off;");
          delSqlList.add("whenever SQLERROR EXIT ROLLBACK;");
          
          delSqlList.add(procModel.getRemoveAlertsSql());
          
          delSqlList.add(procModel.getRemoveProcParamSql());
          
          delSqlList.add(procModel.getRemoveProcDownstreamApplSql());

          delSqlList.add(procModel.getRemoveFieldStdRulesSql());

          delSqlList.add(procModel.getDeleteFieldNamePatternsSql());

          delSqlList.add(procModel.getRemoveFieldDetailSql());

          delSqlList.add(procModel.getRemoveControlFileDetailSql());
          
          delSqlList.add(procModel.getRemoveFileDetailSql());

          delSqlList.add(procModel.getRemoveLoadProcessSql());
          
          delSqlList.add(procModel.getRemoveCountryControlCharSql());

          delSqlList.add(procModel.getRemoveProcessCountrySql());
          
          delSqlList.add(procModel.getRemoveProcessMasterSql());
          
          String delSqlKey = null;
          if (StringUtils.isNotEmpty(sqlFileName)) {
            delSqlKey = sqlFileName.replace(UobConstants.SRC_SYS_PARAM, procModel.getSrcSysCd())
            		                   .replace(UobConstants.FILENM_PARAM, procName)
            		                   .replace(UobConstants.QUERY_TYPE_PARAM, UobConstants.METADATA_DELETE);
          }
          
          delSqlList.add("COMMIT; ");
          
          if (sqlList.containsKey(delSqlKey)) {
            List<String> existingSqlList = sqlList.get(procName);
            existingSqlList.addAll(delSqlList);

            sqlList.put(delSqlKey, existingSqlList);
          } else {
            sqlList.put(delSqlKey, delSqlList);
          }
        }
      }

      if (cleanupHive) {
        // Rename Hive Tables
        HadoopModel destModel = procModel.getDestInfo();
        List<String> hiveRenameList = new ArrayList<String>();
        HiveGenerationDao hiveGenDao = new HiveGenerationDao();
        String hiveDbName = destModel.getHiveDbName();
        String hiveTableName = destModel.getHiveTableName();
        String stagingHiveDbName = destModel.getStagingDbName();
        String stagingHiveTableName = destModel.getStagingTableName();
        String stagingErrorHiveTableName = destModel.getStagingTableName() + UobConstants.UNDERSCORE + "err";

        for (String country : procModel.getCountryCodes()) {
          String newStagingDbName = stagingHiveDbName.replace(UobConstants.COUNTRY_PARAM, country);
          String newStagingTableName = stagingHiveTableName + new SimpleDateFormat("yyyyMMddHHmmss").format(System.currentTimeMillis());
          String sql = hiveGenDao.renameHiveTable(newStagingDbName, stagingHiveTableName, newStagingTableName);
          hiveRenameList.add(sql);

          String newStagingErrorTableName = stagingErrorHiveTableName + new SimpleDateFormat("yyyyMMddHHmmss").format(System.currentTimeMillis());
          String errorSql = hiveGenDao.renameHiveTable(newStagingDbName, stagingErrorHiveTableName, newStagingErrorTableName);
          hiveRenameList.add(errorSql);
        }

        // EDF-209. If dump table name is not empty, it means it's a history file. Don't rename T1.1 table since the table is shared with normal daily file.
        if (!UobConstants.HISTORY_CD.equalsIgnoreCase(procModel.getProcFreqCd())) {
	        String newHiveTableName = hiveTableName + new SimpleDateFormat("yyyyMMddHHmmss").format(System.currentTimeMillis());
	        String sql = hiveGenDao.renameHiveTable(hiveDbName, hiveTableName, newHiveTableName);
	        hiveRenameList.add(sql);
        }

        String renameSqlKey = null;
        if (StringUtils.isNotEmpty(sqlFileName)) {
          renameSqlKey = sqlFileName.replace(UobConstants.SRC_SYS_PARAM, procModel.getSrcSysCd())
          		                      .replace(UobConstants.FILENM_PARAM, procName)
          		                      .replace(UobConstants.QUERY_TYPE_PARAM, UobConstants.HIVE_RENAME);
        }
        
        if (sqlList.containsKey(renameSqlKey)) {
          List<String> existingSqlList = sqlList.get(procName);
          existingSqlList.addAll(hiveRenameList);

          sqlList.put(renameSqlKey, existingSqlList);
        } else {
          sqlList.put(renameSqlKey, hiveRenameList);
        }
      }
    }
  }

  /**
   * This method is used to do the actual registration for any given process.
   * @param procModel The Process Model object with the metadata information to be registered.
   * @throws EDAGMyBatisException 
   * @throws Exception when there is an error in the Registration Process
   */
  private void process(ProcessModel procModel) throws EDAGMyBatisException {
    logger.info("Going to start registering process: " + procModel.getProcNm());

    HiveGenerationDao hiveDao = new HiveGenerationDao();

    List<String> metadataInsertList = new ArrayList<String>();
    List<String> hiveCreateList = new ArrayList<String>();
    String procName = procModel.getProcNm();
    logger.debug("ProcModel is: " + procModel);
    
    metadataInsertList.add("set define off;");
    metadataInsertList.add("set sqlblanklines on;");
    metadataInsertList.add("whenever SQLERROR EXIT ROLLBACK;");

    // Insert into Process Master
    RegistrationDao regDao = new RegistrationDao();
    metadataInsertList.add(procModel.getInsertProcessMasterSql());

    // Insert into Process Country
    List<String> ctrySqlList = procModel.getInsertProcessCountrySql();
    if (CollectionUtils.isNotEmpty(ctrySqlList)) {
      metadataInsertList.addAll(ctrySqlList);
    }
    
    // Insert country control character
    List<String> ctryControlCharList = procModel.getInsertCountryControlCharacter();
    if (CollectionUtils.isNotEmpty(ctryControlCharList)) {
      metadataInsertList.addAll(ctryControlCharList);
    }
    
    // Insert into Load Process
    HadoopModel destModel = procModel.getDestInfo();
    destModel.setProcessId(procModel.getProcId());
    metadataInsertList.add(destModel.getInsertLoadProcessSql());

    // Insert into Control File Detail
    FileModel srcFileModel = procModel.getSrcInfo();

    if (UobConstants.CTRL_INFO_C.equalsIgnoreCase(srcFileModel.getControlInfo())) {
      int ctrlFileId = regDao.selectControlFileId();
      srcFileModel.setControlFileId(Integer.toString(ctrlFileId));
      metadataInsertList.add(srcFileModel.getInsertControlFileDetailSql());
    }

    // Insert into File Detail
    int fileId = regDao.selectFileId();
    srcFileModel.setFileId(fileId);
    srcFileModel.setProcessId(procModel.getProcId());
    metadataInsertList.add(srcFileModel.getInsertFileDetailSql());

    // Insert into Field Details - Data Records
    List<FieldModel> fieldModelList = srcFileModel.getSrcFieldInfo();
    Collections.sort(fieldModelList);
    for (FieldModel model : fieldModelList) {
      model.setFileId(srcFileModel.getFileId());
    }
    
    for (FieldModel model : fieldModelList) {
    	metadataInsertList.add(model.getInsertFieldDetailSql());
    }
    
    // Insert into Field Standardization Rules
    for (FieldModel model : fieldModelList) {
    	metadataInsertList.addAll(model.getInsertFieldStdRulesSql());
    }
    
    // Insert into Field Name Patterns
    for (FieldModel model : fieldModelList) {
        String insertedSql = model.getInsertFieldNamePatternsSql(procModel.getSrcSystemId(), procModel.getProcId());
        if(!StringUtils.isEmpty(insertedSql)) {
            metadataInsertList.add(insertedSql);
        }
    }


    // Insert into Load Params
    metadataInsertList.addAll(procModel.getInsertProcParamSql());
    
    // Insert into Downstream Application
    if (StringUtils.isNotBlank(procModel.getDownstreamAppl())) {
      metadataInsertList.add(procModel.getInsertProcDownstreamApplSql());
    }

    // Insert into Alert Detail
    if (procModel.getAlertInfo() != null) {
      metadataInsertList.add(procModel.getAlertInfo().getInsertAlertSql());
    }

    // Insert into Field Details - Control Records
    List<FieldModel> controlModel = srcFileModel.getCtrlInfo();
    if (CollectionUtils.isNotEmpty(controlModel)) {
      for (FieldModel model : controlModel) {
        model.setFileId(srcFileModel.getFileId());
        model.setRecordType(RecordType.CONTROL_INFO);
        
        metadataInsertList.add(model.getInsertFieldDetailSql());
      }
    }

    String insertSqlKey = null;
    if (StringUtils.isNotEmpty(sqlFileName)) {
      insertSqlKey = sqlFileName.replace(UobConstants.SRC_SYS_PARAM, procModel.getSrcSysCd())
      													.replace(UobConstants.FILENM_PARAM, procName)
      													.replace(UobConstants.QUERY_TYPE_PARAM, UobConstants.METADATA_INSERT);
    }
    
    if (sqlList.containsKey(insertSqlKey)) {
      List<String> existingSqlList = sqlList.get(procName);
      existingSqlList.addAll(metadataInsertList);

      sqlList.put(insertSqlKey, existingSqlList);
    } else {
      sqlList.put(insertSqlKey, metadataInsertList);
    }

    // Create Hive Schema and Tables
    for (String country : procModel.getCountryCodes()) {
      String stagingDbName = destModel.getStagingDbName();
      String newStagingDbName = stagingDbName.replace(UobConstants.COUNTRY_PARAM, country);
      hiveCreateList.add(hiveDao.createDatabase(newStagingDbName));
      hiveCreateList.add(hiveDao.createStagingHiveTable(procModel, country));
      hiveCreateList.add(hiveDao.createErrorHiveTable(procModel, country));
    }
    
    hiveCreateList.add(hiveDao.createDatabase(destModel.getHiveDbName()));
    hiveCreateList.add(hiveDao.createHiveTable(procModel));

    String action = "Registration of Process " + procModel.getProcNm() + " with ID " + procModel.getProcId();
    metadataInsertList.add(procModel.getInsertAuditDetailSql(action, UobConstants.SUCCESS));
    metadataInsertList.add("COMMIT;");
    
    String hiveCreateSqlKey = null;
    if (StringUtils.isNotEmpty(sqlFileName)) {
      hiveCreateSqlKey = sqlFileName.replace(UobConstants.SRC_SYS_PARAM, procModel.getSrcSysCd())
      															.replace(UobConstants.FILENM_PARAM, procName)
      															.replace(UobConstants.QUERY_TYPE_PARAM, UobConstants.HIVE_CREATE);
    }
    
    if (sqlList.containsKey(hiveCreateSqlKey)) {
      List<String> existingSqlList = sqlList.get(procName);
      existingSqlList.addAll(hiveCreateList);

      sqlList.put(hiveCreateSqlKey, existingSqlList);
    } else {
      sqlList.put(hiveCreateSqlKey, hiveCreateList);
    }

    logger.info("Process Registration completed: " + procModel.getProcNm());
  }
  
  private void processAdobeModel(AdobeProcessMapModel adobeProcModel) throws EDAGMyBatisException {
	    logger.info("Going to start registering process: " + adobeProcModel.getProcId());
	    List<String> metadataInsertList = new ArrayList<String>();
	    metadataInsertList.add("set define off;");
	    metadataInsertList.add("set sqlblanklines on;");
	    metadataInsertList.add("whenever SQLERROR EXIT ROLLBACK;");
	    List <String> processIdList = adobeProcModel.getProcId();
	    for(String temp : processIdList) {
	    	if(temp.endsWith("_H01"))
	    	{
	    		adobeProcModel.setMasterProcId(UobConstants.ADOBE_SITE_CATALYST_MASTER_PROCESS_HISTORY);
	    		adobeProcModel.setProcFreqCd("H");
	    		adobeProcModel.setProcDesc("Adobe Master Process for History Ingestion");
	    	}
	    	else
	    	{
	    		adobeProcModel.setMasterProcId(UobConstants.ADOBE_SITE_CATALYST_MASTER_PROCESS);
	    		adobeProcModel.setProcFreqCd("D");
	    		adobeProcModel.setProcDesc("Adobe Master Process for Ingestion");
	    	}
	    	metadataInsertList.add(adobeProcModel.getInsertAdobeProcSubProcSql(temp));
	    }
	    metadataInsertList.add(adobeProcModel.getInsertAdobeProcMasterSql());
	    metadataInsertList.add("COMMIT;");
	    
	    String insertSqlKey = null;
		String processName = "";

	    if(processIdList.get(0).endsWith("_H01"))
	    	processName = UobConstants.ADOBE_SITE_CATALYST_MASTER_PROCESS_HISTORY;
	    else
	    	processName = UobConstants.ADOBE_SITE_CATALYST_MASTER_PROCESS;
		
	    if (StringUtils.isNotEmpty(sqlFileName)) {
	      insertSqlKey = sqlFileName.replace(UobConstants.SRC_SYS_PARAM, UobConstants.ADOBE_SITE_CATALYST_SRC_SYS_CD)
	      													.replace(UobConstants.FILENM_PARAM, processName)
	      													.replace(UobConstants.QUERY_TYPE_PARAM, UobConstants.METADATA_INSERT);
	    }
	    
	    if (sqlList.containsKey(insertSqlKey)) {
	      List<String> existingSqlList = sqlList.get(UobConstants.ADOBE_SITE_CATALYST_MASTER_PROCESS);
	      existingSqlList.addAll(metadataInsertList);

	      sqlList.put(insertSqlKey, existingSqlList);
	    } else {
	      sqlList.put(insertSqlKey, metadataInsertList);
	    }
	    logger.info("Process Registration completed: " + UobConstants.ADOBE_SITE_CATALYST_MASTER_PROCESS);
  }
  private void cleanupAdobeModel(AdobeProcessMapModel adobeProcModel) throws EDAGMyBatisException {

	  logger.debug("Going to cleanup for file " + UobConstants.ADOBE_SITE_CATALYST_MASTER_PROCESS);

	 // Drop Metadata Entries
	  List<String> delSqlList = new ArrayList<String>();

	  delSqlList.add("set define off;");
	  delSqlList.add("whenever SQLERROR EXIT ROLLBACK;");
		
	  String processName = "";
	  List <String> processIdList = adobeProcModel.getProcId();
	  if(processIdList.get(0).endsWith("_H01"))
	    	processName = UobConstants.ADOBE_SITE_CATALYST_MASTER_PROCESS_HISTORY;
	  else
	    	processName = UobConstants.ADOBE_SITE_CATALYST_MASTER_PROCESS;
	  adobeProcModel.setMasterProcId(processName);
	  
	  delSqlList.add(adobeProcModel.getRemoveAdobeProcSubProcSql());
	  delSqlList.add(adobeProcModel.getRemoveAdobeProcMasterSql());
	
	  String delSqlKey = null;
	  if (StringUtils.isNotEmpty(sqlFileName)) {
		  delSqlKey = sqlFileName.replace(UobConstants.SRC_SYS_PARAM, UobConstants.ADOBE_SITE_CATALYST_SRC_SYS_CD)
		                   .replace(UobConstants.FILENM_PARAM, processName)
		                   .replace(UobConstants.QUERY_TYPE_PARAM, UobConstants.METADATA_DELETE);
	  }

	  delSqlList.add("COMMIT; ");

	  if (sqlList.containsKey(delSqlKey)) {
		  List<String> existingSqlList = sqlList.get(UobConstants.ADOBE_SITE_CATALYST_MASTER_PROCESS);
		  existingSqlList.addAll(delSqlList);

		  sqlList.put(delSqlKey, existingSqlList);
	  } else {
		  sqlList.put(delSqlKey, delSqlList);
	  }
	}
}
