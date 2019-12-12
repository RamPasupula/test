package com.uob.edag.processor;

import java.io.File;
import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
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
import org.apache.commons.lang3.StringUtils;
import org.apache.log4j.Logger;

import com.uob.edag.constants.UobConstants;
import com.uob.edag.exception.EDAGException;
import com.uob.edag.exception.EDAGIOException;
import com.uob.edag.exception.EDAGMyBatisException;
import com.uob.edag.exception.EDAGProcessorException;
import com.uob.edag.exception.EDAGValidationException;
import com.uob.edag.model.ExportModel;
import com.uob.edag.model.InterfaceSpec;
import com.uob.edag.model.ProcessModel;
import com.uob.edag.utils.FileUtils;
import com.uob.edag.utils.InterfaceSpecMap;
import com.uob.edag.utils.PropertyLoader;
import com.uob.edag.utils.UobInterfaceFileParser;
import com.uob.edag.utils.UobUtils;
import com.uob.edag.validation.InterfaceSpecHandler;

/**
 * @Author : Daya Venkatesan
 * @Date of Creation: 12/21/2016
 * @Description : The class is used for processing the uploaded process spec
 *              and generating the metadata for insertion later into the
 *              Metadata Database for the export process.
 * 
 */

public class ExportRegistrationGenerator {

  private static Logger logger = null;
  private static Options options = new Options();
  private Map<String, List<String>> sqlList = new HashMap<String, List<String>>();
  private static String sqlFileName = null;

  /**
   * This method will generate the SQLs to upload the data into the metadata database.
   * @param arguments Acceptable arguments are -h,-f and -r. Use Help to see more usage info.
   * @throws EDAGProcessorException 
   * @throws EDAGIOException 
   * @throws EDAGValidationException 
   * @throws Exception when there is any error in the registration process.
   */
  public static void main(String[] arguments) throws EDAGValidationException, EDAGIOException, EDAGProcessorException {
    boolean force = false;
    String filesToCleanup = null;
    String filesToProcess = null;

    CommandLineParser parser = new DefaultParser();
      options.addOption("h", "help", false, "Show Help");

      Option forceOpt = new Option("f", "force", false, 
      		                         "Rerun Registration Process for specific files within Process Spec with or without force cleanup. process_id arguments are optional");
      forceOpt.setArgs(2);
      forceOpt.setOptionalArg(true);
      forceOpt.setArgName("cleanup_process_id> <run_only_process_id");
      options.addOption(forceOpt);

      Option regOpt = new Option("r", "register", false, "Run Registration Process for given Process Spec");
      regOpt.setArgs(1);
      regOpt.setArgName("Process Spec Path");
      options.addOption(regOpt);

      CommandLine command;
			try {
				command = parser.parse(options, arguments);
			} catch (ParseException e) {
	      throw new EDAGProcessorException(EDAGProcessorException.CANNOT_PARSE_CLI_OPTIONS, e.getMessage());
			}

      if (command.hasOption("h")) {
        showHelp();
      }
      
      // TODO if it's mandatory, why do we have to specify it as an option?
      if (!command.hasOption("r")) {
        throw new EDAGProcessorException(EDAGProcessorException.MISSING_MANDATORY_OPTION, "Register Option is mandatory");
      }

      String[] registerArgs = command.getOptionValues("r");
      if (registerArgs == null || registerArgs.length < 1) {
        throw new EDAGProcessorException(EDAGProcessorException.INCORRECT_ARGS_FOR_OPTION, "Not enough arguments passed to run registration");
      }
      
      String processSpecPath = registerArgs[0];
      if (StringUtils.isBlank(processSpecPath)) {
        throw new EDAGValidationException(EDAGValidationException.EMPTY_VALUE, "Process Spec Path cannot be empty");
      }

      File procSpec = new File(processSpecPath);
      String fileName = procSpec.getName();
      String execDate = new SimpleDateFormat("yyyyMMddHHmmss").format(System.currentTimeMillis());
      String logFileName = "EDA_EXP_" + fileName + "_" + execDate + ".log";
      System.setProperty("logFileName", logFileName);
      logger = Logger.getLogger(ExportRegistrationGenerator.class);
      UobUtils.logJavaProperties();
      UobUtils.logPackageProperties();
      sqlFileName = PropertyLoader.getProperty(UobConstants.EXPORT_SQL_FILE_NAME);

      if (command.hasOption("f")) {
        force = true;
        logger.info("Force Rerun flag has been set.");

        String[] forceArgs = command.getOptionValues("f");
        logger.debug("Arguments for Force:" + Arrays.toString(forceArgs));
        if (forceArgs != null && forceArgs.length != 0) {
          if (forceArgs.length == 2) {
            filesToCleanup = forceArgs[0];
            filesToProcess = forceArgs[1];
          } else if (forceArgs.length == 1) {
            filesToCleanup = forceArgs[0];
          }

          if ("*".equalsIgnoreCase(filesToCleanup)) {
            if (StringUtils.isNotBlank(filesToProcess) && !"*".equalsIgnoreCase(filesToProcess.trim())) {
              throw new EDAGProcessorException("Request to cleanup all the processes, but load back only for certain processes. This can result in data loss.");
            } else {
              filesToProcess = "*";
              logger.info("Going to cleanup all processes and process all process ids again");
            }
          } else if (StringUtils.isBlank(filesToCleanup)) {
            if (StringUtils.isBlank(filesToProcess)) {
              logger.info("Going to Cleanup all processes and process all process ids again");
            } else if ("*".equalsIgnoreCase(filesToProcess.trim())) {
              logger.info("No cleanup required; Going to process all the process ids");
            } else {
              logger.info("No cleanup required; Going to process only selected process ids");
            }
          } else if (!"*".equalsIgnoreCase(filesToCleanup)) {
            if (StringUtils.isNotBlank(filesToProcess)) {
              logger.info("Going to cleanup the selected processes and process the selected process ids again");
              List<String> filesToCleanupList = Arrays.asList(filesToCleanup.split(UobConstants.COMMA));
              List<String> filesToProcessList = Arrays.asList(filesToProcess.split(UobConstants.COMMA));
              boolean allCleanupProcessed = filesToProcessList.containsAll(filesToCleanupList);
              if (!filesToCleanupList.isEmpty() && ! allCleanupProcessed) {
                throw new EDAGProcessorException("Request to cleanup some processes, but not load the same processes again. This can result in data loss.");
              }
            } else {
              filesToProcess = "*";
              logger.info("Going to do cleanup of the given processes; Going to process all process ids");
            }
          }
        } else {
          logger.info("Going to Cleanup all processes and process all process ids again");
          filesToCleanup = "*";
          filesToProcess = "*";
        }
      }
      
      ExportRegistrationGenerator regis = new ExportRegistrationGenerator();
			regis.processSpec(processSpecPath, force, filesToCleanup, filesToProcess);
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
   * This method is used to process an process specification.
   * @param procSpecPath The Path of the Process Specification in the file system.
   * @param force Whether or not to force overwrite existing registration.
   * @param filesToForce List of process ids which needs to be force overwritten and registered.
   * @param filesToProcess List of process ids which needs to be processed from the Process Spec.
   * @throws EDAGValidationException, EDAGIOException 
   * @throws EDAGProcessorException 
   * @throws Exception when there is any error in the Registration Process.
   */
  private void processSpec(String procSpecPath, boolean force, String filesToForce, 
  		                     String filesToProcess) throws EDAGValidationException, EDAGIOException, EDAGProcessorException {
    logger.info("Going to start registering process spec: " + procSpecPath);
    Map<String, EDAGException> errorProcesses = new HashMap<String, EDAGException>();
    int procSize = 0;
    try {
      // Parse the Interface Spec file
      UobInterfaceFileParser parser = new UobInterfaceFileParser();
      InterfaceSpec spec = parser.parseExportProcessSpecFile(procSpecPath);
      if (spec == null) {
        throw new EDAGValidationException(EDAGValidationException.NULL_VALUE, "Interface Specification", "There is no data to register in the process spec");
      }
      
      Map<String, InterfaceSpecMap> procSpecList = spec.getExportProcessSpec();
        
      // Validate the Spec file and Convert into ProcessModel
      InterfaceSpecHandler handler = new InterfaceSpecHandler();
      procSize = procSpecList.entrySet().size();
      for (Entry<String, InterfaceSpecMap> fileMap : procSpecList.entrySet()) {
        String processId = null;
        try {
          processId = fileMap.getKey();
          InterfaceSpecMap procSpec = fileMap.getValue();
          ProcessModel procModel = handler.handleExportSpecDetails(spec, force, filesToForce, filesToProcess, processId, 
          		                                                     procSpec);
          if (procModel == null) {
            continue;
          }
          
          // Force Cleanup
          if (force) {
            logger.info("Force is true, Cleanup files: " + filesToForce);
            cleanup(procModel, filesToForce, true);
          }
  
          String[] filesToProcessArr = new String[] {};
          if (StringUtils.isNotEmpty(filesToProcess) && !"*".equalsIgnoreCase(filesToProcess)) {
            logger.info("Going to process only for files: " + filesToProcess);
            filesToProcessArr = filesToProcess.split(UobConstants.COMMA);
          }
          
          if (filesToProcessArr.length > 0) {
            boolean process = false;
            for (String fileProc : filesToProcessArr) {
              if (StringUtils.trimToEmpty(fileProc).equalsIgnoreCase(procModel.getProcNm().trim())) {
                process(procModel);
                process = true;
              }
            }
            
            if (!process) {
              logger.info("Did not process the file: " + procModel.getProcNm());
            }
          } else {
            process(procModel);
          }
        } catch (EDAGException excp) {
          errorProcesses.put(processId, excp);
        } catch (Exception excp) {
        	errorProcesses.put(processId, new EDAGException(excp.getMessage()));
        }
      }

      // Create SQL File
      createSqlFile();

      logger.info("Bulk Registration completed for process spec: " + procSpecPath);
    } finally {
      if (!errorProcesses.isEmpty()) {
        for (Entry<String, EDAGException> errorProc : errorProcesses.entrySet()) {
          EDAGException excp = errorProc.getValue();
          logger.error("Process: " + errorProc.getKey() + " failed with error: " + excp.getMessage()); 
        }

        String message = procSize == errorProcesses.size() ? "All Processes failed to register" : "Some Processes failed to Register";
        throw new EDAGProcessorException(message);
      }
    }
  }

  /**
   * This method is used to create the SQL file with the inserts for the metadata tables and DDLs 
   * for the Hive table creation.
   * @throws EDAGIOException 
   * @throws IOException when there is an error in writing the SQL file.
   * @throws UobException when there is custom thrown exception.
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
          SimpleDateFormat sdf = new SimpleDateFormat("yyyyMMddHHmmss");
          sqlArchiveFileName = sqlFileArchiveLoc + File.separator + basename + UobConstants.UNDERSCORE + 
          		                 sdf.format(System.currentTimeMillis()) + "." + extension;
          if (!sqlFile.renameTo(new File(sqlArchiveFileName))) {
            throw new EDAGIOException(EDAGIOException.CANNOT_MOVE_FILE, sqlFile.getPath(), sqlArchiveFileName, "SQL file: " + sqlFile.getPath() + " was not archived");
          }
          
          logger.info("Existing SQL file: " + sqlFileNm + " archived successfully");
        }
      }
      
      FileUtils fileUtil = new FileUtils();
      fileUtil.writeToFile(sqlList, sqlFile);
      logger.info("New SQL file created successfully: " + fullFileName);
    }
  }

  /**
   * This method is used to remove the existing metadata entries if any for the process being
   * registered. This method will also rename the existing Hive tables with the current timestamp.
   * @param procModel Process to be cleaned up
   * @param filesToCleanup List of Files for which cleanup needs to be run
   * @param cleanupMetadata Whether or not to clean up the metadata
   * @throws Exception when there is any error in the cleanup process
   */
  private void cleanup(ProcessModel procModel, String filesToCleanup, boolean cleanupMetadata) {
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

    logger.info("Going to cleanup for file:" + procModel.getProcNm());

    String procId = procModel.getProcId();

    if (cleanup && cleanupMetadata) {
      if (procId != null && !"".equalsIgnoreCase(procId)) {
        // Drop Metadata Entries
        List<String> delSqlList = new ArrayList<String>();

        delSqlList.add("set define off;");
        
        delSqlList.add(procModel.getRemoveAlertsSql());
        
        delSqlList.add(procModel.getRemoveProcParamSql());
        
        delSqlList.add(procModel.getRemoveProcDownstreamApplSql());

        delSqlList.add(procModel.getRemoveExportProcessSql());

        delSqlList.add(procModel.getRemoveProcessCountrySql());

        delSqlList.add(procModel.getRemoveProcessMasterSql());

        String delSqlKey = null;
        if (StringUtils.isNotEmpty(sqlFileName)) {
          delSqlKey = sqlFileName.replace(UobConstants.SRC_SYS_PARAM, procModel.getSrcSysCd())
          		                   .replace(UobConstants.FILENM_PARAM, procId)
          		                   .replace(UobConstants.QUERY_TYPE_PARAM, UobConstants.METADATA_DELETE);
        }
        
        delSqlList.add("COMMIT;");
        
        if (sqlList.containsKey(delSqlKey)) {
          List<String> existingSqlList = sqlList.get(procId);
          existingSqlList.addAll(delSqlList);

          sqlList.put(delSqlKey, existingSqlList);
        } else {
          sqlList.put(delSqlKey, delSqlList);
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

    List<String> metadataInsertList = new ArrayList<String>();
    String procName = procModel.getProcNm();
    logger.debug("ProcModel is: " + procModel);
    
    metadataInsertList.add("set define off;");

    // Insert into Process Master
    metadataInsertList.add(procModel.getInsertProcessMasterSql());

    // Insert into Process Country
    List<String> ctrySqlList = procModel.getInsertProcessCountrySql();
    if (CollectionUtils.isNotEmpty(ctrySqlList)) {
      metadataInsertList.addAll(ctrySqlList);
    }

    // Insert into Export Process
    ExportModel destModel = procModel.getDestInfo();
    destModel.setProcessId(procModel.getProcId());
    metadataInsertList.add(destModel.getInsertExportProcessSql());

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
    
    String action = "Registration of Process " + procModel.getProcNm() + " with ID " + procModel.getProcId();
    metadataInsertList.add(procModel.getInsertAuditDetailSql(action, UobConstants.SUCCESS));

    metadataInsertList.add("COMMIT;");

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

    logger.info("Process Registration completed: " + procModel.getProcNm());
  }
}
