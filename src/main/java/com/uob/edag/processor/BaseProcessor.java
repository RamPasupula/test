package com.uob.edag.processor;


import java.io.File;
import java.sql.Timestamp;
import java.text.DateFormat;
import java.text.DecimalFormat;
import java.text.Format;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.exception.ExceptionUtils;
import org.apache.log4j.Logger;

import com.uob.edag.constants.UobConstants;
import com.uob.edag.constants.UobConstants.FileLayoutIdentifier;
import com.uob.edag.dao.IngestionDao;
import com.uob.edag.exception.EDAGException;
import com.uob.edag.exception.EDAGMyBatisException;
import com.uob.edag.exception.EDAGProcessorException;
import com.uob.edag.exception.EDAGValidationException;
import com.uob.edag.model.DestModel;
import com.uob.edag.model.FieldModel;
import com.uob.edag.model.FileModel;
import com.uob.edag.model.LoadProcess;
import com.uob.edag.model.ProcessInstanceModel;
import com.uob.edag.model.ProcessModel;
import com.uob.edag.model.ProcessParam;
import com.uob.edag.utils.MemoryLogger;
import com.uob.edag.utils.PropertyLoader;
import com.uob.edag.utils.StageHandler;
import com.uob.edag.utils.UobUtils;


/**
 * @Author : Daya Venkatesan
 * @Date of Creation: 10/31/2016
 * @Description : The class covers the workflow of a Ingestion Process.
 * 
 */

public class BaseProcessor {
  
  private static Logger staticLogger;
  private static Options options = new Options();
  private static final Format SALT_FORMATTER = new DecimalFormat("0000000000");
	
  protected Logger logger = Logger.getLogger(getClass());
  protected DateFormat sdf = new SimpleDateFormat("yyyyMMddHHmmss");
  protected IngestionDao ingestDao = new IngestionDao();
  protected StageHandler stgHndle = new StageHandler();
  protected ProcessInstanceModel procInstance;
  protected boolean failDumpedProcess = true;
  protected String hourToRun = null;
  protected boolean isBizDateValidationDisabled = Boolean.FALSE;
  protected boolean isRowCountReconciliationDisabled = Boolean.FALSE;
  protected boolean isHashSumReconciliationDisabled = Boolean.FALSE;
  protected boolean isMD5SumValidationDisabled = Boolean.FALSE;
  protected boolean isRowCountValidationDisabled = Boolean.FALSE;
  protected boolean isSkipErrRecordsDisabled = Boolean.FALSE;
  protected boolean isSkipIndexingFlag = Boolean.FALSE;

  private boolean init = false;
  private boolean implicitDecimalIndicator = false;
  private Map<Integer, FieldModel> destFieldMap = null;


  /**
   * This is the main class of any Ingestion Process. This retrieves the metadata for the
   * process id and calls the appropriate Ingestion Processor.
   * @param arguments Valid arguments are -h, -f and -i
   * @throws Exception when any error occurs in the Ingestion Process
   */
  public static void main(String[] arguments) throws EDAGException {
    boolean force = false;
    String forceFileName = null;
    CommandLineParser parser = new DefaultParser();
    String hourToRun = null;
    boolean isBizDateValidationDisabled = Boolean.FALSE;
    boolean isRowCountReconciliationDisabled = Boolean.FALSE;
    boolean isHashSumReconciliationDisabled = Boolean.FALSE;
    boolean isMD5SumValidationDisabled = Boolean.FALSE;
    boolean isRowCountValidationDisabled = Boolean.FALSE;
    boolean isSkipErrRecordsDisabled = Boolean.FALSE;
    boolean isSkipIndexingFlag = Boolean.FALSE;

    try {
      options.addOption("h", "help", false, "Show Help");

      Option forceOpt = new Option("f", "force", false, "Use this flag to force rerun the Ingestion Job from the Start");
      forceOpt.setArgs(1);
      forceOpt.setOptionalArg(true);
      forceOpt.setArgName("File Location");
      options.addOption(forceOpt);

      Option inOpt = new Option("i", "ingest", false, "Run Ingestion Process for given Parameters");
      inOpt.setArgs(3);
      inOpt.setArgName("Process ID> <Biz Date> <Country Code");
      options.addOption(inOpt);
      
      Option hourOpt = new Option("o", "hour", false, "Run Ingestion Process for specified hour");
      hourOpt.setArgs(1);
      inOpt.setArgName("hour option to run the ingestion process for the specified hour");
      options.addOption(hourOpt);
      
      Option businessDateOpt = new Option("b", "skip-biz-date-val", false, "Skip Business Date Validation");
      businessDateOpt.setArgs(0);
      inOpt.setArgName("Skip business date validation");
      options.addOption(businessDateOpt);
      
      Option rowCountOpt = new Option("r", "skip-row-count-recon", false, "Skip Row Count Reconciliation");
      rowCountOpt.setArgs(0);
      inOpt.setArgName("Skip Row Count Reconciliation");
      options.addOption(rowCountOpt);
      
      Option rowCountValidationOpt = new Option("c", "skip-row-count-val", false, "Skip Row Count Reconciliation");
      rowCountValidationOpt.setArgs(0);
      inOpt.setArgName("Skip Row Count Validation");
      options.addOption(rowCountValidationOpt);
      
      Option hashSumOpt = new Option("s", "skip-hash-sum-recon", false, "Skip Hash Sum Reconciliation");
      hashSumOpt.setArgs(0);
      inOpt.setArgName("Skip Hash Sum Reconciliation");
      options.addOption(hashSumOpt);
      
      Option md5SumOpt = new Option("m", "skip-md5-sum-val", false, "Skip MD5 Hash Sum Validation");
      md5SumOpt.setArgs(0);
      inOpt.setArgName("Skip MD5 Hash Sum Validation");
      options.addOption(md5SumOpt);


      Option skipErrRecordsOpt = new Option("p", "skip-err-records-in-pre-proc", false, "Skip error records ingestion failures");
      skipErrRecordsOpt.setArgs(0);
      inOpt.setArgName("Skip error records ingestion failures");
      options.addOption(skipErrRecordsOpt);
      
      Option skipIndexingFlag = new Option("x", "skip-indexing-flag", false, "Skip Indexing Process");
      skipIndexingFlag.setArgs(0);
      inOpt.setArgName("Skip Indexing Process");
      options.addOption(skipIndexingFlag);

      CommandLine command = parser.parse(options, arguments);

      if (command.hasOption("h")) {
        showHelp();
      }

      if (command.hasOption("f")) {
        force = true;
        forceFileName = command.getOptionValue("f");
      }
      
      if(command.hasOption("o")) {
    	  hourToRun = command.getOptionValue("o");
      }
      
      if(command.hasOption("b")) {
    	  isBizDateValidationDisabled = Boolean.TRUE;
      }
      
      if(command.hasOption("r")) {
    	  isRowCountReconciliationDisabled = Boolean.TRUE;
      }
      
      if(command.hasOption("c")) {
    	  isRowCountValidationDisabled = Boolean.TRUE;
      }
      
      if(command.hasOption("s")) {
    	  isHashSumReconciliationDisabled = Boolean.TRUE;
      }
      
      if(command.hasOption("m")) {
    	  isMD5SumValidationDisabled = Boolean.TRUE;
      }
      
      if(command.hasOption("x")) {
    	  isSkipIndexingFlag = Boolean.TRUE;
      }
      
      if (!command.hasOption("i")) {
        throw new EDAGProcessorException("Ingestion Option is mandatory");
      }
      
      String[] args = command.getOptionValues("i");
      if (args == null || args.length < 3) {
        throw new EDAGProcessorException("Not enough arguments passed to run Ingestion");
      }

      if(command.hasOption("p")) {
        isSkipErrRecordsDisabled = Boolean.TRUE;

        if(!isRowCountReconciliationDisabled)
          throw new EDAGProcessorException("param -r or --skip-row-count-recon must be passed alongwith -p");

        if(!isRowCountValidationDisabled)
          throw new EDAGProcessorException("param -c or --skip-row-count-val must be passed alongwith -p");

        if(!isHashSumReconciliationDisabled)
          throw new EDAGProcessorException("param -s or --skip-hash-sum-recon must be  passed alongwith -p");
      }

      
      DateFormat formatter = new SimpleDateFormat("yyyyMMddHHmmss");
      String execDate = formatter.format(System.currentTimeMillis());
      String procId = args[0];
      String bizDate = args[1];
      try {
				new SimpleDateFormat("yyyy-MM-dd").parse(bizDate);
			} catch (java.text.ParseException e) {
				throw new EDAGValidationException(EDAGValidationException.INVALID_DATE_FORMAT, bizDate, "yyyy-MM-dd", e.getMessage());
			}
      
      String ctryCd = args[2];
      String logFileName = "EDA_FI_" + ctryCd + "_DL_" + procId + "_" + bizDate + "_" + execDate + ".log";
      System.setProperty("logFileName", logFileName);
      staticLogger = Logger.getLogger(BaseProcessor.class);
      UobUtils.logJavaProperties();
      UobUtils.logPackageProperties();
      staticLogger.info("Base Processor Starts: " + execDate);
      staticLogger.info(String.format("Arguments: Proc ID: %s, bizDate: %s, ctryCd: %s, hourToRun: %s, "
      		+ "isBizDateValidationDisabled: %s, isRowCountReconciliationDisabled: %s, isHashSumValidationDisabled %s, isMD5SumValidationDisabled: %s, "
      		+ "isRowCountValidationDisabled %s isSkipErrRecordsDisabled %s" ,  
      		procId, bizDate, ctryCd, StringUtils.defaultString(hourToRun), String.valueOf(isBizDateValidationDisabled), String.valueOf(isRowCountReconciliationDisabled),
      		String.valueOf(isHashSumReconciliationDisabled), String.valueOf(isMD5SumValidationDisabled), String.valueOf(isRowCountValidationDisabled),
      		String.valueOf(isSkipErrRecordsDisabled)));
      
      // monitor memory usage if applicable
      MemoryLogger memLogger = new MemoryLogger();
      new Thread(memLogger).start();
      
      BaseProcessor ingProc = new BaseProcessor();
      if(hourToRun != null) {
    	  ingProc.hourToRun = hourToRun;
      }
      
      //Changed as part of Adobe Site Catalyst Ingestion
		if(!procId.equalsIgnoreCase(UobConstants.ADOBE_SITE_CATALYST_MASTER_PROCESS) && 
			!procId.equalsIgnoreCase(UobConstants.ADOBE_SITE_CATALYST_MASTER_PROCESS_HISTORY)) {
          ingProc.isBizDateValidationDisabled = isBizDateValidationDisabled;
          ingProc.isRowCountReconciliationDisabled = isRowCountReconciliationDisabled;
          ingProc.isHashSumReconciliationDisabled = isHashSumReconciliationDisabled;
          ingProc.isMD5SumValidationDisabled = isMD5SumValidationDisabled;
          ingProc.isRowCountValidationDisabled = isRowCountValidationDisabled;
          ingProc.isSkipErrRecordsDisabled = isSkipErrRecordsDisabled;
          ingProc.isSkipIndexingFlag = isSkipIndexingFlag;
    	  ingProc.runIngestion(procId, bizDate, ctryCd, force, forceFileName);
    	  staticLogger.info("procInsModel : " + ingProc.procInstance);
     } else {
    	  AdobeSiteCatalystIngestionProcessor adbIngProc = new AdobeSiteCatalystIngestionProcessor();
    	  adbIngProc.isBizDateValidationDisabled = isBizDateValidationDisabled;
    	  adbIngProc.isRowCountReconciliationDisabled = isRowCountReconciliationDisabled;
    	  adbIngProc.isHashSumReconciliationDisabled = isHashSumReconciliationDisabled;
    	  adbIngProc.isMD5SumValidationDisabled = isMD5SumValidationDisabled;
    	  adbIngProc.isRowCountValidationDisabled = isRowCountValidationDisabled;
          adbIngProc.isSkipErrRecordsDisabled = isSkipErrRecordsDisabled;
          adbIngProc.runIngestion(procId, bizDate, ctryCd, force, forceFileName);
      }
      staticLogger.info("Base Processor Completed: " + formatter.format(System.currentTimeMillis()));
    } catch (ParseException excp) {
      throw new EDAGProcessorException(EDAGProcessorException.CANNOT_PARSE_CLI_OPTIONS, excp.getMessage());
    }
  }

  /**
   * This method displays the Help for the Usage of this Java class.
   */
  public static void showHelp() {
    HelpFormatter formatter = new HelpFormatter();
    formatter.printHelp("BaseProcessor", options);
    System.exit(0);
  }

  /**
   * This method is used to initiate the Ingestion Process. It logs the start of the process 
   * in the metadata tables.
   * @param procModel Process Model containing metadata for the process to be executed
   * @param bizDate Business Date for which process is executed
   * @param ctryCd Country Code for which process is executed
   * @throws Exception when there is an error in inserting into the Process Log
   */ 
  protected void initIngestion(ProcessModel procModel, String bizDate, String ctryCd) throws EDAGMyBatisException {
		String fail = PropertyLoader.getProperty(BaseProcessor.class + ".FailDumpedProcess");
		if ("false".equalsIgnoreCase(fail)) {
			failDumpedProcess = false;
		}
  	
    // Process Log
  	init = false;
  	procInstance = new ProcessInstanceModel(procModel);
    String procInstanceId = createProcInstanceId(procModel.getProcId(), bizDate, ctryCd);
    procInstance.setProcInstanceId(procInstanceId);
    procInstance.setBizDate(bizDate);
    procInstance.setCountryCd(ctryCd);
    procInstance.setStatus(UobConstants.RUNNING);
    procInstance.setStartTime(new Timestamp(System.currentTimeMillis()));
    procInstance.setHourToRun(this.hourToRun);
    procInstance.setBizDateValidationDisabled(this.isBizDateValidationDisabled);
    procInstance.setRowCountReconciliationDisabled(this.isRowCountReconciliationDisabled);
    procInstance.setRowCountValidationDisabled(this.isRowCountValidationDisabled);
    procInstance.setHashSumReconciliationDisabled(this.isHashSumReconciliationDisabled);
    procInstance.setMD5SumValidationDisabled(this.isMD5SumValidationDisabled);
    procInstance.setSkipErrRecordsEnabled(this.isSkipErrRecordsDisabled);
    procInstance.setSkipIndexingEnabled(this.isSkipIndexingFlag);
	ingestDao.insertProcessLog(procInstance);
	init = true;
  }

  private String createProcInstanceId(String procId, String bizDate, String ctryCd) {
		String result = new SimpleDateFormat("yyyyMMddHHmmssSSS").format(new Date());
		String salt = SALT_FORMATTER.format((procId + "|" + bizDate + "|" + ctryCd).hashCode());
		return result + "." + salt;
	}

	/**
   * This method is used to Finalize the Ingestion Process. It inserts the Completion entry
   * in the metadata tables.
   * @param excp The exception that was caught in the execution of the process
   * @throws Exception when there is an error inserting into the metadata
   */
  protected void finalizeIngestion(EDAGException excp, String status) throws EDAGMyBatisException {
    if (init) {
    	procInstance.setStatus(status);
    	procInstance.setEndTime(new Timestamp(System.currentTimeMillis()));
    	
    	if (excp != null) {
        String errorMessage = StringUtils.trimToEmpty(excp.getMessage());
        errorMessage = errorMessage.length() > 2000 ? errorMessage.substring(0, 1999) : errorMessage; 
        procInstance.setException(excp);
    	}
    	
			ingestDao.updateProcessLog(procInstance);
    }
  }
  
  /**
   * This method is used the retrieve the Process Metadata from the Metadata tables for the
   * given arguments.
   * @param procId Process ID of the File to be Ingested
   * @param ctryCd Country for which the Process is to be executed
   * @return Returns a Process Model objects
   * @throws Exception when there is an error in retrieving the metadata from tables
   */
  public ProcessModel retrieveMetadata(String procId, String ctryCd) throws EDAGMyBatisException, EDAGValidationException {

    ProcessModel procModel = ingestDao.retrieveProcessMaster(procId);
    if (procModel == null) {
      throw new EDAGValidationException(EDAGValidationException.NULL_VALUE, "Process Model", "There is no active process for Proc ID: " + procId);
    }

    logger.debug("Retrieving Country Code from Process Country for: " + procId);
    procModel.setCountryAttributesMap(ingestDao.retrieveProcessCountry(procId));

    // Retrieve from Load Process
    String priority = procModel.getProcPriority();
    String bdmConnName = PropertyLoader.getProperty("BDM_" + priority + "_CONNECTION");
    if (bdmConnName == null || "".equalsIgnoreCase(bdmConnName)) {
      bdmConnName = PropertyLoader.getProperty("BDM_M_CONNECTION");
    }
    
    String queueName = PropertyLoader.getProperty(priority + "_QUEUE");
    if (queueName == null || "".equalsIgnoreCase(queueName)) {
      queueName = PropertyLoader.getProperty("M_QUEUE");
    }

    // Retrieve from Load Process
    logger.debug("Retrieving data from Load Process for: " + procId);
    DestModel destModel = ingestDao.retrieveLoadProcess(procId);
    destModel.setBdmConnName(bdmConnName);
    destModel.setHadoopQueueName(queueName);

    FileModel fileModel = retrieveFileModel(procId);
    if (fileModel != null) {
    	destModel.setDestFieldInfo(this.destFieldMap);
    }

    // Retrieve from Load Params
    logger.debug("Retrieving data from Process Param for: " + procId);
    List<ProcessParam> procParamList = ingestDao.retrieveLoadParams(procId);
    procModel.setProcParam(procParamList);
    
    procModel.setSrcInfo(fileModel);
    procModel.setDestInfo(destModel);
    
    // retrieve source table detail
    procModel.setSourceTableDetail(ingestDao.retrieveSourceTableDetail(procId));
    
    // set load process from dest model
    procModel.setLoadProcess(new LoadProcess(destModel));

    return procModel;
  }
  
  protected Map<Integer, FieldModel> retrieveDestFieldMap(List<FieldModel> fieldList) throws EDAGMyBatisException {
  	Map<Integer, FieldModel> destFieldList = new HashMap<Integer, FieldModel>();
  	
  	// Retrieve from Field Standardization Rules
  	implicitDecimalIndicator = false;
    for (FieldModel fldModel : fieldList) {
      List<Integer> rulesList = ingestDao.retrieveFieldStdRules(fldModel);
			
      fldModel.setRulesList(rulesList);
      if (UobConstants.SRC_SIGNED_DECIMAL.equalsIgnoreCase(fldModel.getDataType()) || 
      		UobConstants.SRC_NUMERIC.equalsIgnoreCase(fldModel.getDataType()) || 
      		UobConstants.SRC_PACKED.equalsIgnoreCase(fldModel.getDataType())) {
        if (rulesList.contains(1)) {
          fldModel.setDecimalIndicator(UobConstants.IMPLICIT);
          implicitDecimalIndicator = true;
        } else {
          fldModel.setDecimalIndicator(UobConstants.EXPLICIT);
        }
      }

      // Create Destination Field Information
      if (fldModel.getRecordType().isData()) {
        FieldModel destFldModel = fldModel.clone();
        destFldModel.setFieldName(destFldModel.getNormalizedFieldName());

        switch (destFldModel.getDataType()) {
          case UobConstants.SRC_ALPHANUMERIC:
            destFldModel.setDataType(UobConstants.STRING);
            break;
          case UobConstants.SRC_SIGNED_DECIMAL:
          case UobConstants.SRC_NUMERIC:
          case UobConstants.SRC_PACKED:
            int length = destFldModel.getLength();
            int precision = destFldModel.getDecimalPrecision();
            if(length > 38) length = 38;
            destFldModel.setDataType(UobConstants.DECIMAL + "(" + length +  UobConstants.COMMA + precision + ")");
            break;
          case UobConstants.SRC_DATE:
            destFldModel.setDataType(UobConstants.TIMESTAMP);
            break;
          case UobConstants.SRC_TIMESTAMP:
            destFldModel.setDataType(UobConstants.TIMESTAMP);
            break;
          case UobConstants.SRC_OPEN:
            destFldModel.setDataType(UobConstants.STRING);
            break;
          default:
            break;
        }
        
        destFieldList.put(fldModel.getFieldNum(), destFldModel);
      }
    }
  	
  	return destFieldList;
  }
  
  protected FileModel retrieveFileModel(String procId) throws EDAGMyBatisException {
    // Retrieve from File Detail & Control File Detail
    FileModel fileModel = ingestDao.retrieveFileDetails(procId);
    
    if (fileModel != null) {
    	// Retrieve from Source Fields Info
      List<FieldModel> fieldList = ingestDao.retrieveFieldDetails(fileModel.getFileId());
      
      this.destFieldMap = retrieveDestFieldMap(fieldList);
      
      // Retrieve from Control Fields Info
      List<FieldModel> controlFieldList = ingestDao.retrieveControlFieldDetails(fileModel.getFileId());
  		
      for (FieldModel fldModel : controlFieldList) {
        if (UobConstants.SRC_SIGNED_DECIMAL.equalsIgnoreCase(fldModel.getDataType()) || 
        		UobConstants.SRC_NUMERIC.equalsIgnoreCase(fldModel.getDataType())) {
        	fldModel.setDecimalIndicator(implicitDecimalIndicator ? UobConstants.IMPLICIT : UobConstants.EXPLICIT);
        }
      }
      
      fileModel.setCtrlInfo(controlFieldList);
      fileModel.setSrcFieldInfo(fieldList);
    }

    return fileModel;
  }  

  /**
   * This method is used to run the Ingestion process from the main method.
   * @param procId Process ID for which the file is to be ingested
   * @param bizDate Business Date for which the process is to be executed
   * @param ctryCd Country code for which the process is to be executed
   * @param force Force flag applied for Ingestion
   * @param forceFileName Names of the files which need to be force ingested.
   * @throws EDAGException 
   * @throws Exception when there is any error in the Ingestion Process
   */
  public void runIngestion(String procId, String bizDate, String ctryCd, boolean force, 
  		                     String forceFileName) throws EDAGException {
    ProcessModel procModel = null;
    try {
      if (StringUtils.isBlank(procId)) {
        throw new EDAGValidationException(EDAGValidationException.NULL_VALUE, "Process ID", "Process ID is NULL or Empty");
      }
      
      if (bizDate == null ) { 
        throw new EDAGValidationException(EDAGValidationException.NULL_VALUE, "Business Date", "Biz Date is NULL");
      }
      
      if (StringUtils.isEmpty(ctryCd)) {
        throw new EDAGValidationException(EDAGValidationException.NULL_VALUE, "Country Code", "Country Code is NULL or Empty");
      }
      
      if (StringUtils.isNotEmpty(forceFileName)) {
        logger.info("Going to process file: " + forceFileName);
      }

      // Retrieve from Process Master
      procModel = retrieveMetadata(procId, ctryCd);
      
      // Init Ingestion
      logger.debug("Going to initialize Process log entry");
      initIngestion(procModel, bizDate, ctryCd);
        
      try {
        if (!procModel.getCountryCodes().contains(ctryCd)) {
          throw new EDAGValidationException(EDAGValidationException.MISSING_VALUE, ctryCd, 
          		                              "Available countries for process ID " + procModel.getProcId() + " are " + procModel.getCountryCodes());
        }

        // Check for Biz Date Expression
        String bizDateExpr = null;

        // Evaluate Business Date from Expression
        List<ProcessParam> procParamList = procModel.getProcParam();
        for (ProcessParam param : procParamList) {
          if (UobConstants.BIZ_DT_EXPR.equalsIgnoreCase(param.getParamName())) {
            bizDateExpr = param.getParamValue();
            // TODO TO_DATE function is Oracle-specific
            bizDateExpr = bizDateExpr.toLowerCase().replace(UobConstants.BIZ_DATE_PARAM, "TO_DATE('" + bizDate + "', 'yyyy-MM-dd')");
          }
          
          if (UobConstants.BIZ_DT.equalsIgnoreCase(param.getParamName())) {
            bizDate = param.getParamValue();
            logger.debug("Going to use overridden business date: " + bizDate);
          }
        }

        // Evaluate Biz Date Expression
        if (bizDateExpr != null) {
          bizDate = ingestDao.evaluateBizDtExpr(bizDateExpr);
          logger.debug(bizDateExpr + " business date is evaluated to " + bizDate);
        }

        // Set Variables

        // Setting Source Dir Path
        FileModel fileModel = procModel.getSrcInfo();
        String filePath = fileModel.getSourceDirectory();
        if (StringUtils.isNotEmpty(filePath)) {
          filePath = filePath.replace(UobConstants.COUNTRY_PARAM, ctryCd.toLowerCase());
          //For Adobe Site Catalyst Ingestion
          if(procModel.getSrcSysCd().equalsIgnoreCase(UobConstants.ADOBE_SITE_CATALYST_SRC_SYS_CD)) {
        	  filePath = filePath + "." + fileModel.getSourceFileExtn();
          }
          fileModel.setSourceDirectory(filePath);

          logger.info("Setting additional proc instance file attributes ...");
          procInstance = setFileSizeTime(filePath, procInstance); // EDF 236

          logger.info("Additional File Attributes = { " + "file size (bytes): " + procInstance.getSrcFileSizeBytes()
                  + ", file arrival time: " + procInstance.getSrcFileArrivalTime() + " }");
          ingestDao.updateProcessLogFileSizeTime(procInstance); // EDF 236
        }

        // Setting Archive Dir Path
        String archFilePath = fileModel.getSourceArchivalDir();
        if (StringUtils.isNotEmpty(archFilePath)) {
          archFilePath = archFilePath.replace(UobConstants.COUNTRY_PARAM, ctryCd.toLowerCase());
          fileModel.setSourceArchivalDir(archFilePath);
        }

        // Setting Control File Path
        String ctrlFilePath = fileModel.getControlFileDir();
        if (StringUtils.isNotEmpty(ctrlFilePath)) {
          ctrlFilePath = ctrlFilePath.replace(UobConstants.COUNTRY_PARAM, ctryCd.toLowerCase());
          fileModel.setControlFileDir(ctrlFilePath);
        }

        // Setting Staging Dir Path
        DestModel destModel = procModel.getDestInfo();
        String stagingDirName = destModel.getStagingDir();
        if (StringUtils.isNotEmpty(stagingDirName)) {
          stagingDirName = stagingDirName.replace(UobConstants.COUNTRY_PARAM, ctryCd)
          															 .replace(UobConstants.BIZ_DATE_PARAM, bizDate);
          destModel.setStagingDir(stagingDirName);
        }
        
        // Setting Staging DB Name
        String stagingDb = destModel.getStagingDbName();
        if (StringUtils.isNotEmpty(stagingDb)) {
          stagingDb = stagingDb.replace(UobConstants.COUNTRY_PARAM, ctryCd);
          destModel.setStagingDbName(stagingDb);
        }

        // Setting Staging Partition
        String stagingHivePart = destModel.getStagingHivePartition();
        if (StringUtils.isNotEmpty(stagingHivePart)) {
          String bizDatePartVal = "'" + bizDate + "'";
          stagingHivePart = stagingHivePart.toLowerCase().replace(UobConstants.BIZ_DATE_PARAM, bizDatePartVal);
          destModel.setStagingHivePartition(stagingHivePart);
        }

        // Setting DDS Partition
        String hivePart = destModel.getHivePartition();
        destModel.setTargetTablePartitionString(hivePart);
        if (StringUtils.isNotEmpty(hivePart)) {
          String ctryCdPartVal = "'" + ctryCd + "'";
          String bizDatePartVal = "'" + bizDate + "'";
          String procInstanceIdPartVal = "'" + this.procInstance.getProcInstanceId() + "'";
          hivePart = hivePart.toLowerCase().trim().replace(UobConstants.SITE_ID_PARAM, ctryCdPartVal)
          		                                    .replace(UobConstants.BIZ_DATE_PARAM, bizDatePartVal)
          		                                    .replace(UobConstants.PROC_INSTANCE_ID_PARAM, procInstanceIdPartVal);
          destModel.setHivePartition(hivePart);
        }

        procModel.setSrcInfo(fileModel);
        procModel.setDestInfo(destModel);
        
      } catch (Throwable excp) {
        procInstance.setException(excp instanceof EDAGException ? (EDAGException) excp : new EDAGException(excp.getMessage()));
        stgHndle.addStageLog(procInstance, UobConstants.STAGE_INGEST_PROC_INIT);
        stgHndle.updateStageLog(procInstance, UobConstants.STAGE_INGEST_PROC_INIT, null);
        throw excp;
      }

      String procType = procModel.getProcTypeCd();
      switch (procType) {
        case UobConstants.FILE_INGEST_PROC_TYPE:
          IngestionProcessor ingestionProcessor = getIngestionprocessor(procModel);
          ingestionProcessor.runFileIngestion(procInstance, procModel, bizDate, ctryCd, force, forceFileName);
          break;
        default:
          throw new EDAGValidationException(EDAGValidationException.INVALID_VALUE, procType, "Invalid Process Type: " + procType);
      }

      // Finalize Ingestion
      logger.debug("Going to finalize process log entry");
      if (procInstance.isDumped()) {
        // EDF-209
      	if (failDumpedProcess) {
         	throw procInstance.getException();
      	} else {
      		finalizeIngestion(procInstance.getException(), UobConstants.SUCCESS);
      	}
      } else {
      	finalizeIngestion(null, UobConstants.SUCCESS);
      }
    } catch (EDAGException excp) {
			finalizeIngestion(excp, UobConstants.FAILURE);
			logger.error(ExceptionUtils.getStackTrace(excp));
      throw excp;
    } catch (Throwable excp) {
    	EDAGException ex = new EDAGException(excp.getMessage(), excp);
    	finalizeIngestion(ex, UobConstants.FAILURE);
    	logger.error(ExceptionUtils.getStackTrace(excp));
    	throw ex;
    }
  }

  private IngestionProcessor getIngestionprocessor(ProcessModel procModel) {
	  IngestionProcessor ingestionProcessor;
	  FileModel fileModel = procModel.getSrcInfo();
	  FileLayoutIdentifier layout = FileLayoutIdentifier.getFileLayoutType(fileModel.getSourceFileLayoutCd());
	  
	  switch(layout) {
	  	case XLS_WITH_HEADER:
	  	case XLS_WITHOUT_HEADER:
	  	case XLSX_WITH_HEADER:
	  	case XLSX_WITHOUT_HEADER:
	  		ingestionProcessor = new ExcelFileIngestionProcessor();
	  		break;
	  	case REG_EXPRESSION:
	  		ingestionProcessor = getRegularExpressionProcessor(procModel);
	  		break;
                case CSV:
                        ingestionProcessor = new SwiftFileIngestionProcessor();
                        break;
	  	default:
	  		ingestionProcessor = new FileIngestionProcessor();
	  		break;
	  }
	  
	  if(ingestionProcessor instanceof FileIngestionProcessor) {
		  long count = fileModel.getSrcFieldInfo().stream().filter( 
				  f -> f.getDataType().equalsIgnoreCase(UobConstants.SRC_FILE_REFERENCE)).count();
		  if( count > 0) {
			  ingestionProcessor = new FileAttachmentProcessor();
		  }
	  }
	  return ingestionProcessor;
  }

  private IngestionProcessor getRegularExpressionProcessor(ProcessModel procModel) {
	  
	  long count = procModel.getProcParam().stream().filter(f -> f.getParamName().equalsIgnoreCase(UobConstants.WEBLOGS_HDFS_FOLDER_PATH) 
			  || f.getParamName().equalsIgnoreCase(UobConstants.WEBLOGS_TOPIC_NAME)
			  || f.getParamName().equalsIgnoreCase(UobConstants.WEBLOGS_EDAG_LOAD_TABLE_FREQUENCY)).count();
	  
 	  if(count > 0) {
 		  procModel.getSrcInfo().setIsWebLogsProcessing(Boolean.TRUE);
 		  return new WebLogsIngestionProcessor();
 	  } else {
 		 return new PSWeblogIngestionProcessor();
 	  }
	  
  }

  // EDF 236
  static ProcessInstanceModel setFileSizeTime(String filePath, ProcessInstanceModel procIns){
    procIns.setSrcFileSizeBytes(String.valueOf(new File(filePath).length()));
    procIns.setSrcFileArrivalTime(new SimpleDateFormat("yyyy/MM/dd HH:mm:ss")
            .format(new File(filePath).lastModified()));
    return procIns;
  }
  
}
