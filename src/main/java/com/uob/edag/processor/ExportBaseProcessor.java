package com.uob.edag.processor;

import java.sql.Timestamp;
import java.text.SimpleDateFormat;
import java.util.List;
import java.util.UUID;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.commons.lang.StringUtils;
import org.apache.log4j.Logger;

import com.uob.edag.constants.UobConstants;
import com.uob.edag.dao.ExportDao;
import com.uob.edag.exception.EDAGException;
import com.uob.edag.exception.EDAGMyBatisException;
import com.uob.edag.exception.EDAGProcessorException;
import com.uob.edag.exception.EDAGValidationException;
import com.uob.edag.model.DestModel;
import com.uob.edag.model.ProcessInstanceModel;
import com.uob.edag.model.ProcessModel;
import com.uob.edag.model.ProcessParam;
import com.uob.edag.utils.StageHandler;
import com.uob.edag.utils.UobUtils;


/**
 * @Author : Ganapathy Raman & Daya Venkatesan
 * @Date of Creation: 1/11/2017
 * @Description : The class covers the workflow of a Export Process.
 * 
 */

public class ExportBaseProcessor {
	private static Logger staticLogger;
	private static Options options = new Options();
	
  protected Logger logger = Logger.getLogger(getClass());
  protected StageHandler stgHndle = new StageHandler();
  protected ExportDao exportDao = new ExportDao();

  /**
   * This is the main class of any Export Process. This retrieves the metadata for the
   * process id and calls the appropriate Export Processor.
   * @param arguments Valid arguments are -h, -f and -e
   * @throws EDAGProcessorException 
   * @throws Exception when any error occurs in the Export Process
   */
  public static void main(String[] arguments) throws EDAGException {
    boolean force = false;
    String forceFileName = null;
    CommandLineParser parser = new DefaultParser();
    try {
      options.addOption("h", "help", false, "Show Help");
    
      Option forceOpt = new Option("f", "force", false, "Use this flag to force rerun the Export Job from the Start");
      forceOpt.setArgs(1);
      forceOpt.setOptionalArg(true);
      forceOpt.setArgName("File Location");
      options.addOption(forceOpt);
    
      Option exOpt = new Option("e", "export", false, "Run export Process for given Parameters");
      exOpt.setArgs(3);
      exOpt.setArgName("Process ID> <Biz Date> <Country Code");
      options.addOption(exOpt);
    
      CommandLine command = parser.parse(options, arguments);

      if (command.hasOption("h")) {
        showHelp();
      }

      if (command.hasOption("f")) {
        // logger.info("The Force Rerun flag has been set.");
        force = true;
        forceFileName = command.getOptionValue("f");
      }

      if (! command.hasOption("e")) {
        throw new EDAGProcessorException(EDAGProcessorException.MISSING_MANDATORY_OPTION, "e", "Export Option is mandatory");
      }
      String[] args = command.getOptionValues("e");
      if (args == null || args.length < 3) {
        throw new EDAGProcessorException(EDAGProcessorException.INCORRECT_ARGS_FOR_OPTION, "e", "Not enough arguments passed to run Export");
      }
      String procId = args[0];
      String bizDate = args[1];
      
      try {
      	new SimpleDateFormat("yyyy-MM-dd").parse(bizDate);
      } catch (java.text.ParseException e) {
      	throw new EDAGValidationException(EDAGValidationException.INVALID_DATE_FORMAT, bizDate, "yyyy-MM-dd", e.getMessage());
      }
      
      String ctryCd = args[2];
      String execDate = new SimpleDateFormat("yyyyMMddHHmmss").format(System.currentTimeMillis());
      String logFileName = "EDA_EXP_" + ctryCd + "_DL_" + procId + "_" + bizDate + "_" + execDate + ".log";
      System.setProperty("logFileName", logFileName);
      staticLogger = Logger.getLogger(ExportBaseProcessor.class);
      UobUtils.logJavaProperties();
      UobUtils.logPackageProperties();
      ExportBaseProcessor exProc = new ExportBaseProcessor();

      exProc.runExport(procId, bizDate, ctryCd, force, forceFileName);
      staticLogger.info("Process ID " + procId + " for business date " + bizDate +  " and country " + ctryCd + " exported successfully");
    } catch (ParseException excp) {
      throw new EDAGProcessorException(EDAGProcessorException.CANNOT_PARSE_CLI_OPTIONS, excp.getMessage());
    }
  }

  /**
   * This method displays the Help for the Usage of this Java class.
   */
  public static void showHelp() {
    HelpFormatter formatter = new HelpFormatter();
    formatter.printHelp("ExportBaseProcessor", options);
    System.exit(0);
  }

  /**
   * This method is used to initiate the Export Process. It logs the start of the process 
   * in the metadata tables.
   * @param procModel Process Model containing metadata for the process to be executed
   * @param bizDate Business Date for which process is executed
   * @param ctryCd Country Code for which process is executed
   * @throws EDAGMyBatisException 
   * @throws Exception when there is an error in inserting into the Process Log
   */ 
  public ProcessInstanceModel initExport(ProcessModel procModel, String bizDate, String ctryCd) throws EDAGMyBatisException {
    // Process Log
  	ProcessInstanceModel procInstance = new ProcessInstanceModel(procModel);
    String procInstanceId = UUID.randomUUID().toString();
    procInstance.setProcInstanceId(procInstanceId);
    procInstance.setBizDate(bizDate);
    procInstance.setCountryCd(ctryCd);
    procInstance.setStatus(UobConstants.RUNNING);
    procInstance.setStartTime(new Timestamp(System.currentTimeMillis()));
		exportDao.insertProcessLog(procInstance);
    
    return procInstance;
  }

  /**
   * This method is used to Finalize the Export Process. It inserts the Completion entry
   * in the metadata tables.
   * @param excp The exception that was caught in the execution of the process
   * @throws EDAGMyBatisException 
   * @throws Exception when there is an error inserting into the metadata
   */
  public void finalizeExport(ProcessInstanceModel procInstance, EDAGException excp) throws EDAGMyBatisException {
    if (procInstance != null) {
    	procInstance.setStatus(excp == null ? UobConstants.SUCCESS : UobConstants.FAILURE);
    	procInstance.setEndTime(new Timestamp(System.currentTimeMillis()));
    	
    	if (excp != null) {
        procInstance.setException(excp);
    	}
      
			exportDao.updateProcessLog(procInstance);
    }
  }

  /**
   * This method is used the retrieve the Process Metadata from the Metadata tables for the
   * given arguments.
   * @param procId Process ID of the File to be Exported
   * @param bizDate Business Date for which the Process is to be executed
   * @param ctryCd Country for which the Process is to be executed
   * @return Returns a Process Model objects
   * @throws EDAGMyBatisException 
   * @throws EDAGValidationException 
   * @throws Exception when there is an error in retrieving the metadata from tables
   */
  private ProcessModel retrieveMetadata(String procId, String bizDate, String ctryCd) throws EDAGMyBatisException, EDAGValidationException {
    ProcessModel procModel = exportDao.retrieveProcessMaster(procId);
    if (procModel == null) {
      throw new EDAGValidationException(EDAGValidationException.NULL_VALUE, "Process Model", "There is no active process for Proc ID:" + procId);
    }

    procModel.setCountryAttributesMap(exportDao.retrieveProcessCountry(procId));

    // Retrieve from Export
    DestModel exportModel = exportDao.retrieveExportProcess(procId);
    
    // Retrieve from Load Params
    List<ProcessParam> procParamList = exportDao.retrieveLoadParams(procId);
    procModel.setProcParam(procParamList);
    procModel.setDestInfo(exportModel);

    return procModel;
  }

  /**
   * This method is used to run the Export process from the main method.
   * @param procId Process ID for which the file is to be exported
   * @param bizDate Business Date for which the process is to be executed
   * @param ctryCd Country code for which the process is to be executed
   * @param force Force flag applied for Export
   * @param forceFileName Names of the files which need to be force exported.
   * @throws EDAGException 
   * @throws Exception when there is any error in the Export Process
   */
  public void runExport(String procId, String bizDate, String ctryCd, boolean force, String forceFileName) throws EDAGException {
    ProcessModel procModel = null;
    ProcessInstanceModel procInstance = null;
    try {
      if (StringUtils.isEmpty(procId)) {
        throw new EDAGValidationException(EDAGValidationException.EMPTY_VALUE, "Process ID", "Process ID cannot be empty");
      }
      
      if (StringUtils.isEmpty(bizDate)) {
        throw new EDAGValidationException(EDAGValidationException.EMPTY_VALUE, "Business Date", "Business Date cannot be empty");
      }
      
      if (StringUtils.isEmpty(ctryCd)) {
        throw new EDAGValidationException(EDAGValidationException.EMPTY_VALUE, "Country Code", "Country Code cannot be empty");
      }
      
      if (StringUtils.isNotEmpty(forceFileName)) {
        logger.info("Going to process for file: " + forceFileName);
      }

      // Retrieve from Process Master
      procModel = retrieveMetadata(procId, bizDate, ctryCd);

      // Init Export
      procInstance = initExport(procModel, bizDate, ctryCd);
      EDAGException instanceException = null;
      try {
        if (!procModel.getCountryCodes().contains(ctryCd)) {
          throw new EDAGValidationException(EDAGValidationException.INVALID_VALUE, ctryCd, "Country: " + ctryCd + " is not applicable for the Process ID: " + procId);
        }

        // Check for Biz Date Expression
        String bizDateExpr = null;

        // Evaluate Business Date from Expression
        List<ProcessParam> procParamList = procModel.getProcParam();
        for (ProcessParam param : procParamList) {
          if (UobConstants.BIZ_DT_EXPR.equalsIgnoreCase(param.getParamName())) {
          	// TODO TO_DATE is Oracle-specific function
            bizDateExpr = param.getParamValue().toLowerCase().replace(UobConstants.BIZ_DATE_PARAM, "TO_DATE('" + bizDate + "', 'yyyy-MM-dd')");
          }
          
          if (UobConstants.BIZ_DT.equalsIgnoreCase(param.getParamName())) {
            bizDate = param.getParamValue();
            logger.debug("Going to use overridden business date:" + bizDate);
          }
        }

        // Evaluate Biz Date Expression
        if (bizDateExpr != null) {
          bizDate = exportDao.evaluateBizDtExpr(bizDateExpr);
          logger.debug(bizDateExpr + " business date evaluated to " + bizDate);
        }
      } catch (EDAGException excp) {
      	instanceException = excp;
      } catch (Exception excp) {
      	instanceException = new EDAGException(excp.getMessage());
      } finally {
      	if (instanceException != null) {
      		procInstance.setException(instanceException); 
          stgHndle.addStageLog(procInstance, UobConstants.STAGE_EXPORT_PROC_INIT);
          stgHndle.updateStageLog(procInstance, UobConstants.STAGE_EXPORT_PROC_INIT, null);
          throw instanceException;
      	}
      }

      String procType = procModel.getProcTypeCd();
      switch (procType) {
        case UobConstants.TPT_EXPORT_PROC_TYPE:
          new FileExportProcessor().runFileExport(procInstance, procModel, bizDate, ctryCd, force, forceFileName);
          break;
        default:
          throw new EDAGValidationException(EDAGValidationException.INVALID_VALUE, procModel.getProcTypeCd(), "Invalid process type");
      }

      // Finalize Export
      finalizeExport(procInstance, null);
    } catch (EDAGException excp) {
      finalizeExport(procInstance, excp);
      throw excp;
    }
  }
}

