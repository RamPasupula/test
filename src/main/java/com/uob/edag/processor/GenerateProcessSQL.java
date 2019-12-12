/**
 * 
 */
package com.uob.edag.processor;

import java.io.File;
import java.io.IOException;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
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
import org.apache.commons.io.FilenameUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.log4j.Logger;

import com.uob.edag.constants.UobConstants;
import com.uob.edag.exception.EDAGIOException;
import com.uob.edag.exception.EDAGProcessorException;
import com.uob.edag.exception.EDAGValidationException;
import com.uob.edag.model.ProcDownstreamAppl;
import com.uob.edag.model.ProcParam;
import com.uob.edag.model.ProcessMaster;
import com.uob.edag.utils.FileUtils;
import com.uob.edag.utils.PropertyLoader;
import com.uob.edag.utils.UobInterfaceFileParser;
import com.uob.edag.utils.UobUtils;

/**
 * @author Muhammad Bilal
 *
 */
public class GenerateProcessSQL {
	private static Logger logger = null;
	private static Options options = new Options();
	private static String sqlFileName = null;
	
	private Map<String, List<String>> sqlList = new HashMap<String, List<String>>();

	public static void main(String[] args) throws EDAGProcessorException, EDAGValidationException, EDAGIOException {
		CommandLineParser parser = new DefaultParser();
		options.addOption("h", "help", false, "Show Help");
    
	  Option regOpt = new Option("r", "register", false, "Run Registration Process for given Interface Spec");
    regOpt.setArgs(1);
    regOpt.setArgName("Interface Spec Path> <Process Spec Path");
    options.addOption(regOpt);
	
	  CommandLine command;
		try {
			command = parser.parse(options, args);
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
    if (registerArgs == null || registerArgs.length != 1) {
      throw new EDAGProcessorException(EDAGProcessorException.INCORRECT_ARGS_FOR_OPTION, "r", "Not enough arguments passed to run registration");
    }
	        
    String processSpecPath = registerArgs[0];
    if (StringUtils.isBlank(processSpecPath)) {
      throw new EDAGValidationException(EDAGValidationException.EMPTY_VALUE, "Process Spec Path", "Process Spec Path cannot be empty");
    }
    
    File processSpec = new File(processSpecPath);
    String fileName = processSpec.getName();
    String execDate = new SimpleDateFormat("yyyyMMddHHmmss").format(System.currentTimeMillis());
    String logFileName = "EDA_DL_" + fileName + "_" + execDate + ".log";
    System.setProperty("logFileName", logFileName);
    logger = Logger.getLogger(GenerateProcessSQL.class);
    UobUtils.logJavaProperties();
    UobUtils.logPackageProperties();
    
    sqlFileName = PropertyLoader.getProperty(UobConstants.SQL_FILE_NM);
    
    UobInterfaceFileParser fileParser = new UobInterfaceFileParser();
    Map<String, ProcessMaster> processMastersMap = fileParser.parseProcessSpecification(processSpecPath);
    processMastersMap = fileParser.parseProcessSpecification(processSpecPath, processMastersMap);
    
    Collection<ProcessMaster> collection = processMastersMap.values();
    Iterator<ProcessMaster> iterator = collection.iterator();
    ProcessMaster processMaster = null;
    GenerateProcessSQL processSql = new GenerateProcessSQL();
    while(iterator.hasNext()) {
    	processMaster = iterator.next();
    	processSql.generateInsertSQL(processMaster);
    }
    
    processSql.createSqlFile();
    processSql.sqlList = new HashMap<String, List<String>>();
    iterator = collection.iterator();
    while(iterator.hasNext()) {
    	processMaster = iterator.next();
    	processSql.generateDeleteSQL(processMaster);
    }
    
    processSql.createSqlFile();
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
	 * This method is used to do the actual registration for any given process.
	 * 
	 * @param procModel
	 *            The Process Model object with the metadata information to be
	 *            registered.
	 * @throws Exception
	 *             when there is an error in the Registration Process
	 */
	private void generateInsertSQL(ProcessMaster processMaster) {
		logger.info("Going to start registering process:" + processMaster.getProcNm());

		List<String> metadataInsertList = new ArrayList<String>();
		String procName = processMaster.getProcNm();
		
		metadataInsertList.add("set define off;");
		metadataInsertList.add("set sqlblanklines on;");
		metadataInsertList.add("whenever SQLERROR EXIT ROLLBACK;");

		// Insert into Process Master
		metadataInsertList.add(processMaster.getInsertProcessMasterSql());

		// Insert into Load Process
		metadataInsertList.add(processMaster.getLoadProcess().getInsertLoadProcessSql());
		
		// Insert into Source Table Detail
		metadataInsertList.add(processMaster.getSourceTableDetail().getInsertSourceTableSql());

		// Insert into Proc Params
		for (ProcParam param : processMaster.getProcParamsList()) {
			metadataInsertList.add(param.getInsertProcParamSql());
		}

		// Insert into Downstream Application
		for (ProcDownstreamAppl procAppl : processMaster.getProcApplList()) {
			metadataInsertList.add(procAppl.getInsertProcDownstreamApplSql());
		}

		String insertSqlKey = null;
		if (StringUtils.isNotEmpty(sqlFileName)) {
			insertSqlKey = sqlFileName.replace(UobConstants.SRC_SYS_PARAM, processMaster.getSrcSysCd())
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

		String action = "Registration of Process " + processMaster.getProcNm() + " with ID " + processMaster.getProcId();
		metadataInsertList.add(processMaster.getInsertAuditDetailSql(action, UobConstants.SUCCESS));

		metadataInsertList.add("COMMIT;");

		logger.info("Process Registration completed:" + processMaster.getProcNm());
	}
	
	/**
	 * This method is used to create the SQL file with the inserts for the
	 * metadata tables and DDLs for the Hive table creation.
	 * @throws EDAGIOException 
	 * 
	 * @throws IOException
	 *             when there is an error in writing the SQL file.
	 * @throws UobException
	 *             when there is custom thrown exception.
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
						throw new EDAGIOException(EDAGIOException.CANNOT_MOVE_FILE, sqlFile.getPath(), sqlArchiveFileName,  
								                      "SQL file: " + sqlFile.getPath() + " was not archived");
					}
					
					logger.debug("Existing SQL file: " + sqlFileNm + " archived successfully");
				}
			}
			
			FileUtils fileUtil = new FileUtils();
			fileUtil.writeToFile(sqlList, sqlFile);
			logger.info("New SQL file created successfully: " + fullFileName);
		}
	}
	
	/**
	 * This method is used to remove the existing metadata entries if any for
	 * the process being registered. .
	 * 
	 * @param processMaster
	 *            Process to be cleaned up
	 * @throws Exception
	 *             when there is any error in the cleanup process
	 */
	private void generateDeleteSQL(ProcessMaster processMaster) {
		logger.debug("Going to cleanup for file: " + processMaster.getProcNm());

		String procName = processMaster.getProcNm();

		String procId = procName;

		if (StringUtils.isNotEmpty(procId)) {
			// Drop Metadata Entries
			List<String> delSqlList = new ArrayList<String>();

			delSqlList.add("set define off;");
			delSqlList.add("whenever SQLERROR EXIT ROLLBACK;");

			delSqlList.add(processMaster.getRemoveProcParamSql());

			delSqlList.add(processMaster.getRemoveProcDownstreamApplSql());

			delSqlList.add(processMaster.getRemoveSourceTableDetailSql());

			delSqlList.add(processMaster.getRemoveLoadProcessSql());

			delSqlList.add(processMaster.getRemoveProcessMasterSql());

			String delSqlKey = null;
			if (StringUtils.isNotEmpty(sqlFileName)) {
				delSqlKey = sqlFileName.replace(UobConstants.SRC_SYS_PARAM, processMaster.getSrcSysCd())
															 .replace(UobConstants.FILENM_PARAM, procName)
															 .replace(UobConstants.QUERY_TYPE_PARAM, UobConstants.METADATA_DELETE);
			}

			delSqlList.add("COMMIT;");

			if (sqlList.containsKey(delSqlKey)) {
				List<String> existingSqlList = sqlList.get(procName);
				existingSqlList.addAll(delSqlList);

				sqlList.put(delSqlKey, existingSqlList);
			} else {
				sqlList.put(delSqlKey, delSqlList);
			}
		}
	}
}
