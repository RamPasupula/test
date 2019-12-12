package com.uob.edag.processor;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.io.Writer;
import java.math.BigDecimal;
import java.text.DateFormat;
import java.text.MessageFormat;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.SortedSet;
import java.util.TreeSet;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.OptionGroup;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.collections4.MapUtils;
import org.apache.commons.lang.exception.ExceptionUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.log4j.Priority;
import org.apache.velocity.VelocityContext;
import org.apache.velocity.context.Context;

import com.uob.edag.connection.TeradataConnectionFactory;
import com.uob.edag.constants.UobConstants;
import com.uob.edag.dao.CalendarDao;
import com.uob.edag.dao.ColumnMetaData;
import com.uob.edag.dao.HiveDao;
import com.uob.edag.dao.TableMetaData;
import com.uob.edag.dao.TeradataDao;
import com.uob.edag.exception.EDAGException;
import com.uob.edag.exception.EDAGIOException;
import com.uob.edag.exception.EDAGMyBatisException;
import com.uob.edag.exception.EDAGProcessorException;
import com.uob.edag.exception.EDAGSecurityException;
import com.uob.edag.exception.EDAGValidationException;
import com.uob.edag.model.BizDateRowCount;
import com.uob.edag.model.CalendarModel;
import com.uob.edag.model.ControlModel;
import com.uob.edag.model.CountryAttributes;
import com.uob.edag.model.CountryAttributes.FRREmptyBizDateControl;
import com.uob.edag.model.FieldModel;
import com.uob.edag.model.FieldModel.RecordType;
import com.uob.edag.model.FileModel;
import com.uob.edag.model.HadoopModel;
import com.uob.edag.model.LoadProcess;
import com.uob.edag.model.ProcessInstanceModel;
import com.uob.edag.model.ProcessModel;
import com.uob.edag.model.SourceTableDetail;
import com.uob.edag.model.StageModel;
import com.uob.edag.security.EncryptionUtil;
import com.uob.edag.utils.PropertyLoader;
import com.uob.edag.utils.UobUtils;
import com.uob.edag.utils.VelocityUtils;

public class FRRLoader extends BaseProcessor {
	
	public static final String MAPPER_COUNT_PROPERTY = FRRLoader.class.getName() + ".NumMappers";
	public static final int DEFAULT_MAPPER_COUNT = 2;
	public static final String LOAD_METHOD_PROPERTY = FRRLoader.class.getName() + ".LoadMethod";
	public static final String DEFAULT_LOAD_METHOD = "internal.fastload";
	public static final String MAX_COLUMN_COUNT_PROPERTY = FRRLoader.class.getName() + ".MaxColumnCount";
	public static final int DEFAULT_MAX_COLUMN_COUNT = 1900;
	public static final String SPLIT_TABLE_SUFFIX_PROPERTY = FRRLoader.class.getName() + ".TableSplitSuffix";
	public static final String DEFAULT_SPLIT_TABLE_SUFFIX = "_S${viewIndex}";
	public static final String VIEW_HASH_SUM_FIELD_NAME_PROPERTY = FRRLoader.class.getName() + ".ViewHashSumFieldName";
	public static final String DEFAULT_VIEW_HASH_SUM_FIELD_NAME = "t11_hash_sum";
	
	private class StagingResult {
		// not very nice since the cohesion between properties in this class is not strong, but it'll do for now
		private ProcessInstanceModel fiProcInstance = null;
		private boolean allowIngestionBypass = false;
		private String maxProcInstanceId = null;
		private boolean hasProcInstanceIdPartition = false;
		
		private String hashSumFieldName = null;
		private String hiveHashSum = null;
		private int hiveRowCount = -1;
		private boolean emptyStaging = false;
		
		private FileModel ingestionModel = null;
		private List<String> viewSuffixList = new ArrayList<String>();
	}
	
	private class StreamLogger implements Runnable {
		
		private Logger logger = Logger.getLogger(getClass());
		private InputStream is;
		private Priority p;
		private String name;
		
		private StreamLogger(InputStream is, Priority p, String name) {
			this.is = is;
			this.p = p;
			this.name = name;
		}

		@Override
		public void run() {
			logger.info("Start of " + name + " on thread " + Thread.currentThread().getName());
			boolean eof = false;
			try (BufferedReader reader = new BufferedReader(new InputStreamReader(is))) {
				String line = null;
				while ((line = reader.readLine()) != null) {
					logger.log(p, line);
				}
				
				eof = true;
			} catch (IOException e) {
				if (eof) {
					logger.warn("Unable to close input stream reader: " + e.getMessage());
				} else {
					logger.error("Error reading input stream: " + e.getMessage());
				}
			}
			
			logger.info("End of " + name + " on thread " + Thread.currentThread().getName());
		}
	}
	
	private static final String SUFFIX_ERR1 = "_ERR_1";
	private static final String SUFFIX_ERR2 = "_ERR_2";
	
	private TeradataDao tdDao = new TeradataDao();
	private EncryptionUtil encryptionUtil = new EncryptionUtil();
	private HiveDao hiveDao = new HiveDao();
	private CalendarDao calendarDao = new CalendarDao();
	private int maxColumnCount = DEFAULT_MAX_COLUMN_COUNT;
	private String splitTableSuffix = DEFAULT_SPLIT_TABLE_SUFFIX;
	private String viewHashSumFieldName = DEFAULT_VIEW_HASH_SUM_FIELD_NAME;
	
	public FRRLoader() {
		String prop = PropertyLoader.getProperty(MAX_COLUMN_COUNT_PROPERTY);
		this.maxColumnCount = prop == null ? DEFAULT_MAX_COLUMN_COUNT : Integer.parseInt(prop);
		
		prop = PropertyLoader.getProperty(SPLIT_TABLE_SUFFIX_PROPERTY);
		this.splitTableSuffix = prop == null ? DEFAULT_SPLIT_TABLE_SUFFIX : prop;
		
		prop = PropertyLoader.getProperty(VIEW_HASH_SUM_FIELD_NAME_PROPERTY);
		this.viewHashSumFieldName = prop == null ? DEFAULT_VIEW_HASH_SUM_FIELD_NAME : prop;
	}
	
	// TODO this is ineffective since 2 new tables are created based on the original staging table 
	//      should've created 2 staging tables based on the original T1.1 table
	//      but that requires lots of code change, so for now we build 2 extra tables on top of staging table
	private static final String STAGING_HIVE_VIEW_TEMPLATE = "CREATE TABLE ${databaseName}.${viewName} " +
	                                                         "AS " +
			                                                     "SELECT #foreach($pk in $pkList) " +
	                                                         "         ${pk.normalizedFieldName}, " +
	                                                         "       #{end} " +
			                                                     "       #foreach($field in $fieldList) " +
	                                                         "         ${field.normalizedFieldName}, " +
	                                                         "       #{end} " +
	                                                         "       $!{hashSumFieldName} as ${t11HashSumFieldName}, " +
	                                                         "       proc_instance_id, proc_ts, site_id, biz_dt, " +
	                                                         "       proc_instance_id_2, proc_ts_2 " +
	                                                         "FROM ${databaseName}.${tableName} ";
	
	/* 0 = $STG_DB_NAME
	 * 1 = $STG_TBL_NAME
	 * 2 = * for all columns, or column list if there's at least 1 column that needs to be passed through replace_char() function
	 * 3 = $PROCESS_INSTANCE_ID
	 * 4 = $PROCESS_START_TS
	 * 5 = $SRC_DB_NAME
	 * 6 = $SRC_TABLE_NAME
	 * 7 = $CTRY_CD
	 * 8 = $BIZ_DT
	 * 9 = process instance ID, if load type is HST
	 */
  // TODO change table template to Velocity as well
	private static final String STAGING_HIVE_TABLE_TEMPLATE = "CREATE TABLE {0}.{1} " + 
																														"AS " + 
																														"SELECT {2}, ''{3}'' as proc_instance_id_2, cast(''{4}'' as timestamp) as proc_ts_2 " +  
																														"FROM {5}.{6} " + 
																														"where SITE_ID = ''{7}'' " +
																														"  and BIZ_DT = ''{8}'' ";
	
	private static final MessageFormat CREATE_STAGING_HIVE_TABLE_TEMPLATE = new MessageFormat(
			STAGING_HIVE_TABLE_TEMPLATE
	); 
	
	private static final MessageFormat CREATE_STAGING_HIVE_TABLE_BY_PROC_INSTANCE_TEMPLATE = new MessageFormat(
			STAGING_HIVE_TABLE_TEMPLATE + 
			"  and PROC_INSTANCE_ID = ''{9}'' "
	);
	
	/* 0 = $TDCH_JAR
	 * 1 = $LIB_JARS
	 * 2 = jdbc:teradata://APLEDATSG90.SG.UOBNET.COM (TD JDBC URL)
	 * 3 = D01_EDW_LND_T 
	 * 4 = sysdba (TD DB username)
	 * 5 = sysdba (TD DB password, decrypted at runtime)
	 * 6 = textfile
	 * 7 = staging_d01_SG_TAS (staging schema)
	 * 8 = t_eda_TAS_CORE_BALANCE_SHEET_D (staging table name)
	 * 9 = target table name
	 * 10 = load method (-method internal.fastload / multiple.fastload / batch.insert)
	 * 11 = number of mappers (-nummappers 20)
	 */
	private static final MessageFormat TDCH_TRANSFER_DATA_TEMPLATE = new MessageFormat(
			"hadoop jar {0} com.teradata.connector.common.tool.ConnectorExportTool -libjars {1} " +
	    "-classname com.teradata.jdbc.TeraDriver -url {2}/DATABASE={3},CHARSET=UTF8 -username {4} -password {5} " +
			"-jobtype hive -fileformat {6} -sourcedatabase {7} -sourcetable {8} -targettable {9} -method {10} -nummappers {11} "
	); 
	
	private static final MessageFormat TDCH_TRANSFER_DATA_TEMPLATE_TDWALLET = new MessageFormat(
			"hadoop jar {0} com.teradata.connector.common.tool.ConnectorExportTool -Dtdch.output.teradata.jdbc.url={2}/DATABASE={3},CHARSET=UTF8 " +
	    "-Dtdch.output.teradata.jdbc.user.name={4} " +
	    "-Dtdch.output.teradata.jdbc.password={5} -libjars {1} " +
	    "-classname com.teradata.jdbc.TeraDriver " +
			"-jobtype hive -fileformat {6} -sourcedatabase {7} -sourcetable {8} -targettable {9} -method {10} -nummappers {11} "
	); 
	
	public static void main(String[] args) {
		Options options = new Options();
		
		OptionGroup grp = new OptionGroup();
		grp.addOption(Option.builder("h").desc("Show this help")
        		   											 .longOpt("help")
        														 .build());
		
		grp.addOption(Option.builder("i").numberOfArgs(2)
						  										   .argName("Process ID> <Country code")
							   										 .desc("Process ID and country code")
							   										 .required(true)
							   										 .build());
		
		options.addOptionGroup(grp);
		
		options.addOption(Option.builder("b").desc("Business date (yyyy-MM-dd)")
						 													   .hasArg()
																		 	   .argName("Business date")
				                                 .build());
		
		CommandLineParser parser = new DefaultParser();
		CommandLine cmd = null;
		try {
			cmd = parser.parse(options, args);
		} catch (ParseException e) {
			e.printStackTrace();
			showHelp(options);
			System.exit(-1);
		}
		
		if (cmd.hasOption('h')) {
			showHelp(options);
		} else if (!cmd.hasOption("i")) {
			System.out.println("Option -i (process ID and country code) is mandatory"); 
			showHelp(options);
		} else {	
			FRRLoader instance = null;
			
			String bizDate = null;
			if (cmd.hasOption("b")) {
				bizDate = cmd.getOptionValue("b"); 
			}
			String[] params = cmd.getOptionValues("i");
			String now = new SimpleDateFormat("yyyyMMddHHmmss").format(new Date());
			System.setProperty("logFileName", "EDA_LD_" + params[0] + "_" + params[1] + "_" + 
			                   (bizDate == null ? "" : bizDate + "_") + now + ".log");
			UobUtils.logJavaProperties();
			UobUtils.logPackageProperties();
      
      instance = new FRRLoader();
			try {
				instance.runLoader(params[0], params[1], bizDate);
			} catch (EDAGValidationException e) {
				String err = "Invalid argument: " + e.getMessage();
				e.printStackTrace();
				showHelp(options);
				System.exit(-1);
			} catch (EDAGException e) {
				e.printStackTrace();
				System.exit(-1);
			}
			
			System.out.println("FRR for process ID " + params[0] + " country code " + params[1] +
			                   (bizDate == null ? "" : ", business date " + bizDate + " loaded successfully"));
		}
	}

	public void runLoader(String processID, String countryCode) throws EDAGException {
		runLoader(processID, countryCode, null);
	}

	private String validateProcessID(String processID) throws EDAGValidationException {
		String procID = StringUtils.trimToEmpty(processID);
		if (StringUtils.isBlank(procID)) {
			throw new EDAGValidationException(EDAGValidationException.EMPTY_VALUE, "Process ID", "Process ID cannot be empty");
		}
		
		return procID;
	}
	
	private String validateCountryCode(String countryCode) throws EDAGValidationException {
		String country = StringUtils.trimToEmpty(countryCode);
		if (StringUtils.isBlank(country)) {
			throw new EDAGValidationException(EDAGValidationException.EMPTY_VALUE, "Country Code", "Country code cannot be null");
		}
		
		return country;
	}
	
	public String validateBusinessDate(ProcessModel processModel, String countryCode, String bizDate) throws EDAGValidationException, EDAGMyBatisException {
		String businessDate = StringUtils.trimToEmpty(bizDate);
		DateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd");
		
		if (StringUtils.isNotEmpty(businessDate)) {
			try {
				dateFormat.parse(businessDate);
			} catch (java.text.ParseException e) {
				throw new EDAGValidationException(EDAGValidationException.INVALID_DATE_FORMAT, businessDate, "yyyy_MM-dd", e.getMessage());
			}
		} else {
			String sourceSystemCode = processModel.getSrcSysCd();
			String frequency = processModel.getProcFreqCd();
			
			CalendarModel cal = calendarDao.retrieveCurrentBizDate(sourceSystemCode, countryCode, frequency);
			if (cal == null) {
				throw new EDAGValidationException(EDAGValidationException.NULL_VALUE, 
						                              "Cannot find calendar for source system " + sourceSystemCode + 
						                              ", country " + countryCode + ", frequency " + frequency);
			}
			
			businessDate = cal.getCurrBizDate();
		}
		
		return businessDate;
	}
	
	public void runLoader(String processID, String countryCode, String businessDate) throws EDAGException {
		String procID = validateProcessID(processID);
		String ctryCode = validateCountryCode(countryCode);
		ProcessModel procModel = retrieveMetadata(procID, ctryCode);
		if (procModel == null) {
			throw new EDAGValidationException(EDAGValidationException.NULL_VALUE, "Process Model", 
					                              "Process model " + procID + " cannot be retrieved from metadata store");
		}
		
		String bizDate = validateBusinessDate(procModel, countryCode, businessDate);
		
		initIngestion(procModel, bizDate, ctryCode);
		
		EDAGException ingestionException = null;
		try {
			runIngestion(procModel, countryCode, bizDate);
		} catch (EDAGException e) {
			ingestionException = e;
			throw ingestionException;
		} catch (Throwable e) {
			ingestionException = new EDAGException(e.getMessage(), e);
			throw ingestionException;
		} finally {
			ingestionException = loadStackTraceAsErrorMessageToEmptyException(ingestionException);
			finalizeIngestion(ingestionException, ingestionException == null ? UobConstants.SUCCESS : UobConstants.FAILURE);
		}
	}
	
	protected void initIngestion(ProcessModel procModel, String bizDate, String ctryCd) throws EDAGMyBatisException {
		super.initIngestion(procModel, bizDate, ctryCd);
		
		this.stgHndle.addStageLog(this.procInstance, UobConstants.STAGE_REGISTER_PROC_INIT);
		this.stgHndle.updateStageLog(this.procInstance, UobConstants.STAGE_REGISTER_PROC_INIT, null); // immediately set stage status to S
	}

	protected void runIngestion(ProcessModel procModel, String countryCode, String bizDate) throws EDAGException {
		SourceTableDetail sourceTableDetail = procModel.getSourceTableDetail();
		if (sourceTableDetail == null) {
			throw new EDAGValidationException(EDAGValidationException.NULL_VALUE, "Source table detail", "Cannot get source table detail for process ID " + procModel.getProcId());
		}
		
		LoadProcess loadProcess = procModel.getLoadProcess();
		if (loadProcess == null) {
			String err = "Load process in process model is null";
			logger.error(err);
			throw new EDAGValidationException(EDAGValidationException.NULL_VALUE, "Load process", "Cannot get load process for process ID " + procModel.getProcId());
		}
		
		HadoopModel hadoopModel = procModel.getDestInfo();
		
		StageModel registerStage = this.stgHndle.addStageLog(this.procInstance, UobConstants.STAGE_REGISTER_PROC_HIVE_TABLE);
		EDAGException instanceException = null;
		StagingResult stagingResult = null;
		boolean stagingValid = false;
		try {
			// get attributes for this process / country
			CountryAttributes.FRREmptyBizDateControl emptyBizDateControl = CountryAttributes.FRREmptyBizDateControl.NORMAL;
			Map<String, CountryAttributes> attrs = ingestDao.retrieveProcessCountry(procModel.getProcId());
			if (MapUtils.isNotEmpty(attrs)) {
				CountryAttributes attr = attrs.get(countryCode);
				if (attr != null) {
					emptyBizDateControl = attr.getFRRLoaderEmptyBizDateControl();
				}
			}
			registerStage.setFRREmptyBizDateControl(emptyBizDateControl.name());
			
			SortedSet<BizDateRowCount> bizDateRowCountSet = new TreeSet<BizDateRowCount>();
			String bizDateOverwrite = null;
			if (emptyBizDateControl == FRREmptyBizDateControl.PREVIOUS || emptyBizDateControl == FRREmptyBizDateControl.PREVIOUS_THEN_FAIL) {
				// get row counts for this process + site ID
				bizDateRowCountSet = hiveDao.getBizDateRowCountByCountry(sourceTableDetail.getSrcSchemaNm(), sourceTableDetail.getSrcTblNm(), 
				                                                         countryCode, bizDate, hadoopModel.getHadoopQueueName()); 
				// since we're going to look for a previous date, the records from previous date run which go into staging table need to have the supplied business date
				bizDateOverwrite = bizDate;
		  }
		  
			String activeBizDate = bizDate;
			do {
				// create staging table
				dropStagingHiveTable(loadProcess);
				stagingResult = createStagingHiveTable(sourceTableDetail, loadProcess, countryCode, activeBizDate, bizDateOverwrite, hadoopModel.getHadoopQueueName());
				
				// determine if the staging table is valid
				if (!stagingResult.emptyStaging || emptyBizDateControl == FRREmptyBizDateControl.NORMAL) {
					stagingValid = true;
				} else if (emptyBizDateControl == FRREmptyBizDateControl.FAIL) {
					// staging table is empty and we don't allow FRRLoad job that has empty dataset to succeed
				  throw new EDAGValidationException(EDAGValidationException.EMPTY_HIVE_TABLE, sourceTableDetail.getSrcSchemaNm() + "." + sourceTableDetail.getSrcTblNm(), countryCode, bizDate);
				} else if (emptyBizDateControl == FRREmptyBizDateControl.SKIP) {
					logger.debug(bizDate + " doesn't have any data, empty biz date control is set to SKIP, skipping the rest of the operations");
					break; // get out of the loop, skip the rest of the operations
				} else {
					try {
						// get previous biz date and try creating staging table again
						com.uob.edag.model.BizDateRowCount previousBizDateRowCount = bizDateRowCountSet.first();
						bizDateRowCountSet.remove(previousBizDateRowCount);
						activeBizDate = previousBizDateRowCount.getBizDate();
					} catch (NoSuchElementException e) {
						if (emptyBizDateControl == FRREmptyBizDateControl.PREVIOUS_THEN_FAIL) {
							throw new EDAGValidationException(EDAGValidationException.EMPTY_HIVE_TABLE, sourceTableDetail.getSrcSchemaNm() + "." + sourceTableDetail.getSrcTblNm(), countryCode, bizDate);
						}
						
						// previous biz date set is empty
						logger.debug("Cannot find business date prior to " + bizDate + " which has data for process " + 
						             this.procInstance.getProcId() + ", country " + countryCode);
						break; // get out of while loop
					}
				} 
			} while (!stagingValid);
			
			// set actual biz date used
			if (stagingValid) {
				registerStage.setActualBizDate(activeBizDate);
				logger.debug("Actual business date used is " + activeBizDate);
			}
			
			if (!stagingResult.emptyStaging) {
				retrieveReconciliationAttributes(sourceTableDetail, countryCode, activeBizDate, hadoopModel.getHadoopQueueName(), stagingResult);
			}
		} catch (EDAGException e) {
			instanceException = e;
			throw instanceException;
		} catch (Throwable e) {
			instanceException = new EDAGException(e.getMessage(), e);
			throw instanceException;
		} finally {
			if (instanceException != null) {
				instanceException = loadStackTraceAsErrorMessageToEmptyException(instanceException);
				this.procInstance.setException(instanceException);
			}
			
			this.stgHndle.updateStageLog(this.procInstance, UobConstants.STAGE_REGISTER_PROC_HIVE_TABLE, registerStage, null);
		}
		
		this.stgHndle.addStageLog(this.procInstance, UobConstants.STAGE_REGISTER_PROC_TDCH_INSERT);
		instanceException = null;
		try {
			if (stagingValid) {
				runHousekeepingInTD(loadProcess);
				if (!stagingResult.emptyStaging) {
					prepareSourceTable(loadProcess, stagingResult);
					transferData(loadProcess, countryCode, stagingResult);
				}
			} else {
				logger.info("Not truncating Teradata table and not calling TDCH since Hive table is empty for biz date " + bizDate + 
						        " and empty business date control flag is set to SKIP or PREVIOUS");
			}
		} catch (EDAGException e) {
			instanceException = e;
			throw instanceException;
		} catch (Throwable e) {
			instanceException = new EDAGException(e.getMessage());
			throw instanceException;
		} finally {
			if (instanceException != null) {
				instanceException = loadStackTraceAsErrorMessageToEmptyException(instanceException);
				this.procInstance.setException(instanceException);
			}
			
			this.stgHndle.updateStageLog(this.procInstance, UobConstants.STAGE_REGISTER_PROC_TDCH_INSERT, null);
		}
		
		this.stgHndle.addStageLog(this.procInstance, UobConstants.STAGE_REGISTER_PROC_TECH_RECON);
		instanceException = null;
		try {
			if (stagingValid && !stagingResult.emptyStaging) {
				reconcile(sourceTableDetail, hadoopModel.getHadoopQueueName(), loadProcess, countryCode, bizDate, stagingResult);
			} else {
				logger.info("Skipping reconciliation since TDCH is not called due to empty Hive table or empty business date control flag is set to SKIP or PREVIOUS");
			}
		} catch (EDAGException e) {
			instanceException = e;
			throw instanceException;
		} catch (Throwable e) {
			instanceException = new EDAGException(e.getMessage(), e);
			throw instanceException;
		} finally {
			if (instanceException != null) {
				String msg = instanceException.getTruncatedMessage();
				if (StringUtils.isBlank(msg)) {
					boolean closingStringWriter = false;
					try (StringWriter strWriter = new StringWriter()) {
						try (PrintWriter writer = new PrintWriter(strWriter)) {
							instanceException.printStackTrace(writer);
							msg = strWriter.getBuffer().toString();
						}
						
						closingStringWriter = true;
					} catch (IOException e) {
						if (closingStringWriter) {
							logger.warn("Unable to close StringWriter: " + e.getMessage());
						} else {
							msg = instanceException.getCause().getClass().getName() + " has occured. No other information is available";
						}
					}
					
					this.procInstance.setException(new EDAGException(msg));
				} else {
					instanceException = loadStackTraceAsErrorMessageToEmptyException(instanceException);
					this.procInstance.setException(instanceException);
				}
			}
			stgHndle.updateStageLog(procInstance, UobConstants.STAGE_REGISTER_PROC_TECH_RECON, null);
		}
		
		logger.info(procModel.getProcId() + " for " + countryCode + ", business date " + bizDate + " completed successfully");
	}
	
	private void prepareSourceTable(LoadProcess loadProcess, StagingResult stagingResult) throws EDAGMyBatisException {
		if (stagingResult.ingestionModel != null) {
			List<FieldModel> fieldList = stagingResult.ingestionModel.getSrcFieldInfo();
			if (fieldList != null) {
				Context ctx = new VelocityContext();
				SortedSet<FieldModel> pkFields = new TreeSet<FieldModel>();
				Map<String, SortedSet<FieldModel>> splitTableMap = new HashMap<String, SortedSet<FieldModel>>();
				int counter = this.maxColumnCount;
				int viewIndex = 0;
				SortedSet<FieldModel> currentFieldSet = null;
				
				for (FieldModel field : fieldList) {
					counter++;
					if (counter > this.maxColumnCount) {
						viewIndex++;
						ctx.put("viewIndex", viewIndex);
						Writer writer = new StringWriter();
						VelocityUtils.evaluate(ctx, writer, "Evaluating view suffix for " + loadProcess.getStgTblNm() + ", view index = " + viewIndex, this.splitTableSuffix);
						String viewSuffix = writer.toString();
						currentFieldSet = new TreeSet<FieldModel>();
						splitTableMap.put(viewSuffix, currentFieldSet);
						counter -= this.maxColumnCount;
					} 
					
					if (field.getRecordType() == RecordType.PRIMARY_KEY) {
						pkFields.add(field);
					} else if (field.getRecordType().isData()) {
						currentFieldSet.add(field);
					}
				}
				
				if (splitTableMap.size() > 1) {
					logger.debug("Splitting table " + loadProcess.getStgTblNm() + " into " + splitTableMap.size() + " views");
					for (String viewSuffix : splitTableMap.keySet()) {
					  // drop the table first since now we use table instead of view
						hiveDao.dropHiveTable(loadProcess.getStgDbNm(), loadProcess.getStgTblNm() + viewSuffix);
						
						// create split staging table
						ctx = new VelocityContext();
						ctx.put("pkList", pkFields);
						ctx.put("fieldList", splitTableMap.get(viewSuffix));
						ctx.put("hashSumFieldName", StringUtils.trimToNull(stagingResult.hashSumFieldName) == null ? "''" : stagingResult.hashSumFieldName);
						ctx.put("t11HashSumFieldName", this.viewHashSumFieldName);
						ctx.put("databaseName", loadProcess.getStgDbNm());
						ctx.put("tableName", loadProcess.getStgTblNm());
						ctx.put("viewName", loadProcess.getStgTblNm() + viewSuffix);
						Writer writer = new StringWriter();
						VelocityUtils.evaluate(ctx, writer, "Creating table DDL for " + loadProcess.getStgTblNm() + viewSuffix, STAGING_HIVE_VIEW_TEMPLATE);
						hiveDao.createHiveTable(writer.toString());
						stagingResult.viewSuffixList.add(viewSuffix);
					}
				} 
			}
		}
	}

	// TODO ugly, staging result has become an arbitrary object containing irrelevant information. Refactor this later
	private void retrieveReconciliationAttributes(SourceTableDetail sourceTable, String countryCode, String bizDate, 
			                                          String hadoopQueueName, StagingResult stagingResult) throws EDAGException {
		// get load process from the ingestion process that writes into the source Hive table
		HadoopModel ingestionProcess = stagingResult.fiProcInstance != null ? ingestDao.retrieveLoadProcess(stagingResult.fiProcInstance.getProcId())
				                                                                : ingestDao.retrieveLoadProcessByTargetTable(sourceTable.getSrcSchemaNm(), sourceTable.getSrcTblNm());
		if (ingestionProcess == null) {
			logger.warn("Unable to get ingestion load process that populates " + sourceTable.getSrcSchemaNm() + "." + sourceTable.getSrcTblNm());
			return;
		}
		
	  // 1st pass, search from stage log
		// get stage log of the ingestion process that writes into Hive
		if (stagingResult.fiProcInstance != null) {
			List<StageModel> stages = ingestDao.getStageInfo(stagingResult.fiProcInstance);
			for (StageModel stage : stages) {
				if (UobConstants.STAGE_INGEST_PROC_FINAL.equals(stage.getStageId())) {
					stagingResult.hashSumFieldName = stage.getHashsumFld();
					stagingResult.hiveHashSum = stage.getDestHashsumAmt() == null ? null : stage.getDestHashsumAmt().toPlainString();
					stagingResult.hiveRowCount = stage.getDestRowCount();
				}
			}
		} else if (stagingResult.allowIngestionBypass) {
			stagingResult.hiveRowCount = hiveDao.getRowCount(sourceTable.getSrcSchemaNm(), sourceTable.getSrcTblNm(), countryCode, 
			                              							     bizDate, stagingResult.hasProcInstanceIdPartition ? stagingResult.maxProcInstanceId : null, hadoopQueueName);
		} else {
			stagingResult.hiveRowCount = 0;
		}
		
		if (stagingResult.hashSumFieldName == null) {
			// 2nd pass, search from metadata
			// get file detail from process ID of the ingestion process that writes into Hive
			stagingResult.ingestionModel = this.retrieveFileModel(ingestionProcess.getProcessId());
			if (stagingResult.ingestionModel != null) {
				FieldModel hashSumField = stagingResult.ingestionModel.getHashSumField();
				if (hashSumField != null) {
					stagingResult.hashSumFieldName = hashSumField.getNormalizedFieldName();
				}
			}
		}
		
		if (stagingResult.hashSumFieldName == null) {
			logger.debug(sourceTable.getSrcSchemaNm() + "." + sourceTable.getSrcTblNm() + 
					         " doesn't have hash sum field, skipping hash sum reconciliation");
			return;
		}
		
		if (stagingResult.hiveHashSum == null) {
			if (stagingResult.fiProcInstance == null && !stagingResult.allowIngestionBypass) {
				stagingResult.hiveHashSum = "0";
			} else {
				String procInstanceId = stagingResult.fiProcInstance != null ? stagingResult.fiProcInstance.getProcInstanceId()
           		                                                       : stagingResult.maxProcInstanceId;
				// hive hash sum cannot be retrieved from stage log
			  stagingResult.hiveHashSum = hiveDao.getHashSum(sourceTable.getSrcSchemaNm(), sourceTable.getSrcTblNm(), 
				  	 					              									 countryCode, bizDate, stagingResult.hasProcInstanceIdPartition ? procInstanceId 
				  	 					              											                                                            : null, stagingResult.hashSumFieldName, hadoopQueueName);
			}
		}
		
		// hive hash sum cannot be retrieved from hive table as well
		if (stagingResult.hiveHashSum == null) {
			stagingResult.hiveHashSum = "0";
		}
	}

	private void reconcile(SourceTableDetail sourceTable, String hadoopQueueName, LoadProcess loadProcess, 
			                   String countryCode, String bizDate, StagingResult stagingResult) throws EDAGException {
		ControlModel controlModel = this.procInstance.getControlModel();
		if (controlModel == null) {
			controlModel = new ControlModel();
		  controlModel.setBizDate(bizDate);
		  controlModel.setCtryCd(countryCode);
		  controlModel.setSrcSystemCd(this.procInstance.getSourceSystemId());
			this.procInstance.setControlModel(controlModel);
		}
		
		// if hive row count is never set, it means retrieveReconciliationAttributes() method call terminates early
		if (stagingResult.hiveRowCount < 0) {
			logger.debug("Unable to find ingestion process corresponding to " + loadProcess.getProcId() + ", skipping reconciliation");
			return;
		}
		
		controlModel.setTotalRecords(stagingResult.hiveRowCount);
		
		String[] suffixes = stagingResult.viewSuffixList.isEmpty() ? new String[] {""}
				                                                       : stagingResult.viewSuffixList.toArray(new String[stagingResult.viewSuffixList.size()]);
		
		for (String suffix : suffixes) {
			// find TD row count
			String tblName = loadProcess.getTgtTblNm(true) + suffix;
			int tdRowCount = tdDao.getRowCount(tblName);
			controlModel.setTotalRecordsTarget(tdRowCount);
			
			// find TD error row count
			controlModel.setETErrorRecordCount(tdDao.getRowCount(tblName + SUFFIX_ERR1, false));
			controlModel.setUVErrorRecordCount(tdDao.getRowCount(tblName + SUFFIX_ERR2, false));
			int tdErrorRowCount = controlModel.getETErrorRecordCount() + controlModel.getUVErrorRecordCount();
			controlModel.setTotalErrRecordsTarget(tdErrorRowCount);
			
			double errorThreshold = loadProcess.getErrThreshold();
			String info = "Hive row count: " + stagingResult.hiveRowCount + ", TD row count: " + tdRowCount + 
	        					", TD error row count: " + tdErrorRowCount + ", error threshold: " + errorThreshold;
			logger.info(info);
			
			// logic copied from ReconciliationUtil
			boolean reconciled = true;
			if (errorThreshold < 1.0) {
				reconciled = stagingResult.hiveRowCount == 0 ? (tdErrorRowCount == 0)
						                                         : Integer.valueOf(tdErrorRowCount).doubleValue() / Integer.valueOf(stagingResult.hiveRowCount).doubleValue() <= errorThreshold;
			} else {
				reconciled = tdErrorRowCount <= errorThreshold;
			}
			
			if (!reconciled) {
				throw new EDAGValidationException(EDAGValidationException.ERROR_COUNT_ABOVE_THRESHOLD, 
						                              this.procInstance.getProcInstanceId(), tdErrorRowCount, errorThreshold, stagingResult.hiveRowCount);
			}
			
			boolean skipHashSumReconciliation = UobUtils.parseBoolean(PropertyLoader.getProperty(UobConstants.T14_SKIP_HASHSUM_RECONCILIATION));
			if (!skipHashSumReconciliation) {
				if (stagingResult.hashSumFieldName == null) {
					logger.debug(sourceTable.getSrcSchemaNm() + "." + sourceTable.getSrcTblNm() + " doesn't have hash sum field, skipping hash sum reconciliation");
					return;
				}
				
				controlModel.setHashSumCol(stagingResult.hashSumFieldName);
				controlModel.setHashSumVal(stagingResult.hiveHashSum);
				
			  // get TD hash sum
				String tdHashSum;
				String hashSumFieldName = "".equals(suffix) ? stagingResult.hashSumFieldName : this.viewHashSumFieldName;
				if (stagingResult.hiveHashSum.indexOf(".") > -1) {
					tdHashSum = tdDao.getSumOfHashSumAsString(tblName, hashSumFieldName, stagingResult.hiveHashSum.length() - 1);
				} else {
					tdHashSum = tdDao.getSumOfHashSumAsString(tblName, hashSumFieldName, stagingResult.hiveHashSum.length());
				}
				controlModel.setHashSumValTarget(tdHashSum);
				
				logger.debug("Source hash sum: " + stagingResult.hiveHashSum + ", target hash sum: " + tdHashSum);
				if (new BigDecimal(stagingResult.hiveHashSum).compareTo(new BigDecimal(tdHashSum)) != 0) {
					throw new EDAGValidationException(EDAGValidationException.HASH_SUM_RECONCILIATION_FAILURE, 
							                              this.procInstance.getProcInstanceId(), stagingResult.hiveHashSum, tdHashSum);
				}
			}
		}
	}
	
	/* 0 = $TDCH_JAR
	 * 1 = $LIB_JARS
	 * 2 = jdbc:teradata://APLEDATSG90.SG.UOBNET.COM (TD JDBC URL)
	 * 3 = D01_EDW_LND_T (TD schema)
	 * 4 = sysdba (TD DB username)
	 * 5 = sysdba (TD DB password, decrypted at runtime)
	 * 6 = textfile
	 * 7 = staging_d01_SG_TAS (staging schema)
	 * 8 = t_eda_TAS_CORE_BALANCE_SHEET_D (staging table)
	 * 9 = target table name
	 * 10 = method
	 * 11 = nummappers
	 */
	private String getTDCHTransferDataCommand(LoadProcess loadProcess, String countryCode, String suffix, boolean maskPassword) throws EDAGSecurityException {
		boolean useTDWallet;
		try {
			useTDWallet = UobUtils.parseBoolean(PropertyLoader.getProperty(UobConstants.TDCH_USE_TDWALLET));
		} catch (EDAGValidationException e) {
			logger.debug("Forcing to use TD Wallet since " + UobConstants.TDCH_USE_TDWALLET + " property cannot be retrieved: " + e.getMessage());
			useTDWallet = true;
		}
		
		MessageFormat template = useTDWallet ? TDCH_TRANSFER_DATA_TEMPLATE_TDWALLET : TDCH_TRANSFER_DATA_TEMPLATE;
		String username = useTDWallet ? PropertyLoader.getProperty(UobConstants.TERADATA_JDBC_USER_TDWALLET, true) : PropertyLoader.getProperty(UobConstants.TERADATA_JDBC_USER, true);
    String password = maskPassword ? "xxx" : (useTDWallet ? PropertyLoader.getProperty(UobConstants.TERADATA_JDBC_PASSWORD_TDWALLET) : PropertyLoader.getProperty(UobConstants.TERADATA_JDBC_PASSWORD));
		
		if (!maskPassword && !password.startsWith(TeradataConnectionFactory.ENCRYPTED_PASSWORD_PREFIX) && 
				!password.contains(TeradataConnectionFactory.TD_WALLET_PREFIX)) {
			password = encryptionUtil.decrypt(password);
		}
		
		String loadMethod = PropertyLoader.getProperty(LOAD_METHOD_PROPERTY);
		if (StringUtils.isBlank(loadMethod)) {
			loadMethod = DEFAULT_LOAD_METHOD;
		}
		
		String mapperCountStr = PropertyLoader.getProperty(MAPPER_COUNT_PROPERTY);
		int mapperCount = StringUtils.isBlank(mapperCountStr) ? DEFAULT_MAPPER_COUNT : Integer.parseInt(mapperCountStr);
		
		try {
			Map<String, CountryAttributes> countryAttrMap = this.ingestDao.retrieveProcessCountry(loadProcess.getProcId());
			CountryAttributes countryAttr = MapUtils.getObject(countryAttrMap, countryCode, null);
			if (countryAttr != null && countryAttr.getFRRLoaderMapperCount() > 0) {
				mapperCount = countryAttr.getFRRLoaderMapperCount();
				logger.debug("Mapper count for process " + loadProcess.getProcId() + ", country " + countryCode + " is " + mapperCount);
			}
		} catch (EDAGMyBatisException e) {
			logger.warn("Unable to get countries for process " + loadProcess.getProcId() + ": " + e.getMessage());
		}
		
		
		return template.format(new Object[] {PropertyLoader.getProperty(UobConstants.TDCH_JAR, true),
									 											 PropertyLoader.getProperty(UobConstants.TDCH_LIB_JARS, true),
																				 PropertyLoader.getProperty(UobConstants.TERADATA_JDBC_URL, true),
																				 loadProcess.getTgtDbNm(),
																				 username,
																				 password,
																				 PropertyLoader.getProperty(UobConstants.T14_FILE_FORMAT, true),
																				 loadProcess.getStgDbNm(),
																				 loadProcess.getStgTblNm() + suffix,
																				 loadProcess.getTgtTblNm() + suffix,
																				 loadMethod,
																				 mapperCount
    });
	}
	
	private void transferData(LoadProcess loadProcess, String countryCode, StagingResult stagingResult) throws EDAGException {
		System.setProperty(UobConstants.HADOOP_CLASSPATH, PropertyLoader.getProperty(UobConstants.HADOOP_CLASSPATH, true));
		
		String[] suffixes = stagingResult.viewSuffixList.isEmpty() ? new String[] {""}
                                                               : stagingResult.viewSuffixList.toArray(new String[stagingResult.viewSuffixList.size()]);
		for (String suffix : suffixes) {
			String command = getTDCHTransferDataCommand(loadProcess, countryCode, suffix, false);
			String maskedCommand = getTDCHTransferDataCommand(loadProcess, countryCode, suffix, true);
			logger.info("Executing ConnectorExportTool to transfer " + loadProcess.getStgDbNm() + "." + loadProcess.getStgTblNm() + suffix + 
	                " to " + loadProcess.getTgtDbNm() + "." + loadProcess.getTgtTblNm() + suffix);
			try {
				Process p = Runtime.getRuntime().exec(command);
				new Thread(new StreamLogger(p.getInputStream(), Level.INFO, "process output")).start();
				new Thread(new StreamLogger(p.getErrorStream(), Level.INFO, "process error")).start();
				
				int exitCode = p.waitFor();
				
				if (exitCode != 0) {
					throw new EDAGProcessorException(EDAGProcessorException.EXT_CMD_ERROR, maskedCommand, exitCode);
				} else {
					logger.info(maskedCommand + " executed successfully");
				}
			} catch (IOException | InterruptedException e) {
				throw new EDAGIOException(EDAGIOException.CANNOT_EXEC_CMD, maskedCommand, e.getMessage());
			}  
		}
	} 

	private void runHousekeepingInTD(LoadProcess loadProcess) throws EDAGMyBatisException {
		String tableName = loadProcess.getTgtTblNm(true);
		tdDao.dropTable(tableName + SUFFIX_ERR1);
		tdDao.dropTable(tableName + SUFFIX_ERR2);
		
		int index = -1;
		boolean truncated = false;
		Context ctx = new VelocityContext();
		do {
			index++;
			String splitTableName = index == 0 ? tableName : tableName + this.splitTableSuffix;
			ctx.put("viewIndex", index);
			Writer writer = new StringWriter();
			VelocityUtils.evaluate(ctx, writer, "Getting table name with suffix index " + index, splitTableName);
			splitTableName = writer.toString();
			
			try {
				truncated = tdDao.truncateTable(splitTableName);
			} catch (EDAGMyBatisException e) {
				if (StringUtils.trimToEmpty(e.getMessage()).toLowerCase().contains("is being loaded")) {
					logger.warn(splitTableName + " cannot be truncated because it is being loaded (most likely by previous process). Dropping and recreating the table");
					String ddl = tdDao.showTable(splitTableName);
					tdDao.dropTable(splitTableName);
					tdDao.executeUpdate(ddl, null);
					truncated = true;
				} else {
					throw e;
				}
			}
			
			tdDao.dropTable(splitTableName + SUFFIX_ERR1);
			tdDao.dropTable(splitTableName + SUFFIX_ERR2);
		} while (truncated || index == 0);
	}

	/* 0 = $STG_DB_NAME
	 * 1 = $STG_TBL_NAME
	 * 2 = * for all columns, or column list if there's at least 1 column that needs to be passed through replace_char() function
	 * 3 = $PROCESS_INSTANCE_ID
	 * 4 = $PROCESS_START_TS
	 * 5 = $SRC_DB_NAME
	 * 6 = $SRC_TABLE_NAME
	 * 7 = $CTRY_CD
	 * 8 = $BIZ_DT
	 * 9 = process instance ID
	 */
	private StagingResult createStagingHiveTable(SourceTableDetail sourceTableDetail, LoadProcess loadProcess,
			                                         String countryCode, String bizDate, String bizDateOverwrite, String queue) throws EDAGException {
		logger.debug("Creating staging Hive table for process " + loadProcess.getProcId() + ", country " + countryCode + ", biz date " + bizDate);
		String statement = null;
		StagingResult result = new StagingResult();
		result.allowIngestionBypass = UobUtils.parseBoolean(PropertyLoader.getProperty(UobConstants.T14_ALLOW_INGESTION_BYPASS));
		
		// check source table detail partitions first
		List<Map<String, String>> partitionList = hiveDao.getTablePartitions(sourceTableDetail.getSrcSchemaNm() + "." + sourceTableDetail.getSrcTblNm());
		if (CollectionUtils.isNotEmpty(partitionList)) {
			Map<String, String> partitionMap = partitionList.get(0);
			for (String key : partitionMap.keySet()) {
				if ("proc_instance_id".equalsIgnoreCase(key)) {
					result.hasProcInstanceIdPartition = true;
					logger.debug(sourceTableDetail.getSrcSchemaNm() + "." + sourceTableDetail.getSrcTblNm() + " has process instance ID as partition. Records from the latest process instance will copied to staging table");
					break;
				}
			}
		}
		
		result.fiProcInstance = ingestDao.getDLIngestionInstanceByEDWLoadInstance(this.procInstance, bizDate, true);
		
		if (result.fiProcInstance != null || result.allowIngestionBypass) {
			if (result.fiProcInstance == null) {
				logger.debug("Ingestion for " + loadProcess.getProcId() + " (" + countryCode + ", " + bizDate + ") not found, but ingestion bypass flag is set to true");
			}
			
			if (result.hasProcInstanceIdPartition) {
				result.maxProcInstanceId = hiveDao.getMaxProcInstanceId(sourceTableDetail.getSrcSchemaNm() + "." + sourceTableDetail.getSrcTblNm(), countryCode, bizDate);
				String procInstanceIdToUse = result.fiProcInstance == null ? result.maxProcInstanceId : result.fiProcInstance.getProcInstanceId();
				statement = CREATE_STAGING_HIVE_TABLE_BY_PROC_INSTANCE_TEMPLATE.format(new Object[] {loadProcess.getStgDbNm(), 
																																										         loadProcess.getStgTblNm(),
																																										         getColumnList(this.procInstance.getProcId(), countryCode, sourceTableDetail, null, bizDateOverwrite),
																																										         procInstance.getProcInstanceId(),
																																										         new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(procInstance.getStartTime()),
																																										         sourceTableDetail.getSrcSchemaNm(),
																																										         sourceTableDetail.getSrcTblNm(),
																																										         countryCode, 
																																										         bizDate,
																																										         procInstanceIdToUse});
				result.emptyStaging = hiveDao.getRowCount(sourceTableDetail.getSrcSchemaNm(), sourceTableDetail.getSrcTblNm(), 
						                                      countryCode, bizDate, procInstanceIdToUse, queue) == 0;
			} else {
				statement = CREATE_STAGING_HIVE_TABLE_TEMPLATE.format(new Object[] {loadProcess.getStgDbNm(), 
																																            loadProcess.getStgTblNm(),
																																            getColumnList(procInstance.getProcId(), countryCode, sourceTableDetail, null, bizDateOverwrite),
																																	          procInstance.getProcInstanceId(),
																																	          new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(procInstance.getStartTime()),
																																	          sourceTableDetail.getSrcSchemaNm(),
																																	          sourceTableDetail.getSrcTblNm(),
																																	          countryCode, 
																																	          bizDate});
				result.emptyStaging = hiveDao.getRowCount(sourceTableDetail.getSrcSchemaNm(), sourceTableDetail.getSrcTblNm(), 
						                                      countryCode, bizDate, null, queue) == 0;
			}
		} else {
			logger.debug("Ingestion for " + loadProcess.getProcId() + " (" + countryCode + ", " + bizDate + ") not found, and ingestion is not allowed to be bypassed. Creating empty staging Hive table");
			statement = CREATE_STAGING_HIVE_TABLE_BY_PROC_INSTANCE_TEMPLATE.format(new Object[] {loadProcess.getStgDbNm(), 
																																									         loadProcess.getStgTblNm(),
																																									         getColumnList(this.procInstance.getProcId(), countryCode, sourceTableDetail, null, bizDateOverwrite),
																																									         procInstance.getProcInstanceId(),
																																									         new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(procInstance.getStartTime()),
																																									         sourceTableDetail.getSrcSchemaNm(),
																																									         sourceTableDetail.getSrcTblNm(),
																																									         countryCode, 
																																									         bizDate,
																																									         null});
			result.emptyStaging = true;
		}
		 
		if (!result.emptyStaging) {
			hiveDao.createHiveTable(statement);
			logger.debug("Hive staging table is created for country " + countryCode + ", biz date " + bizDate + 
					         (result.fiProcInstance == null ? "" : ", ingestion proc instance ID " + result.fiProcInstance.getProcInstanceId()));
		} else {
			logger.debug("Not creating staging table since there's no data for country " + countryCode + ", biz date " + bizDate + 
					         (result.fiProcInstance == null ? "" : ", ingestion proc instance ID " + result.fiProcInstance.getProcInstanceId()));
		}
		return result;
	}
	
	private Object getColumnList(String procId, String countryCode, SourceTableDetail sourceTableDetail, String prefix, String bizDateOverwrite) throws EDAGMyBatisException {
		logger.debug("Getting column list for process " + procId + ", country " + countryCode + ", source table " + sourceTableDetail);
		String colList = prefix == null ? "*" : prefix + ".*";
		String sourceTable = sourceTableDetail.getSrcSchemaNm() + "." + sourceTableDetail.getSrcTblNm();
		
		Map<String, Character> replacementMap = ingestDao.getTDUnsupportedCharacterReplacement(procId, countryCode);
		if (MapUtils.isNotEmpty(replacementMap) || StringUtils.isNotBlank(bizDateOverwrite)) {
			String udf = PropertyLoader.getProperty("T14_REPLACE_UNSUPPORTED_CHAR_FUNCTION");
			StringBuilder sb = null;
			TableMetaData tableMeta = hiveDao.getTableMetadata(sourceTable);
			if (tableMeta != null) {
				for (ColumnMetaData colMeta : tableMeta.getColumnMetadata()) {
					if (sb == null) {
						sb = new StringBuilder();
					} else {
						sb.append(", ");
					}
					
					String key = colMeta.getName().toLowerCase();
					if ("biz_dt".equals(key) && StringUtils.isNotBlank(bizDateOverwrite)) {
						sb.append("'" + bizDateOverwrite + "' as biz_dt");
					} else {
						sb.append(replacementMap.containsKey(key) ? udf + "(" + (prefix == null ? key : prefix + "." + key) + ") as " + key 
								                                      : (prefix == null ? key : prefix + "." + key));
					}
				}
			}
			
			if (sb != null && sb.length() > 0) {
				colList = sb.toString();
			}
		}
		
		logger.debug("Column list for proc " + procId + ", country " + countryCode + ", source table " + sourceTable + ": " + colList);
		return colList;
	}

	private void dropStagingHiveTable(LoadProcess loadProcess) throws EDAGMyBatisException {
		hiveDao.dropHiveTable(loadProcess.getStgDbNm(), loadProcess.getStgTblNm());
	}

	private static void showHelp(Options options) {
		HelpFormatter format = new HelpFormatter();
		format.printHelp(FRRLoader.class.getName(), options, true);
	}
	
	public void runIngestion(String procId, String bizDate, String ctryCd, boolean force,
			                     String forceFileName) {
		throw new UnsupportedOperationException("runIngestion(String, String, String, boolean, String) is not supported by " + getClass().getName());
	}
	
	protected EDAGException loadStackTraceAsErrorMessageToEmptyException(EDAGException e) {
		if (e == null || (e.getMessage() != null && !e.getMessage().trim().isEmpty()))
			return e;
		else {
			String errorMessage = ExceptionUtils.getRootCauseMessage(e);
			EDAGException ret = null;
			if (errorMessage == null || errorMessage.trim().isEmpty()) {
				if (e.getCause() != null && e.getCause() instanceof NullPointerException) {
					errorMessage = "Caused by :" + e.getCause().getClass().getName()
							+ ". Check logs for more info.";
				} else {
					errorMessage = ExceptionUtils.getStackTrace(e);
				}
			}
			if (errorMessage != null && errorMessage.trim().endsWith(":")) {
				errorMessage = StringUtils.removeEnd(errorMessage.trim(), ":") + " - " + " Check logs for more info.";
			}
			ret = new EDAGException(errorMessage, e);
			return ret;
		}
	}
}
