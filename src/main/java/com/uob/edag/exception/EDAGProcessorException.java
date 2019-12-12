package com.uob.edag.exception;

public class EDAGProcessorException extends EDAGException {
	
	public static final PredefinedException CANNOT_PARSE_CLI_OPTIONS = new PredefinedException(EDAGProcessorException.class, "CANNOT_PARSE_CLI_OPTIONS", "Unable to parse command line options: {0}");
	public static final PredefinedException CANNOT_INGEST = new PredefinedException(EDAGProcessorException.class, "CANNOT_INGEST", "Unable to complete ingestion: {0}");
	public static final PredefinedException MISSING_MANDATORY_OPTION = new PredefinedException(EDAGProcessorException.class, "MISSING_MANDATORY_OPTION", "Option {0} is mandatory: {1}");
	public static final PredefinedException INCORRECT_ARGS_FOR_OPTION = new PredefinedException(EDAGProcessorException.class, "INCORRECT_ARGS_FOR_OPTION", "Incorrect arguments for option {0}: {1}");
	public static final PredefinedException EXT_CMD_ERROR = new PredefinedException(EDAGProcessorException.class, "EXT_CMD_ERROR", "Process {0} exited with exit code {1}");
	public static final PredefinedException CANNOT_INIT_PROCESS = new PredefinedException(EDAGProcessorException.class, "CANNOT_INIT_PROCESS", "Unable to initialize {0} process: {1}");
	public static final PredefinedException RUNNING_INSTANCE_EXISTS = new PredefinedException(EDAGProcessorException.class, "RUNNING_INSTANCE_EXISTS", "Process instance ID {0} is still running for process ID {1}. Cannot run another instance");
	public static final PredefinedException MISSING_EXCEL_FILE_DEFINITION = new PredefinedException(EDAGProcessorException.class, "MISSING_EXCEL_FILE_DEFINITION", "Missing Excel file definition. The Excel file name is missing for process ID {1} in EDAG_PROC_PARAM. Cannot start ingestion process");
	public static final PredefinedException MISSING_EXCEL_FILE = new PredefinedException(EDAGProcessorException.class, "MISSING_EXCEL_FILE", "Missing Excel file. The Excel name {0} is missing for process ID {1}. Cannot start ingestion process");
	public static final PredefinedException MISSING_SHEET_DEFINITION = new PredefinedException(EDAGProcessorException.class, "MISSING_SHEET_DEFINITION", "There is no specific sheet available to read from excel file {0}");
	public static final PredefinedException MISSING_HEADER_FIELD_DEFINITION = new PredefinedException(EDAGProcessorException.class, "MISSING_HEADER_FIELD_DEFINITION", "Missing Header Column Value. The field name {0} is missing a column header field. There is no alternate column name patterns specified");
	public static final PredefinedException MISSING_HEADER_ROW_DEFINITION = new PredefinedException(EDAGProcessorException.class, "MISSING_HEADER_ROW_DEFINITION", "Missing Header row. The header could not be identified for excel file {0}. The missing column names are {1}");
	public static final PredefinedException DUPLICATE_EXCEL_HEADER_FIELD_MAPPING = new PredefinedException(EDAGProcessorException.class, "DUPLICATE_EXCEL_HEADER_FIELD_MAPPING", "Duplicate header field mappings. The field {0} and {1} mapped to the same header field {2}");
	public static final PredefinedException REFERECE_FILE_COUNT_MISMATCH = new PredefinedException(EDAGProcessorException.class, "REFERECE_FILE_COUNT_MISMATCH", "Missing reference files. Maximum reference file missing count allowed is {0}, actual reference file missing count is {1}, for image field {2}");
	public static final PredefinedException DESTINATION_FOLDER_DOES_NOT_EXIST = new PredefinedException(EDAGProcessorException.class, "DESTINATION_FOLDER_DOES_NOT_EXIST", "The target folder {0} does not exist.");
	public static final PredefinedException MISSING_PROCESSING_HOUR_PARAM = new PredefinedException(EDAGProcessorException.class, "MISSING_PROCESSING_HOUR_PARAM", "The hour for which the logs to be processed is missing for proc id {0}.");
	public static final PredefinedException FAILURE_TO_PERFORM_COMPACTION = new PredefinedException(EDAGProcessorException.class, "FAILURE_TO_PERFORM_COMPACTION", "Compaction failed for proc id {0} with error message {1}. Need to perform ingestion for the BIZ DT {2}.");
	public static final PredefinedException INVALID_SPARK_DEPLOY_MODE = new PredefinedException(EDAGProcessorException.class, "INVALID_SPARK_DEPLOY_MODE", "Invalid cluster value for spark deploy mode {0}, for process id {1}");
	public static final PredefinedException SPARK_APPLICATION_FAILED = new PredefinedException(EDAGProcessorException.class, "SPARK_APPLICATION_FAILED", "Spark application failed for process id {0}");
	
	/**
	 * 
	 */
	private static final long serialVersionUID = 6434921510809632715L;

	public EDAGProcessorException(PredefinedException ex, Object... params) {
		super(ex, params);
	}

	public EDAGProcessorException(String message) {
		super(message);
	}

	public EDAGProcessorException(String message, Throwable cause) {
		super(message, cause);
	}

	public EDAGProcessorException(PredefinedException ex, Throwable cause, Object... params) {
		super(ex, cause, params);
	}
}
