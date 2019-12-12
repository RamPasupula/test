package com.uob.edag.exception;

public class EDAGValidationException extends EDAGException {
	
	public static final PredefinedException INVALID_VALUE = new PredefinedException(EDAGValidationException.class, "INVALID_VALUE", "{0} value is invalid: {1}");
	public static final PredefinedException INVALID_EXPECTED_VALUE = new PredefinedException(EDAGValidationException.class, "INVALID_EXPECTED_VALUE", "Value of {0} is invalid, expected: {1}, actual: {2}");
	public static final PredefinedException DUPLICATED_VALUE = new PredefinedException(EDAGValidationException.class, "DUPLICATED_VALUE", "Value {0} already exists: {1}");
	public static final PredefinedException NULL_VALUE = new PredefinedException(EDAGValidationException.class, "NULL_VALUE", "Value of {0} is null: {1}");
	public static final PredefinedException MISSING_VALUE = new PredefinedException(EDAGValidationException.class, "MISSING_VALUE", "Value for {0} is missing: {1}");
	public static final PredefinedException EMPTY_VALUE = new PredefinedException(EDAGValidationException.class, "EMPTY_VALUE", "Value of {0} is empty: {1}");
	public static final PredefinedException INVALID_TYPE = new PredefinedException(EDAGValidationException.class, "INVALID_TYPE", "Type of {0} is invalid. Expected: {1}, actual: {2}");
	public static final PredefinedException ERROR_COUNT_ABOVE_THRESHOLD = new PredefinedException(EDAGValidationException.class, "ERROR_COUNT_ABOVE_THRESHOLD", "Process instance ID {0} error count ({1}) is above threshold ({2}). Total record count is {3}");
	public static final PredefinedException ROW_COUNT_RECONCILIATION_FAILURE = new PredefinedException(EDAGValidationException.class, "ROW_COUNT_RECONCILIATION_FAILURE", "Row count reconciliation failed for process instance ID {0}, source row count is {1}, target row count is {2}, error count is {3}");
  public static final PredefinedException HASH_SUM_RECONCILIATION_FAILURE = new PredefinedException(EDAGValidationException.class, "HASH_SUM_RECONCILIATION_FAILURE", "Hash sum reconciliation failed for process instance ID {0}, source hash sum is {1}, target hash sum is {2}");
  public static final PredefinedException PROCESS_ALREADY_EXISTS = new PredefinedException(EDAGValidationException.class, "PROCESS_ALREADY_EXISTS", "Process {0} already exists");
  public static final PredefinedException INVALID_DATE_FORMAT = new PredefinedException(EDAGValidationException.class, "INVALID_DATE_FORMAT", "Date {0} is unparseable, date must be in {1} format: {2}");
  public static final PredefinedException FIELD_COUNT_MISMATCH = new PredefinedException(EDAGValidationException.class, "FIELD_COUNT_MISMATCH", "Number of fields ({0}) is different from expected number of fields ({1}): {2}");
  public static final PredefinedException LINE_LENGTH_MISMATCH = new PredefinedException(EDAGValidationException.class, "LINE_LENGTH_MISMATCH", "Length of line ({0}) is different from expected length of line ({1}): {2}");
  public static final PredefinedException REGX_COUNT_MISMATCH = new PredefinedException(EDAGValidationException.class, "REGX_COUNT_MISMATCH", "lines {0}) is different from expected regular expression : {1}");
  public static final PredefinedException LINE_TOO_SHORT = new PredefinedException(EDAGValidationException.class, "LINE_TOO_SHORT", "Length of line ({0}) is unable to cater for field {1} which starts from position {2} to position {3}: {4}");
  public static final PredefinedException CONTROL_VALUE_MISMATCH = new PredefinedException(EDAGValidationException.class, "CONTROL_VALUE_MISMATCH", "Submitted {0} value ({1}) does not match Control Information {0} value ({2})");
  public static final PredefinedException MISSING_COLUMN = new PredefinedException(EDAGValidationException.class, "MISSING_COLUMN", "Column {0} cannot be found in table {1}: {2}");
  public static final PredefinedException MISSING_NORMALIZED_COLUMN = new PredefinedException(EDAGValidationException.class, "MISSING_NORMALIZED_COLUMN", "Normalized column {0} cannot be found in table {1}: {2}");
  public static final PredefinedException INGESTION_NOT_FOUND = new PredefinedException(EDAGValidationException.class, "INGESTION_NOT_FOUND_FOR_EDW_LOAD", "EDW load process {0} for country {1} on business date {2} does not have a corresponding file ingestion process: {3}");
  public static final PredefinedException EMPTY_HIVE_TABLE = new PredefinedException(EDAGValidationException.class, "EMPTY_HIVE_TABLE", "Hive table {0} is empty for country {1} and business date {2}.");
  public static final PredefinedException MAX_FILE_REFERENCES_EXCEEDED = new PredefinedException(EDAGValidationException.class, "MAX_FILE_REFERENCES_EXCEEDED", "File {0} has {1} file reference field(s). The maximum number of reference field allowed in a file is {2}");
  public static final PredefinedException INVALID_ATTACHMENT_COLUMN_DEFINITION = new PredefinedException(EDAGValidationException.class, "INVALID_ATTACHMENT_COLUMN_DEFINITION", "Attachment column name configuration in the metadata is invalid.");
  public static final PredefinedException PRE_PROCESSING_EXCEPTION = new PredefinedException(EDAGValidationException.class, "PRE_PROCESSING_EXCEPTION", "Exception during Pre Processing : {0}");
	
	/**
	 * 
	 */
	private static final long serialVersionUID = -1514609800764738217L;

	public EDAGValidationException(String message) {
		super(message);
	}
	
	public EDAGValidationException(PredefinedException ex, Object... params) {
		super(ex, params);
	}

	public EDAGValidationException(String message, Throwable cause) {
		super(message, cause);
	}
	
	public EDAGValidationException(PredefinedException ex, Throwable cause, Object... params) {
		super(ex, cause, params);
	}
}
