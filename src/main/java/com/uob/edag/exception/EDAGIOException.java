package com.uob.edag.exception;

public class EDAGIOException extends EDAGException {
	
	public static final PredefinedException CANNOT_READ_RESOURCE = new PredefinedException(EDAGIOException.class, "CANNOT_READ_RESOURCE", "Resource {0} cannot be loaded as input stream / reader: {1}");
	public static final PredefinedException CANNOT_LIST_FILES_IN_DIR = new PredefinedException(EDAGIOException.class, "CANNOT_LIST_FILES_IN_DIR", "Unable to list files in directory {0}: {1}");
	public static final PredefinedException CANNOT_READ_FILE = new PredefinedException(EDAGIOException.class, "CANNOT_READ_FILE", "File {0} cannot be read: {1}");
	public static final PredefinedException CANNOT_WRITE_FILE = new PredefinedException(EDAGIOException.class, "CANNOT_WRITE_FILE", "File {0} cannot be written into: {1}");
	public static final PredefinedException CANNOT_READ_WRITE_FILE = new PredefinedException(EDAGIOException.class, "CANNOT_READ_WRITE_FILE", "Unable to read file {0} and write its content to file {1}: {2}");
	public static final PredefinedException CANNOT_READ_KEYTAB = new PredefinedException(EDAGIOException.class, "CANNOT_READ_KEYTAB", "Cannot read keytab file {0}: {1}");
	public static final PredefinedException CANNOT_GET_PROPERTY = new PredefinedException(EDAGIOException.class, "CANNOT_GET_PROPERTY", "Cannot get property {0}: {1}");
	public static final PredefinedException CANNOT_LOAD_PROPERTIES_FROM_FILE = new PredefinedException(EDAGIOException.class, "CANNOT_LOAD_PROPERTIES_FROM_FILE", "Cannot load properties from {0}: {1}");
	public static final PredefinedException CANNOT_MOVE_FILE = new PredefinedException(EDAGIOException.class, "CANNOT_MOVE_FILE", "Unable to move file {0} to {1}: {2}");
	public static final PredefinedException CANNOT_COPY_FILE = new PredefinedException(EDAGIOException.class, "CANNOT_COPY_FILE", "Unable to copy file {0} to {1}: {2}");
	public static final PredefinedException CANNOT_DELETE_FILE = new PredefinedException(EDAGIOException.class, "CANNOT_DELETE_FILE", "Unable to delete file {0}: {1}");
	public static final PredefinedException FILE_NOT_FOUND = new PredefinedException(EDAGIOException.class, "FILE_NOT_FOUND", "File / directory {0} is not accessible: {1}");
	public static final PredefinedException CANNOT_READ_INPUT_STREAM = new PredefinedException(EDAGIOException.class, "CANNOT_READ_INPUT_STREAM", "Unable to read from input stream: {0}");
	public static final PredefinedException CANNOT_COPY_STREAM_CONTENT = new PredefinedException(EDAGIOException.class, "CANNOT_COPY_STREAM_CONTENT", "Unable to copy content of input stream to output stream: {0}");
	public static final PredefinedException CANNOT_CREATE_TEMP_FILE = new PredefinedException(EDAGIOException.class, "CANNOT_CREATE_TEMP_FILE", "Unable to create temp file with prefix {0} and suffix {1}: {2}");
	public static final PredefinedException CANNOT_CREATE_TEMP_FILE_IN_DIR = new PredefinedException(EDAGIOException.class, "CANNOT_CREATE_TEMP_FILE_IN_DIR", "Unable to create tmpe file with prefix {0} and suffix {1} in directory {3}: {4}");
	public static final PredefinedException CANNOT_EXEC_CMD = new PredefinedException(EDAGIOException.class, "CANNOT_EXEC_CMD", "Command {0} execution failed: {1}");
	public static final PredefinedException NON_ZERO_EXIT_CODE = new PredefinedException(EDAGIOException.class, "NON_ZERO_EXIT_CODE", "{0}, exit code: {1}");
	public static final PredefinedException CANNOT_GZIP_FILE = new PredefinedException(EDAGIOException.class, "CANNOT_GZIP_FILE", "Unable to gzip file {0} into {1}: {2}");
	public static final PredefinedException FILE_TOO_LARGE = new PredefinedException(EDAGIOException.class, "FILE_TOO_LARGE", "Size of file {0} ({1}) is too large, maximum allowed file size is {2}");
	public static final PredefinedException INVALID_ZIP_FILE = new PredefinedException(EDAGIOException.class, "INVALID_ZIP_FILE", "{0} is not a valid zip file: {1}");
	public static final PredefinedException ZIP_ENTRY_NOT_FOUND = new PredefinedException(EDAGIOException.class, "ZIP_ENTRY_NOT_FOUND", "Zip entry {0} cannot be found in zip file {1}: {2}");
	public static final PredefinedException CANNOT_ARCHIVE_REFERENCE_FILES = new PredefinedException(EDAGProcessorException.class, "CANNOT_ARCHIVE_IMAGE_FILES", "Could not create zip file {0} for procid {1} and biz_dt {2}") ;
	public static final PredefinedException CANNOT_EXTRACT_REFERENCE_ARCHIVE_FILE = new PredefinedException(EDAGProcessorException.class, "CANNOT_EXTRACT_IMAGE_ARCHIVE_FILE", "Cannot extract image {0} from archive file {1}");
	public static final PredefinedException CANNOT_CHECK_IF_FILE_EXISTS = new PredefinedException(EDAGIOException.class, "CANNOT_CHECK_IF_FILE_EXISTS", "Unable to check if the file or directory {0} exists");
	public static final PredefinedException CANNOT_CREATE_DIRECTORY = new PredefinedException(EDAGIOException.class, "CANNOT_CREATE_DIRECTORY", "Unable to create the directory {0}");
	public static final PredefinedException CLASS_NOT_FOUND = new PredefinedException(EDAGIOException.class, "CLASS_NOT_FOUND", "JAVA Reflect does not find the class {0}") ;
	public static final PredefinedException CANNOT_RETRIEVE_DIRECTORY_SIZE = new PredefinedException(EDAGIOException.class, "CANNOT_RETRIEVE_DIRECTORY_SIZE", "Unable to retrieve directory size for path {0}");
	public static final PredefinedException SPARK_IO_EXCEPTION = new PredefinedException(EDAGIOException.class, "SPARK_IO_EXCEPTION", "Exception while running spark for process id {0}");
	
	/**
	 * 
	 */
	private static final long serialVersionUID = 1069779216727970700L;

	public EDAGIOException(String message) {
		super(message);
	}
	
	public EDAGIOException(PredefinedException ex, Object... params) {
		super(ex, params);
	}
	
	public EDAGIOException(String message, Throwable cause) {
		super(message, cause);
	}
	
	public EDAGIOException(PredefinedException ex, Throwable cause, Object... params) {
		super(ex, cause, params);
	}
}
