package com.uob.edag.exception;

public class EDAGSSHException extends EDAGException {
	
	public static final PredefinedException CANNOT_GET_SESSION = new PredefinedException(EDAGSSHException.class, "CANNOT_GET_SESSION", "Unable to open SSH session to {0}:{1} as {2}: {3}");
	public static final PredefinedException CANNOT_CONNECT_SESSION = new PredefinedException(EDAGSSHException.class, "CANNOT_CONNECT_SESSION", "Unable to open TCP connection using SSH session to {0}:{1} as {2}: {3}");
	public static final PredefinedException INVALID_CHANNEL_TYPE = new PredefinedException(EDAGSSHException.class, "INVALID_CHANNEL_TYPE", "Channel of type {0} is not supported");
	public static final PredefinedException CHANNEL_NOT_ASSIGNABLE = new PredefinedException(EDAGSSHException.class, "CHANNEL_NOT_ASSIGNABLE", "Channel of type {0} is not assignable from {1}");
	public static final PredefinedException CANNOT_CONNECT_CHANNEL = new PredefinedException(EDAGSSHException.class, "CANNOT_CONNECT_CHANNEL", "Unable to connect channel of type {0}: {1}");
	public static final PredefinedException FILE_NOT_FOUND = new PredefinedException(EDAGSSHException.class, "FILE_NOT_FOUND", "File {0} not found in host {1}");
	public static final PredefinedException DIR_NOT_FOUND = new PredefinedException(EDAGSSHException.class, "DIR_NOT_FOUND", "Directory {0} not found in host {1}");
	public static final PredefinedException CANNOT_LIST_FILES = new PredefinedException(EDAGSSHException.class, "CANNOT_LIST_FILES", "Unable to list file / directory {0} in host {1}: {2}");
	public static final PredefinedException CANNOT_GET_FILE = new PredefinedException(EDAGSSHException.class, "CANNOT_GET_FILE", "Unable to get file {0} from host {1}: {2}");
	public static final PredefinedException CANNOT_PUT_FILE = new PredefinedException(EDAGSSHException.class, "CANNOT_PUT_FILE", "Unable to put file {0} into {1} in host {2}: {3}");
	public static final PredefinedException CANNOT_DELETE_FILE = new PredefinedException(EDAGSSHException.class, "CANNOT_DELETE_FILE", "Unable to delete file {0} from host {1}: {2}");
	public static final PredefinedException CANNOT_RENAME_FILE = new PredefinedException(EDAGSSHException.class, "CANNOT_RENAME_FILE", "Unable to rename file {0} to {1} in host {2}: {3}");
	public static final PredefinedException NON_ZERO_EXIT_CODE = new PredefinedException(EDAGSSHException.class, "NON_ZERO_EXIT_CODE", "{0}, exit code: {1}");
	public static final PredefinedException CANNOT_EXEC_CMD = new PredefinedException(EDAGSSHException.class, "CANNOT_EXEC_CMD", "Command {0} execution failed: {1}");
	
	/**
	 * 
	 */
	private static final long serialVersionUID = 7338808806833750984L;

	public EDAGSSHException(PredefinedException ex, Object... params) {
		super(ex, params);
	}

	public EDAGSSHException(String message) {
		super(message);
	}

	public EDAGSSHException(String message, Throwable cause) {
		super(message, cause);
	}

	public EDAGSSHException(PredefinedException ex, Throwable cause, Object... params) {
		super(ex, cause, params);
	}
}
