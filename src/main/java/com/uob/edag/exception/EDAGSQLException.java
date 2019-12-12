package com.uob.edag.exception;

public class EDAGSQLException extends EDAGException {
	
	public static final PredefinedException NO_JDBC_DRIVER = new PredefinedException(EDAGSQLException.class, "NO_JDBC_DRIVER", "Unable to instantiate {0} JDBC driver: {1}");
	public static final PredefinedException CANNOT_CONNECT_TO_DB = new PredefinedException(EDAGSQLException.class, "CANNOT_CONNECT_TO_DB", "Unable to create connection to {0}: {1}");

	/**
	 * 
	 */
	private static final long serialVersionUID = -3368032273903783192L;

	public EDAGSQLException(PredefinedException ex, Object... params) {
		super(ex, params);
	}

	public EDAGSQLException(String message) {
		super(message);
	}

	public EDAGSQLException(String message, Throwable cause) {
		super(message, cause);
	}

	public EDAGSQLException(PredefinedException ex, Throwable cause, Object... params) {
		super(ex, cause, params);
	}
}
