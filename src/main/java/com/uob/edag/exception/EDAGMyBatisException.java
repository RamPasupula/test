package com.uob.edag.exception;

public class EDAGMyBatisException extends EDAGException {
	
	public static final PredefinedException CANNOT_BUILD_SESSION_FACTORY = new PredefinedException(EDAGMyBatisException.class, "CANNOT_BUILD_SESSION_FACTORY", "Cannot build session factory using {0}: {1}");
	public static final PredefinedException CANNOT_INITIALIZE_SESSION_FACTORY = new PredefinedException(EDAGMyBatisException.class, "CANNOT_INITIALIZE_SESSION_FACTORY", "Unable to initialize session factory: {0}");
	public static final PredefinedException CANNOT_EXECUTE_SQL = new PredefinedException(EDAGMyBatisException.class, "CANNOT_EXECUTE_SQL", "Unable to execute SQL {0} using parameters {1}: {2}");

	/**
	 * 
	 */
	private static final long serialVersionUID = -4161324844071179998L;

	public EDAGMyBatisException(PredefinedException ex, Object... params) {
		super(ex, params);
	}

	public EDAGMyBatisException(String message) {
		super(message);
	}

	public EDAGMyBatisException(String message, Throwable cause) {
		super(message, cause);
	}

	public EDAGMyBatisException(PredefinedException ex, Throwable cause, Object... params) {
		super(ex, cause, params);
	}
}
